"""A minimal OTLP/HTTP receiver that counts what it is sent, for measuring telemetry volume.

    python otlp_sink.py --port 4318

Point the agent at it with -Dotel.exporter.otlp.endpoint=http://<host>:4318
-Dotel.exporter.otlp.protocol=http/protobuf. It accepts traces, metrics and logs, answers every
export with an empty success response, and keeps per-signal totals.

The totals are served on a separate admin port bound to localhost only (default 4319), so nothing
else on the network can read or reset them mid-run:

    GET  /stats   the totals as JSON
    POST /reset   zero them

Request bodies are capped (--max-bytes, applied to the body as sent and after decompression), so a
malformed or hostile request cannot exhaust memory. An oversized request is rejected with 413.

Bytes are counted twice: as received on the wire (after any gzip the exporter applied) and
decompressed. Spans are counted by walking the protobuf, with no generated classes needed:
ExportTraceServiceRequest.resource_spans(1) -> ScopeSpans(2) -> Span(2), name in field 5.
"""
import argparse
import json
import threading
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

lock = threading.Lock()


def empty_stats():
    return {sig: {"requests": 0, "wire_bytes": 0, "bytes": 0} for sig in ("traces", "metrics", "logs")} | {
        "spans": 0, "spark_spans": 0, "span_names": {}}


stats = empty_stats()
MAX_BYTES = 64 * 1024 * 1024  # replaced from --max-bytes


class TooLarge(Exception):
    pass


def gunzip(data):
    """Decompress gzip without ever holding more than MAX_BYTES of output."""
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    out = d.decompress(data, MAX_BYTES + 1)
    if len(out) > MAX_BYTES or d.unconsumed_tail:
        raise TooLarge()
    return out


def varint(buf, i):
    shift = result = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7


def fields(buf):
    """Yield (field_number, value) for length-delimited fields; skip everything else."""
    i = 0
    while i < len(buf):
        key, i = varint(buf, i)
        number, wire = key >> 3, key & 7
        if wire == 0:
            _, i = varint(buf, i)
        elif wire == 1:
            i += 8
        elif wire == 5:
            i += 4
        elif wire == 2:
            length, i = varint(buf, i)
            yield number, buf[i:i + length]
            i += length
        else:
            raise ValueError(f"unsupported wire type {wire}")


def span_names(request):
    for n1, resource_spans in fields(request):
        if n1 != 1:
            continue
        for n2, scope_spans in fields(resource_spans):
            if n2 != 2:
                continue
            for n3, span in fields(scope_spans):
                if n3 != 2:
                    continue
                name = next((v for n, v in fields(span) if n == 5), b"")
                yield name.decode("utf-8", "replace")


class Base(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _body(self):
        # The agent's exporter sends gzip bodies with chunked transfer encoding and no
        # Content-Length, so both framings have to be read. Either way, stop at MAX_BYTES.
        if "Content-Length" in self.headers:
            length = int(self.headers["Content-Length"])
            if length > MAX_BYTES:
                raise TooLarge()
            return self.rfile.read(length)
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            parts, total = [], 0
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip(), 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                        pass  # trailers
                    return b"".join(parts)
                total += size
                if total > MAX_BYTES:
                    raise TooLarge()
                parts.append(self.rfile.read(size))
                self.rfile.readline()  # CRLF after each chunk
        return b""

    def _send(self, code, body=b"", content_type="application/x-protobuf"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class Admin(Base):
    """Reads and resets the totals. Served on localhost only."""

    def do_GET(self):
        if self.path != "/stats":
            return self._send(404)
        with lock:
            body = json.dumps(stats).encode()
        self._send(200, body, "application/json")

    def do_POST(self):
        global stats
        if self.path != "/reset":
            return self._send(404)
        with lock:
            stats = empty_stats()
        self._send(200, b"{}", "application/json")


class Otlp(Base):
    """Receives OTLP exports and counts them."""

    def do_POST(self):
        signal = self.path.rsplit("/", 1)[-1]
        if signal not in ("traces", "metrics", "logs"):
            return self._send(404)
        try:
            wire = self._body()
            raw = gunzip(wire) if self.headers.get("Content-Encoding") == "gzip" else wire
        except TooLarge:
            self.close_connection = True
            return self._send(413)
        names = list(span_names(raw)) if signal == "traces" else []
        with lock:
            s = stats[signal]
            s["requests"] += 1
            s["wire_bytes"] += len(wire)
            s["bytes"] += len(raw)
            stats["spans"] += len(names)
            for name in names:
                key = name.rstrip("0123456789").rstrip(".") if name.startswith("spark.") else name
                stats["span_names"][key] = stats["span_names"].get(key, 0) + 1
                if name.startswith("spark."):
                    stats["spark_spans"] += 1
        self._send(200)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0", help="where exporters reach the OTLP port")
    parser.add_argument("--port", type=int, default=4318)
    parser.add_argument("--admin-port", type=int, default=4319, help="stats and reset, localhost only")
    parser.add_argument("--max-bytes", type=int, default=MAX_BYTES,
                        help="largest request body accepted, as sent and decompressed")
    args = parser.parse_args()
    MAX_BYTES = args.max_bytes
    admin = ThreadingHTTPServer(("127.0.0.1", args.admin_port), Admin)
    threading.Thread(target=admin.serve_forever, daemon=True).start()
    print(f"OTLP sink on {args.host}:{args.port}, admin on 127.0.0.1:{args.admin_port}", flush=True)
    ThreadingHTTPServer((args.host, args.port), Otlp).serve_forever()
