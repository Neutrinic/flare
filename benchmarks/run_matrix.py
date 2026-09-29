"""Run the overhead benchmark: every configuration against every workload, repeated and interleaved.

Run on the host that submits jobs (client deploy mode, so the driver runs here under
/usr/bin/time), with otlp_sink.py already running. See README.md for the full method.

    python run_matrix.py --agent /opt/flare/opentelemetry-javaagent.jar \\
        --flare /opt/flare/flare-spark.jar --sink http://driver-host:4318 \\
        --event-dir /tmp/flare-bench/events --out /tmp/flare-bench/results.jsonl \\
        --workloads tasks-1000,tasks-10000,tasks-100000,tpch \\
        --tpch-data /data/tpch/sf5 --tpch-queries /data/tpch/queries \\
        --nodes local,worker1,worker2 --ssh-key ~/.ssh/id_ed25519 --repeats 3

Configurations, from nothing to everything:

    off               no agent, no Flare: the baseline
    agent             the OpenTelemetry agent alone, to separate its cost from Flare's
    stages            Flare at its default granularity: spans down to stages, no task spans
    tasks-unsampled   task spans configured, but the application not sampled (ratio 0)
    tasks             everything traced, every task span, under the default span cap
    tasks-uncapped    as tasks, with the span cap raised so every task gets a span. For the
                      per-task cost of tracing: `tasks` stops at 10,000 task spans per executor
    tasks-lean        as tasks, with the agent's own instrumentations off, leaving only Flare's.
                      How much of the agent's start-up cost is avoidable
    tasks-logs        as tasks, with log export on (the agent's default): the cost of log capture
    tasks-lean-logs   as tasks-lean, with the agent's Log4j capture turned back on

The cost of tracing a fraction p of applications is then (1-p) x tasks-unsampled + p x tasks.
Running at p=0.1 directly would leave most repeats unsampled and the average would be noise.
"""
import argparse
import json
import re
import shlex
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFIGS = ["off", "agent", "stages", "tasks-unsampled", "tasks"]
OPTIONAL_CONFIGS = ["tasks-uncapped", "tasks-lean", "tasks-logs", "tasks-lean-logs"]

parser = argparse.ArgumentParser()
parser.add_argument("--spark-submit", default="spark-submit",
                    help="command to submit with, e.g. 'spark-submit --master spark://host:7077'")
parser.add_argument("--agent", required=True)
parser.add_argument("--flare", required=True)
parser.add_argument("--sink", required=True, help="OTLP sink base URL, reachable from every node")
parser.add_argument("--sink-admin", default="http://127.0.0.1:4319",
                    help="the sink's admin URL; it listens on localhost of the driver host")
parser.add_argument("--event-dir", type=Path, required=True)
parser.add_argument("--out", type=Path, required=True)
parser.add_argument("--workloads", default="tasks-1000,tasks-10000,tasks-100000,tpch")
parser.add_argument("--configs", default=",".join(CONFIGS))
parser.add_argument("--repeats", type=int, default=3)
parser.add_argument("--tpch-data")
parser.add_argument("--tpch-queries")
parser.add_argument("--examples-jar", help="Spark's examples JAR, for the rdd-* workloads")
parser.add_argument("--tpch-log-level", default="WARN", help="Spark log level, or 'default'")
parser.add_argument("--tpch-minutes", type=float, default=0, help="loop TPC-H passes for this long")
parser.add_argument("--nodes", default="local", help="hosts to sample CPU on; 'local' is this host")
parser.add_argument("--ssh-key")
parser.add_argument("--ssh-user", default=None)
parser.add_argument("--no-warmup", action="store_true")
parser.add_argument("--extra-java-opts", default="", help="appended to every configuration but off, on driver and executors")
args = parser.parse_args()
if any(w.startswith("rdd-") for w in args.workloads.split(",")) and not args.examples_jar:
    parser.error("the rdd-* workloads need --examples-jar")
if "tpch" in args.workloads.split(",") and not (args.tpch_data and args.tpch_queries):
    parser.error("the tpch workload needs --tpch-data and --tpch-queries")

args.event_dir.mkdir(parents=True, exist_ok=True)
work = args.out.parent / "runs"
work.mkdir(parents=True, exist_ok=True)
CLK_TCK = 100  # jiffies per second on Linux


def java_opts(config, role):
    if config == "off":
        return ""
    opts = [f"-javaagent:{args.agent}",
            f"-Dotel.exporter.otlp.endpoint={args.sink}",
            "-Dotel.exporter.otlp.protocol=http/protobuf",
            "-Dotel.exporter.otlp.compression=gzip",
            "-Dotel.logs.exporter=" + ("otlp" if config.endswith("-logs") else "none"),
            f"-Dotel.service.name=bench-{role}"]
    opts += shlex.split(args.extra_java_opts)
    if config == "agent":
        return " ".join(opts)
    opts.append(f"-Dotel.javaagent.extensions={args.flare}")
    opts.append("-DFLARE_TRACE_GRANULARITY=" + ("stages" if config == "stages" else "all"))
    if config in ("tasks-lean", "tasks-lean-logs"):
        opts += ["-Dotel.instrumentation.common.default-enabled=false",
                 "-Dotel.instrumentation.flare-spark.enabled=true",
                 # Flare calls the OpenTelemetry API; this bridges it to the agent's SDK
                 "-Dotel.instrumentation.opentelemetry-api.enabled=true"]
    if config == "tasks-lean-logs":
        opts.append("-Dotel.instrumentation.log4j-appender.enabled=true")
    if config == "tasks-uncapped":
        opts.append("-DFLARE_MAX_SPANS_PER_TRACE=100000000")
    if config == "tasks-unsampled" and role == "driver":
        # The same code path as the unsampled 90% at a 10% ratio. Executors keep the default and
        # follow the driver's decision through each task's traceparent.
        opts += ["-Dotel.traces.sampler=parentbased_traceidratio", "-Dotel.traces.sampler.arg=0"]
    return " ".join(opts)


def submit_args(config):
    conf = {
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": f"file://{args.event_dir}",
        "spark.eventLog.compress": "false",
        "spark.eventLog.logStageExecutorMetrics": "true",
    }
    if config != "off":
        conf["spark.driver.extraJavaOptions"] = java_opts(config, "driver")
        conf["spark.executor.extraJavaOptions"] = java_opts(config, "executor")
    if config not in ("off", "agent"):
        conf["spark.plugins"] = "io.flare.spark.plugin.FlareSparkPlugin"
        conf["spark.driver.extraClassPath"] = args.flare
        conf["spark.executor.extraClassPath"] = args.flare
    out = []
    for k, v in conf.items():
        out += ["--conf", f"{k}={v}"]
    return out


def workload_args(workload, result):
    if workload.startswith("tasks-"):
        return [str(HERE / "many_tasks.py"), "--tasks", workload.split("-", 1)[1], "--result", str(result)]
    if workload.startswith("many-jobs-"):
        return [str(HERE / "many_jobs.py"), "--jobs", workload.rsplit("-", 1)[1], "--result", str(result)]
    # RDD workloads from Spark's own examples: JVM only, no SQL executions, so no plan capture.
    if workload == "rdd-groupby":  # 60 mappers x 200,000 pairs of 100 bytes, grouped into 60
        return ["--class", "org.apache.spark.examples.GroupByTest", args.examples_jar, "60", "200000", "100", "60"]
    if workload == "rdd-tc":  # transitive closure of a random graph: dozens of small iterative jobs
        return ["--class", "org.apache.spark.examples.SparkTC", args.examples_jar, "12"]
    if workload == "tpch":
        return [str(HERE / "tpch.py"), "--data", args.tpch_data, "--queries", args.tpch_queries,
                "--result", str(result), "--minutes", str(args.tpch_minutes), "--log-level", args.tpch_log_level]
    raise SystemExit(f"unknown workload {workload}")


def cpu_sample(node):
    if node == "local":
        line = Path("/proc/stat").read_text().splitlines()[0]
    else:
        ssh = ["ssh", "-o", "BatchMode=yes"] + (["-i", args.ssh_key] if args.ssh_key else [])
        target = f"{args.ssh_user}@{node}" if args.ssh_user else node
        line = subprocess.check_output(ssh + [target, "head -1 /proc/stat"], text=True).strip()
    vals = [int(x) for x in line.split()[1:9]]
    total = sum(vals)
    return total - vals[3] - vals[4], total  # busy, total (idle and iowait excluded from busy)


def cpu_all(nodes):
    result, errors = {}, {}

    def sample(n):
        try:
            result[n] = cpu_sample(n)
        except Exception as e:  # reported below, with the node that failed
            errors[n] = e

    threads = [threading.Thread(target=sample, args=(n,)) for n in nodes]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise RuntimeError("CPU sampling failed: " + "; ".join(f"{n}: {e!r}" for n, e in errors.items()))
    return result


def sink(path, method="GET"):
    req = urllib.request.Request(args.sink_admin.rstrip("/") + path, method=method,
                                 data=b"" if method == "POST" else None)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def time_v(stderr):
    """Parse GNU time -v output from the tail of stderr."""
    def grab(label):
        m = re.search(rf"^\s*{re.escape(label)}: (.+)$", stderr, re.M)
        return m.group(1).strip() if m else None
    user, system, rss = grab("User time (seconds)"), grab("System time (seconds)"), grab("Maximum resident set size (kbytes)")
    return {"driver_cpu_s": float(user) + float(system) if user and system else None,
            "driver_max_rss_mb": int(rss) / 1024 if rss else None}


def event_log_files(app_id):
    """The event log for app_id: one file, or Spark's rolling layout, a directory of parts."""
    rolling = args.event_dir / f"eventlog_v2_{app_id}"
    if rolling.is_dir():
        parts = rolling.glob("events_*")
        return rolling, sorted(parts, key=lambda p: int(p.name.split("_")[1]))
    single = next(iter(sorted(args.event_dir.glob(f"{app_id}*"))), None)
    return single, [single] if single else []


def event_log(app_id):
    path, files = event_log_files(app_id)
    if not files:
        return {"event_log": None}
    tasks = cpu_ns = run_ms = 0
    heap = {}
    start = end = None
    for f in files:
        with f.open(encoding="utf-8") as fh:
            for line in fh:
                e = json.loads(line)
                kind = e.get("Event")
                if kind == "SparkListenerTaskEnd":
                    tasks += 1
                    m = e.get("Task Metrics") or {}
                    cpu_ns += m.get("Executor CPU Time", 0)
                    run_ms += m.get("Executor Run Time", 0)
                elif kind == "SparkListenerStageExecutorMetrics":
                    eid = e["Executor ID"]
                    heap[eid] = max(heap.get(eid, 0), e["Executor Metrics"].get("JVMHeapMemory", 0))
                elif kind == "SparkListenerApplicationStart":
                    start = e["Timestamp"]
                elif kind == "SparkListenerApplicationEnd":
                    end = e["Timestamp"]
    executors = [v for k, v in heap.items() if k != "driver"]
    return {"event_log": path.name, "tasks": tasks,
            "executor_task_cpu_s": cpu_ns / 1e9, "executor_task_run_s": run_ms / 1e3,
            "driver_peak_heap_mb": heap.get("driver", 0) / 2**20 or None,
            "executor_peak_heap_mb": max(executors) / 2**20 if executors else None,
            "app_seconds": (end - start) / 1e3 if start and end else None}


def run(config, workload, repeat, order, record=True):
    name = f"r{repeat}-{workload}-{config}"
    result = work / f"{name}.json"
    result.unlink(missing_ok=True)
    nodes = args.nodes.split(",")
    sink("/reset", "POST")
    before = cpu_all(nodes)
    cmd = ["/usr/bin/time", "-v"] + shlex.split(args.spark_submit) + submit_args(config) + workload_args(workload, result)
    started = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    wall = time.time() - started
    after = cpu_all(nodes)
    time.sleep(5)  # let late exports from executors land before reading the sink
    telemetry = sink("/stats")
    (work / f"{name}.stderr").write_text(proc.stderr)
    rec = {"config": config, "workload": workload, "repeat": repeat, "order": order,
           "exit": proc.returncode, "wall_s": wall}
    rec.update(time_v(proc.stderr))
    rec["node_cpu_s"] = {n: (after[n][0] - before[n][0]) / CLK_TCK for n in nodes}
    rec["cluster_cpu_s"] = sum(rec["node_cpu_s"].values())
    rec["dropped_event_lines"] = [l for l in proc.stderr.splitlines() if re.search(r"Dropp(ed|ing) .*event", l)]
    if not result.exists():
        # The rdd-* workloads write no result: take the application id from spark-submit's log,
        # or failing that, the newest event log written during the run.
        m = re.search(r"\b(app-\d{14}-\d{4}|application_\d+_\d+|local-\d+)\b", proc.stderr)
        app_id = m.group(1) if m else None
        if not app_id:
            fresh = [p for p in args.event_dir.iterdir() if p.stat().st_mtime >= started]
            if fresh:
                newest = max(fresh, key=lambda p: p.stat().st_mtime).name
                app_id = newest.removeprefix("eventlog_v2_").split(".")[0]
        if app_id and proc.returncode == 0:
            rec["app_id"] = app_id
            rec.update(event_log(app_id))
    if result.exists():
        wl = json.loads(result.read_text())
        rec["seconds"] = wl["seconds"]
        rec["app_id"] = wl["app_id"]
        for key in ("passes", "at_start", "at_end"):
            if key in wl:
                rec[key] = wl[key]
        for p in rec.get("passes", []):
            p.pop("queries", None)
        rec.update(event_log(wl["app_id"]))
    rec["telemetry"] = telemetry
    print(f"{name}: exit={proc.returncode} seconds={rec.get('seconds') or 0:.1f} "
          f"cluster_cpu={rec['cluster_cpu_s']:.1f}s spans={telemetry['spans']} "
          f"dropped={len(rec['dropped_event_lines'])}", flush=True)
    if record:
        with args.out.open("a") as fh:
            fh.write(json.dumps(rec) + "\n")


workloads = args.workloads.split(",")
configs = args.configs.split(",")
unknown = set(configs) - set(CONFIGS + OPTIONAL_CONFIGS)
if unknown:
    parser.error(f"unknown configs: {', '.join(sorted(unknown))}")
if not args.no_warmup:
    for w in workloads:
        run("off", w, 0, 0, record=False)  # page cache, JIT on the hosts, first-touch effects
order = 0
for r in range(1, args.repeats + 1):
    rotated = configs[(r - 1) % len(configs):] + configs[:(r - 1) % len(configs)]
    for w in workloads:
        for c in rotated:
            order += 1
            run(c, w, r, order)
