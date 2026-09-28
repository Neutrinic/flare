"""The 22 TPC-H queries, as a realistic mix of scans, joins, shuffles and spills.

    spark-submit tpch.py --data /path/to/sf5 --queries /path/to/queries --result result.json

--data holds one Parquet directory per table, as a local path or any URI Spark reads (gs://,
s3://, a Unity Catalog volume); --queries holds q1.sql to q22.sql on the driver's filesystem. Both
come from tpch_generate.py. With a local --data, the directory must exist at the same path on every
node.

By default the queries run once. --minutes N keeps running passes until N minutes have gone, for a
long-lived application: each pass records its time, the CPU every Spark JVM has used so far and the
driver's heap after a full GC, so a fixed start-up cost can be told from a per-pass one and a leak
would show as heap that keeps growing.

JVM CPU is read inside the application, so it needs no SSH or /usr/bin/time and works on managed
platforms: the driver's from /proc/<pid>/stat of its own JVM, each executor's from a small probe
job whose tasks walk up from the Python worker to the executor JVM that started it. It counts every
thread of each JVM since it started, start-up included.
"""
import argparse
import json
import os
import time
import urllib.request

from pyspark.sql import SparkSession

TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]

parser = argparse.ArgumentParser()
parser.add_argument("--data", required=True)
parser.add_argument("--queries", required=True)
parser.add_argument("--result", required=True)
parser.add_argument("--minutes", type=float, default=0, help="run passes until this long has gone")
parser.add_argument("--stats-url", help="OTLP sink admin URL to read telemetry totals from at the end")
parser.add_argument("--log-level", default="WARN",
                    help="Spark log level to set, or 'default' to leave the cluster's own")
parser.add_argument("--no-stop", action="store_true", help="leave the session running (Databricks jobs)")
args = parser.parse_args()


def proc_cpu(pid):
    """(CPU seconds, start time in ticks) of a process, from /proc/<pid>/stat."""
    with open(f"/proc/{pid}/stat") as fh:
        stat = fh.read()
    rest = stat[stat.rindex(")") + 2:].split()  # rest[0] is field 3, the state
    return (int(rest[11]) + int(rest[12])) / os.sysconf("SC_CLK_TCK"), int(rest[19])


def executor_jvm(_):
    """Runs in a Python worker: find the executor JVM above it and report its CPU."""
    import socket
    pid = os.getpid()
    while pid > 1:
        with open(f"/proc/{pid}/stat") as fh:
            stat = fh.read()
        comm = stat[stat.index("(") + 1:stat.rindex(")")]
        if comm == "java":
            cpu, start = proc_cpu(pid)
            return [(socket.gethostname(), pid, start, cpu)]
        pid = int(stat[stat.rindex(")") + 2:].split()[1])
    return [(socket.gethostname(), None, None, None)]


def jvm_cpu(spark):
    sc = spark.sparkContext
    jvm = sc._jvm
    driver_pid = int(jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0])
    tasks = 8 * sc.defaultParallelism
    seen = {}
    for host, pid, start, cpu in sc.parallelize(range(tasks), tasks).mapPartitions(executor_jvm).collect():
        if pid is not None:
            seen[(host, pid, start)] = max(cpu, seen.get((host, pid, start), 0))
    try:
        expected = sc._jsc.sc().getExecutorMemoryStatus().size() - 1  # minus the driver
    except Exception:
        expected = None
    return {"driver_cpu_s": proc_cpu(driver_pid)[0],
            "executor_cpu_s": sum(seen.values()),
            "executors_seen": len(seen), "executors_expected": expected}


def driver_heap_mb(spark):
    jvm = spark.sparkContext._jvm
    jvm.java.lang.System.gc()
    return jvm.java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() / 2**20


class StageCpu:
    """CPU and run time inside tasks, from the driver's REST API, one pass at a time so the UI's
    stage retention never drops any before they are counted."""

    def __init__(self, sc):
        self.url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages?status=complete" \
            if sc.uiWebUrl else None
        self.seen = set()

    def new(self):
        if not self.url:
            return None
        try:
            with urllib.request.urlopen(self.url, timeout=30) as r:
                stages = json.load(r)
        except Exception:
            self.url = None
            return None
        cpu = run = 0
        for s in stages:
            key = (s["stageId"], s["attemptId"])
            if key not in self.seen:
                self.seen.add(key)
                cpu += s.get("executorCpuTime", 0) / 1e9
                run += s.get("executorRunTime", 0) / 1e3
        return {"task_cpu_s": cpu, "task_run_s": run}


app_started = time.time()
spark = SparkSession.builder.appName("flare-bench tpch").getOrCreate()
if args.log_level != "default":
    spark.sparkContext.setLogLevel(args.log_level)
for table in TABLES:
    spark.read.parquet(f"{args.data.rstrip('/')}/{table}").createOrReplaceTempView(table)
queries = {n: open(os.path.join(args.queries, f"q{n}.sql")).read() for n in range(1, 23)}
stage_cpu = StageCpu(spark.sparkContext)

at_start = jvm_cpu(spark)
at_start["elapsed_s"] = time.time() - app_started
stage_cpu.new()  # the probe's own stages are not query work

passes = []
started = time.time()
while True:
    t = time.time()
    per_query = {}
    for number, sql in queries.items():
        q = time.time()
        rows = spark.sql(sql).collect()
        per_query[number] = {"seconds": time.time() - q, "rows": len(rows)}
    record = {"seconds": time.time() - t, "queries": per_query}
    record.update(stage_cpu.new() or {})
    if args.minutes:
        record.update(jvm_cpu(spark))
        record["driver_heap_mb"] = driver_heap_mb(spark)
        record["elapsed_s"] = time.time() - app_started
        stage_cpu.new()
    passes.append(record)
    print(f"pass {len(passes)}: {record['seconds']:.1f}s", flush=True)
    if time.time() - started >= args.minutes * 60:
        break
seconds = time.time() - started

result = {"app_id": spark.sparkContext.applicationId, "seconds": seconds,
          "queries": passes[0]["queries"], "passes": passes, "at_start": at_start,
          "at_end": jvm_cpu(spark) if not args.minutes else None,
          "spark_version": spark.version}
if args.stats_url:
    time.sleep(10)  # let the last export batches land
    try:
        with urllib.request.urlopen(args.stats_url.rstrip("/") + "/stats", timeout=10) as r:
            result["telemetry"] = json.load(r)
    except Exception as e:
        result["telemetry_error"] = repr(e)
with open(args.result, "w") as fh:
    json.dump(result, fh)
if not args.no_stop:
    spark.stop()
