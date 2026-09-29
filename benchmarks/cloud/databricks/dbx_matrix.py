"""The overhead matrix on Databricks: one job cluster per run, since JVM options are fixed when a
cluster starts. Runs from any machine with the Databricks CLI; results land in a volume and are
appended to --out here.

    python dbx_matrix.py --profile flare-aws --user me@example.com --out results.jsonl \\
        --configs off,agent,tasks,tasks-lean --repeats 3 --minutes 20 --parallel 2

Expects in --bench (a Unity Catalog volume directory): flare-spark.jar, otlp_sink.py, init.sh and
tpch/sf<N>, tpch/queries (from tpch_generate.py, or --generate to make them). tpch.py and
tpch_generate.py are run from --workspace-dir.
"""
import argparse
import json
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

parser = argparse.ArgumentParser()
parser.add_argument("--profile", required=True)
parser.add_argument("--user", required=True, help="the dedicated cluster's single user")
parser.add_argument("--bench", default="/Volumes/aws/default/flare/bench")
parser.add_argument("--workspace-dir", required=True, help="workspace folder holding tpch.py")
parser.add_argument("--out", required=True)
parser.add_argument("--configs", default="off,agent,tasks,tasks-lean")
parser.add_argument("--repeats", type=int, default=3)
parser.add_argument("--minutes", type=float, default=20)
parser.add_argument("--scale-factor", type=int, default=10)
parser.add_argument("--parallel", type=int, default=2)
parser.add_argument("--spark-version", default="15.4.x-scala2.12")
parser.add_argument("--node-type", default="m5d.xlarge")
parser.add_argument("--workers", type=int, default=3)
parser.add_argument("--photon", action="store_true")
parser.add_argument("--log-level", default="WARN", help="Spark log level for tpch.py, or 'default'")
parser.add_argument("--tag", default="", help="suffix for run names and result files")
parser.add_argument("--generate", action="store_true", help="generate TPC-H into the volume first")
parser.add_argument("--max-failures", type=int, default=3)
args = parser.parse_args()

AGENT = "-javaagent:/opt/flare/opentelemetry-javaagent.jar -Dotel.javaagent.configuration-file=/opt/flare/otel.properties"
lock = threading.Lock()
failures = 0


def cli(*a, parse=True):
    out = subprocess.run(["databricks", *a, "-p", args.profile], capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(a[:3])}: {out.stderr.strip()[:500]}")
    return json.loads(out.stdout) if parse else out.stdout


def java_opts(config, role):
    opts = [AGENT, f"-Dotel.service.name=bench-{role}"]
    if config != "agent":
        opts += ["-Dotel.javaagent.extensions=/opt/flare/flare-spark.jar", "-DFLARE_TRACE_GRANULARITY=all"]
    if config.endswith("-logs"):
        opts.append("-Dotel.logs.exporter=otlp")  # overrides the init script's configuration file
    if config == "tasks-lean-logs":
        opts.append("-Dotel.instrumentation.log4j-appender.enabled=true")
    if config in ("tasks-lean", "tasks-lean-logs"):
        opts += ["-Dotel.instrumentation.common.default-enabled=false",
                 "-Dotel.instrumentation.flare-spark.enabled=true",
                 # Flare calls the OpenTelemetry API; this bridges it to the agent's SDK
                 "-Dotel.instrumentation.opentelemetry-api.enabled=true"]
    return " ".join(opts)


def cluster(config, workers=None, node_type=None):
    conf = {}
    if config != "off":
        conf["spark.driver.extraJavaOptions"] = java_opts(config, "driver")
        conf["spark.executor.extraJavaOptions"] = java_opts(config, "executor")
    if config not in ("off", "agent"):
        conf["spark.plugins"] = "io.flare.spark.plugin.FlareSparkPlugin"
    spec = {
        "spark_version": args.spark_version,
        "node_type_id": node_type or args.node_type,
        "driver_node_type_id": node_type or args.node_type,
        "num_workers": args.workers if workers is None else workers,
        "data_security_mode": "SINGLE_USER",
        "single_user_name": args.user,
        "runtime_engine": "PHOTON" if args.photon else "STANDARD",
        "aws_attributes": {"availability": "ON_DEMAND", "first_on_demand": 100, "zone_id": "auto"},
        "init_scripts": [{"volumes": {"destination": f"{args.bench}/init.sh"}}],
        "spark_conf": conf,
        "custom_tags": {"purpose": "flare-bench"},
    }
    if spec["num_workers"] == 0:
        spec["spark_conf"].update({"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"})
        spec["custom_tags"]["ResourceClass"] = "SingleNode"
    return spec


def submit_and_wait(name, task, timeout_s):
    body = {"run_name": name, "timeout_seconds": timeout_s, "tasks": [task]}
    run_id = cli("api", "post", "/api/2.1/jobs/runs/submit", "--json", json.dumps(body))["run_id"]
    print(f"{time.strftime('%H:%M:%S')} {name}: submitted run {run_id}", flush=True)
    # The run's own timeout does not stop this loop if polling itself keeps failing, such as when
    # the CLI profile's token expires, so it has a deadline of its own.
    deadline = time.time() + timeout_s + 900
    while True:
        time.sleep(30)
        if time.time() > deadline:
            raise RuntimeError(f"{name}: run {run_id} not finished {timeout_s + 900}s after submission")
        try:
            run = cli("api", "get", f"/api/2.1/jobs/runs/get?run_id={run_id}")
        except RuntimeError as e:
            print(f"{name}: poll failed, retrying: {e}", flush=True)
            continue
        state = run["state"]
        if state["life_cycle_state"] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return run


def generate():
    task = {"task_key": "generate",
            "new_cluster": cluster("off", workers=0, node_type="m5d.2xlarge"),
            "libraries": [{"pypi": {"package": "duckdb"}}],
            "spark_python_task": {"python_file": f"{args.workspace_dir}/tpch_generate.py", "parameters": [
                "--scale-factor", str(args.scale_factor), "--out", "/local_disk0/tpch",
                "--database", "/local_disk0/tpch.duckdb", "--memory-limit", "16GB",
                "--copy-to", f"{args.bench}/tpch"]}}
    run = submit_and_wait(f"flare-bench generate sf{args.scale_factor}", task, 7200)
    print("generate:", run["state"], flush=True)
    if run["state"].get("result_state") != "SUCCESS":
        sys.exit("generation failed")


def one(config, repeat, order):
    global failures
    name = f"r{repeat}-tpch-{config}{args.tag}"
    result = f"{args.bench}/results/{name}.json"
    task = {"task_key": "tpch", "new_cluster": cluster(config),
            "spark_python_task": {"python_file": f"{args.workspace_dir}/tpch.py", "parameters": [
                "--data", f"{args.bench}/tpch/sf{args.scale_factor}", "--queries", f"{args.bench}/tpch/queries",
                "--result", result, "--minutes", str(args.minutes),
                "--stats-url", "http://127.0.0.1:4319", "--no-stop", "--log-level", args.log_level]}}
    with lock:
        if failures >= args.max_failures:
            print(f"{name}: skipped, {failures} failures already", flush=True)
            return
    run = submit_and_wait(f"flare-bench {name}", task, int(args.minutes * 60 + 3600))
    state = run["state"]
    rec = {"config": config, "workload": "tpch", "repeat": repeat, "order": order,
           "platform": "databricks", "spark_version_key": args.spark_version, "photon": args.photon,
           "node_type": args.node_type, "workers": args.workers, "run_id": run["run_id"],
           "result_state": state.get("result_state"), "state_message": state.get("state_message"),
           "setup_s": run.get("setup_duration", 0) / 1e3, "execution_s": run.get("execution_duration", 0) / 1e3}
    try:
        rec.update(json.loads(cli("fs", "cat", f"dbfs:{result}", parse=False)))
        rec["exit"] = 0 if state.get("result_state") == "SUCCESS" else 1
    except RuntimeError as e:
        rec["exit"] = 1
        rec["error"] = str(e)
    for p in rec.get("passes", []):
        p.pop("queries", None)
    with lock:
        if rec["exit"]:
            failures += 1
        with open(args.out, "a") as fh:
            fh.write(json.dumps(rec) + "\n")
    s = rec.get("at_start", {})
    print(f"{time.strftime('%H:%M:%S')} {name}: {state.get('result_state')} passes={len(rec.get('passes', []))} "
          f"start_cpu driver={s.get('driver_cpu_s')} executors={s.get('executor_cpu_s')} "
          f"seen={s.get('executors_seen')}/{s.get('executors_expected')} "
          f"spans={(rec.get('telemetry') or {}).get('spans')}", flush=True)


if args.generate:
    generate()
configs = args.configs.split(",")
jobs, order = [], 0
for r in range(1, args.repeats + 1):
    k = (r - 1) % len(configs)
    for c in configs[k:] + configs[:k]:
        order += 1
        jobs.append((c, r, order))
with ThreadPoolExecutor(args.parallel) as pool:
    for f in [pool.submit(one, *j) for j in jobs]:
        f.result()
print(f"done, {failures} failures", flush=True)
