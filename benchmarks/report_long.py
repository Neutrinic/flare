"""Summarise long-running TPC-H results (tpch.py --minutes) as markdown.

    python report_long.py results.jsonl [more.jsonl ...]

Reads run_matrix.py and dbx_matrix.py output alike. Each run's JVM CPU (driver plus every
executor, start-up included) is split into:

    steady   CPU per second of query time from the second pass on, once the JVMs are warm
    fixed    everything above that rate: JVM start-up, and the warm-up the first pass carries
             (class loading and instrumentation, JIT compilation)

so total CPU after t seconds of queries is about fixed + steady x t. The overhead of a
configuration against `off` then follows for any application length, which a single percentage
from one run length cannot show. Figures are medians over repeats.
"""
import json
import statistics
import sys
from collections import defaultdict

ORDER = ["off", "agent", "stages", "tasks", "tasks-lean", "tasks-unsampled", "tasks-uncapped"]

runs = []
for path in sys.argv[1:]:
    for line in open(path, encoding="utf-8"):
        if line.strip():
            r = json.loads(line)
            if r.get("exit") == 0 and r.get("passes") and r.get("at_start"):
                runs.append(r)
by = defaultdict(list)
for r in runs:
    by[r["config"]].append(r)
configs = [c for c in ORDER if c in by] + sorted(set(by) - set(ORDER))


def jvm(sample):
    return sample["driver_cpu_s"] + sample["executor_cpu_s"]


def metrics(r):
    passes = r["passes"]
    query_s = sum(p["seconds"] for p in passes)
    end = jvm(passes[-1])
    if len(passes) > 1:
        rate = (end - jvm(passes[0])) / sum(p["seconds"] for p in passes[1:])
    else:
        rate = (end - jvm(r["at_start"])) / query_s
    tel = r.get("telemetry") or {}
    return {
        "fixed_cpu": end - rate * query_s,
        "ready_cpu": jvm(r["at_start"]),
        "ready_driver_cpu": r["at_start"]["driver_cpu_s"],
        "ready_executor_cpu": r["at_start"]["executor_cpu_s"],
        "ready_s": r["at_start"]["elapsed_s"],
        "steady_rate": rate,
        "task_rate": sum(p.get("task_cpu_s", 0) for p in passes[1:] or passes)
                     / sum(p["seconds"] for p in passes[1:] or passes),
        "passes": len(passes),
        "first_pass_s": passes[0]["seconds"],
        "pass_s": statistics.mean(p["seconds"] for p in passes[1:] or passes),
        "heap_first": passes[0].get("driver_heap_mb"),
        "heap_last": passes[-1].get("driver_heap_mb"),
        "spans": tel.get("spans"),
        "telemetry_kib": (tel.get("traces", {}).get("wire_bytes", 0)
                          + tel.get("metrics", {}).get("wire_bytes", 0)) / 1024 if tel else None,
    }


med = {}
for c in configs:
    ms = [metrics(r) for r in by[c]]
    med[c] = {k: statistics.median([m[k] for m in ms if m[k] is not None])
              if any(m[k] is not None for m in ms) else None for k in ms[0]}
    med[c]["n"] = len(ms)
base = med.get("off")


def pct(v, b):
    return f"{100 * (v - b) / b:+.1f}%" if b else "n/a"


def cell(c, key, fmt, compare):
    v = med[c][key]
    if v is None:
        return "n/a"
    if not compare or base is None or c == "off" or base[key] is None:
        return fmt.format(v)
    return f"{fmt.format(v)} ({pct(v, base[key])})"


print("| | " + " | ".join(configs) + " |")
print("|---|" + "---|" * len(configs))
rows = [
    ("Runs", "n", "{:.0f}", False),
    ("Passes of 22 queries", "passes", "{:.0f}", False),
    ("First pass, s", "first_pass_s", "{:.1f}", True),
    ("Later passes, mean s", "pass_s", "{:.1f}", True),
    ("Fixed JVM CPU, s", "fixed_cpu", "{:.0f}", True),
    ("Steady JVM CPU, cores busy", "steady_rate", "{:.2f}", True),
    ("of which inside tasks", "task_rate", "{:.2f}", True),
    ("Session ready, s", "ready_s", "{:.1f}", True),
    ("JVM CPU by then, driver, s", "ready_driver_cpu", "{:.0f}", True),
    ("JVM CPU by then, executors, s", "ready_executor_cpu", "{:.0f}", True),
    ("Driver heap after GC, first pass, MB", "heap_first", "{:.0f}", False),
    ("Driver heap after GC, last pass, MB", "heap_last", "{:.0f}", False),
    ("Spans exported", "spans", "{:,.0f}", False),
    ("Telemetry sent, gzip, KiB", "telemetry_kib", "{:,.0f}", False),
]
for label, key, fmt, compare in rows:
    print(f"| {label} | " + " | ".join(cell(c, key, fmt, compare) for c in configs) + " |")

if base:
    print("\nJVM CPU overhead against `off` for an application running t minutes of queries,"
          " from fixed + steady rate x t:\n")
    lengths = [2, 5, 20, 60, 240]
    print("| | " + " | ".join(f"{t} min" for t in lengths) + " |")
    print("|---|" + "---|" * len(lengths))
    for c in configs:
        if c == "off":
            continue
        cells = []
        for t in lengths:
            s = 60 * t
            cells.append(pct(med[c]["fixed_cpu"] + med[c]["steady_rate"] * s,
                             base["fixed_cpu"] + base["steady_rate"] * s))
        print(f"| {c} | " + " | ".join(cells) + " |")
