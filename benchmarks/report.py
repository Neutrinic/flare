"""Summarise run_matrix.py output as markdown tables.

    python report.py results.jsonl

Every figure is the median over repeats. Overheads are relative to the `off` configuration of the
same workload. The "10% sampled" column is 0.9 x tasks-unsampled + 0.1 x tasks: the expected cost
when a tenth of applications are traced.
"""
import json
import statistics
import sys
from collections import defaultdict

CONFIGS = ["off", "agent", "stages", "tasks-unsampled", "tasks"]

runs = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
present = {r["config"] for r in runs}
CONFIGS = [c for c in CONFIGS if c in present] + sorted(present - set(CONFIGS))  # e.g. tasks-uncapped
SAMPLED = {"tasks-unsampled", "tasks"} <= present
failed = [r for r in runs if r["exit"] != 0]
runs = [r for r in runs if r["exit"] == 0]
by = defaultdict(list)
for r in runs:
    by[(r["workload"], r["config"])].append(r)


def median(workload, config, key):
    vals = [v for v in (key(r) for r in by.get((workload, config), [])) if v is not None]
    return statistics.median(vals) if vals else None


def pct(value, base):
    return "n/a" if value is None or not base else f"{100 * (value - base) / base:+.1f}%"


def sampled(workload, key, p=0.1):
    unsampled, full = median(workload, "tasks-unsampled", key), median(workload, "tasks", key)
    return None if unsampled is None or full is None else (1 - p) * unsampled + p * full


def workload_order(w):
    head, _, n = w.rpartition("-")
    if w == "tpch":
        return (1, w, 0)
    return (0, head, int(n)) if n.isdigit() else (0, w, 0)


def telemetry_kib(r):
    t = r["telemetry"]
    return (t["traces"]["wire_bytes"] + t["metrics"]["wire_bytes"]) / 1024


metrics = [
    ("Job time", lambda r: r.get("seconds"), "s"),
    ("Application time", lambda r: r.get("wall_s"), "s"),
    ("Driver CPU", lambda r: r.get("driver_cpu_s"), "s"),
    ("Cluster CPU", lambda r: r.get("cluster_cpu_s"), "s"),
    ("Driver peak RSS", lambda r: r.get("driver_max_rss_mb"), "MB"),
]

for w in sorted({r["workload"] for r in runs}, key=workload_order):
    tasks = median(w, "off", lambda r: r.get("tasks"))
    print(f"\n### {w}" + (f" ({tasks:,.0f} tasks per run)" if tasks else "") + "\n")
    print("| | " + " | ".join(CONFIGS) + (" | 10% sampled |" if SAMPLED else " |"))
    print("|---|" + "---|" * (len(CONFIGS) + SAMPLED))
    for label, key, unit in metrics:
        base = median(w, "off", key)
        cells = []
        for c in CONFIGS:
            v = median(w, c, key)
            if v is None:
                cells.append("n/a")
            elif c == "off":
                cells.append(f"{v:.1f} {unit}")
            else:
                cells.append(f"{v:.1f} ({pct(v, base)})")
        if SAMPLED:
            cells.append(pct(sampled(w, key), base))
        print(f"| {label} | " + " | ".join(cells) + " |")
    spans = [median(w, c, lambda r: r["telemetry"]["spans"]) for c in CONFIGS]
    print("| Spans exported | " + " | ".join("n/a" if s is None else f"{s:,.0f}" for s in spans) + (" | |" if SAMPLED else " |"))
    sent = [median(w, c, telemetry_kib) for c in CONFIGS]
    print("| Telemetry sent, gzip | " + " | ".join("n/a" if b is None else f"{b:,.0f} KiB" for b in sent) + (" | |" if SAMPLED else " |"))
    print(f"\nRepeats per cell: {len(by.get((w, 'off'), []))}")

dropped = [r for r in runs if r.get("dropped_event_lines")]
print(f"\nListener-bus drops: {len(dropped)} of {len(runs)} runs logged dropped events.")
for r in dropped:
    print(f"- {r['workload']} / {r['config']} r{r['repeat']}: {r['dropped_event_lines'][0][:160]}")
if failed:
    print(f"\n{len(failed)} run(s) failed and are excluded:")
    for r in failed:
        print(f"- {r['workload']} / {r['config']} r{r['repeat']}: exit {r['exit']}")
