# Overhead benchmark

What Flare costs a Spark application: time, CPU, driver memory and telemetry volume, measured
against the same job with no agent at all (#89).

## Configurations

Five configurations, from nothing to everything, so each layer's cost can be told apart, and a sixth
for measuring the per-task cost of task spans:

| Config | What runs |
|---|---|
| `off` | No agent, no Flare. The baseline |
| `agent` | The OpenTelemetry Java agent alone, with no Flare. Separates the agent's own cost from Flare's |
| `stages` | Flare at its default granularity: application, SQL, job and stage spans, no task spans. Metrics on |
| `tasks-unsampled` | Task spans configured, but the application not sampled (`parentbased_traceidratio` at `0`). What an unsampled application costs |
| `tasks` | Everything traced, including one span per task. Metrics on |
| `tasks-uncapped` | As `tasks`, with the span cap raised so every task gets a span, however many there are |

The cost of tracing a fraction `p` of applications is `(1 - p) × tasks-unsampled + p × tasks`,
shown as the **10% sampled** column. Running at 10% directly would leave most repeats unsampled
and the average would be noise.

## Workloads

- **`tasks-N`** ([`many_tasks.py`](many_tasks.py)): one stage of `N` near-empty tasks, each writing
  100 rows to the `noop` sink. JVM only, no shuffle. Tasks this small make any per-task cost show
  in the job time. At 100,000 tasks the span cap (`FLARE_MAX_SPANS_PER_TRACE`, 10,000) stops task
  spans partway, so that run also covers the circuit breaker. The cap applies per executor, not per
  trace ([#101](https://github.com/Neutrinic/flare/issues/101)): three executors emitted 30,000.
  `tasks-uncapped` lifts the cap, for a per-task cost that is all task spans.
- **`tpch`** ([`tpch.py`](tpch.py)): the 22 TPC-H queries at scale factor 5, one pass. A realistic
  mix of scans, joins, shuffles and spills. Data and queries from
  [`tpch_generate.py`](tpch_generate.py). Not an audited TPC-H run; a fixed query mix.

## What is measured

| Measure | How |
|---|---|
| Job time | For `tasks-N`, the measured stage only, timed inside the application after a small warm-up job. For `tpch`, the 22 queries from the first one, with no warm-up in that application |
| Application time | `spark-submit` start to exit, including JVM and executor start-up |
| Driver CPU | User plus system CPU of the driver process tree, from `/usr/bin/time -v` |
| Driver peak RSS | From `/usr/bin/time -v` |
| Cluster CPU | Busy CPU across every node over the run, from `/proc/stat` before and after |
| Spans, telemetry sent | Counted by [`otlp_sink.py`](otlp_sink.py), a local OTLP receiver: bytes as sent (gzip) and spans decoded from the protobuf |
| Listener-bus drops | Spark's own "Dropped … events" warnings in the driver log |

Cluster CPU includes everything running on the nodes during the run, so it is only compared
between configurations measured the same way, never read as an absolute.

## Method

- Every configuration runs every workload, three times. The configuration order rotates each
  repeat, so drift over the session (thermal, page cache) falls on all of them alike.
- One unrecorded `off` run per workload first, to warm the page cache and the hosts. It does not
  warm the JVMs of later applications, which each start cold.
- Figures are medians over the repeats. Overheads are relative to `off` for the same workload.
- Telemetry goes to the sink on the driver host, over the LAN, with gzip, as a collector would.

## Reproducing

```bash
# On the driver host, with the TPC-H data at the same path on every node:
python otlp_sink.py --port 4318 &
python run_matrix.py \
  --spark-submit "spark-submit --master spark://<master>:7077" \
  --agent /opt/flare/opentelemetry-javaagent.jar --flare /opt/flare/flare-spark.jar \
  --sink http://<driver-host>:4318 \
  --event-dir /tmp/flare-bench/events --out /tmp/flare-bench/results.jsonl \
  --tpch-data /data/tpch/sf5 --tpch-queries /data/tpch/queries \
  --nodes local,<worker1>,<worker2> --ssh-key ~/.ssh/<key> --repeats 3
python report.py /tmp/flare-bench/results.jsonl
```

Client deploy mode is required: the driver has to run on the host where `run_matrix.py` times it.

## Results

Run on 2026-09-28, in two sessions. Raw data: [`results/2026-09-28-lab-spark-4.0.4.jsonl`](results/2026-09-28-lab-spark-4.0.4.jsonl),
the full matrix; and [`results/2026-09-28-lab-spark-4.0.4-uncapped.jsonl`](results/2026-09-28-lab-spark-4.0.4-uncapped.jsonl),
`off`, `agent` and `tasks-uncapped` at 10,000 and 100,000 tasks.

### Environment

| | |
|---|---|
| Spark | 4.0.4, standalone, client deploy mode, Scala 2.13, Java 17.0.20 |
| Flare | `1.2.0+16-630eaaa7-SNAPSHOT`, the 1.3.0 release candidate (`flare-spark-4-0_2.13`) |
| Agent | OpenTelemetry Java agent 2.30.0, exporting OTLP `http/protobuf` with gzip |
| Nodes | Three machines on 1 GbE: Intel i5-8500T (6 threads), Intel i7-6700T (8 threads), AMD A10-9700E (4 threads), 8 GB RAM each |
| Executors | One per node, 2 cores each, 6 cores in total. The driver runs on the i5 node |
| Data | TPC-H scale factor 5, Parquet, on local disk on every node |

Small, heterogeneous hardware. Absolute times do not transfer to other clusters; the relative
overheads and the per-task costs are what to read.

### Summary

| | Per task, CPU | Per task, job time | Per application, CPU | Per application, time |
|---|---|---|---|---|
| Agent only | 0.16 ms | 3 µs | 58 s | 4.4 s |
| Flare, stage spans | 0.32 ms | 26 µs | 64 s | 5.5 s |
| Flare, task spans, application not sampled | 0.39 ms | 28 µs | 64 s | 5.3 s |
| Flare, task spans, all traced | 0.59 ms | 54 µs | 67 s | 5.5 s |

- **Per-task** figures are the difference between the 100,000-task and 10,000-task runs, over the
  90,000 extra tasks, minus the same difference with no agent. CPU is summed over the whole cluster;
  job time is wall-clock time with 6 cores working in parallel.
- **The all-traced row comes from the second session**, against that session's own `off` runs. In
  the first, the span cap stopped task spans at 30,007 of 100,000, so its slope (0.53 ms, 44 µs)
  was mostly tasks with no span. Within the second session, task spans on every task add 0.25 ms of
  CPU and 32 µs of job time per task on top of the agent alone.
- **Read per-task figures to about ±0.1 ms of CPU and ±20 µs of job time.** They are small
  differences between large totals, and they move between sessions: the agent alone came to 0.16 ms
  and 3 µs in the first session, 0.34 ms and 22 µs in the second.
- **Per-application** figures are the 1,000-task run minus the same run with no agent: a fixed
  start-up cost, mostly the agent instrumenting classes as each of the four JVMs (driver and three
  executors) starts, about 15 s of CPU per JVM.
- **On the realistic workload the cost is small:** TPC-H queries took 1.8% longer with every span
  traced, of which 0.9% is the agent alone. Cluster CPU rose 12%, but CPU inside tasks (from the
  event logs) was 423 s with no agent and 421 s with everything traced: the extra CPU is the fixed
  per-JVM cost, spent outside tasks, and shrinks as a share of longer applications.
- **No listener-bus drops** in any of the 78 runs, including 100,000-task stages with a span for
  every task.
- **Telemetry volume:** a task span costs about 37 bytes after gzip, 3.6 MiB for 100,000. TPC-H spans average about 260
  bytes, because SQL spans carry plans; the plan caps bound that.
- **Sampling saves export, not work.** An unsampled application still runs Flare's per-task hooks,
  so it costs most of what a traced one does.

### Per workload

Medians of three runs. Percentages are against `off`. Cluster CPU counts everything busy on the
three nodes during the run. Driver peak RSS varied by about ±10% between repeats of the same
configuration, so differences within that range are noise.

#### TPC-H, 22 queries (about 1,000 tasks)

| | off | agent | stages | tasks-unsampled | tasks | 10% sampled |
|---|---|---|---|---|---|---|
| Query time | 111.0 s | 112.0 (+0.9%) | 113.0 (+1.7%) | 111.8 (+0.7%) | 113.0 (+1.8%) | +0.8% |
| Application time | 121.2 s | 127.6 (+5.2%) | 128.8 (+6.2%) | 127.9 (+5.5%) | 128.8 (+6.2%) | +5.6% |
| Driver CPU | 84.8 s | 101.2 (+19.3%) | 100.7 (+18.8%) | 100.5 (+18.5%) | 103.0 (+21.5%) | +18.8% |
| Cluster CPU | 798.0 s | 884.1 (+10.8%) | 894.7 (+12.1%) | 879.0 (+10.1%) | 892.2 (+11.8%) | +10.3% |
| Driver peak RSS | 1325 MB | 1233 (-7.0%) | 1330 (+0.3%) | 1302 (-1.8%) | 1284 (-3.1%) | -1.9% |
| Spans exported | 0 | 0 | 373 | 0 | 1,380 | |
| Telemetry sent, gzip | 0 KiB | 19 KiB | 727 KiB | 617 KiB | 1,074 KiB | |

#### 100,000 tasks

| | off | agent | stages | tasks-unsampled | tasks | 10% sampled |
|---|---|---|---|---|---|---|
| Job time | 42.3 s | 43.8 (+3.5%) | 46.0 (+8.6%) | 46.6 (+10.1%) | 47.8 (+13.0%) | +10.4% |
| Application time | 52.7 s | 58.4 (+10.8%) | 60.7 (+15.1%) | 61.3 (+16.2%) | 62.2 (+18.0%) | +16.4% |
| Driver CPU | 116.8 s | 130.2 (+11.4%) | 135.4 (+15.9%) | 137.0 (+17.2%) | 138.5 (+18.5%) | +17.4% |
| Cluster CPU | 408.0 s | 486.9 (+19.4%) | 509.2 (+24.8%) | 521.0 (+27.7%) | 535.6 (+31.3%) | +28.1% |
| Driver peak RSS | 818 MB | 909 (+11.1%) | 1002 (+22.5%) | 864 (+5.6%) | 907 (+10.8%) | +6.1% |
| Spans exported | 0 | 0 | 7 | 0 | 30,007 | |
| Telemetry sent, gzip | 0 KiB | 7 KiB | 23 KiB | 20 KiB | 1,143 KiB | |

`tasks` stopped at the span cap here. The uncapped rerun is below.

#### 10,000 tasks

| | off | agent | stages | tasks-unsampled | tasks | 10% sampled |
|---|---|---|---|---|---|---|
| Job time | 11.1 s | 12.3 (+11.0%) | 12.4 (+11.8%) | 12.9 (+15.9%) | 12.7 (+14.1%) | +15.7% |
| Application time | 21.1 s | 26.4 (+25.5%) | 27.1 (+28.8%) | 28.1 (+33.2%) | 27.2 (+29.1%) | +32.8% |
| Driver CPU | 55.8 s | 72.9 (+30.6%) | 74.4 (+33.2%) | 74.3 (+33.2%) | 72.2 (+29.4%) | +32.8% |
| Cluster CPU | 177.3 s | 242.2 (+36.6%) | 249.6 (+40.8%) | 255.0 (+43.8%) | 256.9 (+44.9%) | +43.9% |
| Driver peak RSS | 587 MB | 635 (+8.1%) | 642 (+9.2%) | 653 (+11.1%) | 636 (+8.2%) | +10.8% |
| Spans exported | 0 | 0 | 7 | 0 | 10,017 | |
| Telemetry sent, gzip | 0 KiB | 6 KiB | 21 KiB | 14 KiB | 397 KiB | |

#### 1,000 tasks

| | off | agent | stages | tasks-unsampled | tasks | 10% sampled |
|---|---|---|---|---|---|---|
| Job time | 2.8 s | 3.1 (+12.0%) | 3.3 (+17.9%) | 3.3 (+18.1%) | 3.4 (+21.8%) | +18.5% |
| Application time | 12.7 s | 17.1 (+34.7%) | 18.2 (+43.6%) | 17.9 (+41.5%) | 18.2 (+43.4%) | +41.7% |
| Driver CPU | 27.3 s | 44.0 (+61.1%) | 45.9 (+67.9%) | 45.1 (+64.9%) | 45.0 (+64.5%) | +64.9% |
| Cluster CPU | 89.3 s | 147.3 (+64.9%) | 153.8 (+72.1%) | 152.9 (+71.2%) | 156.2 (+74.8%) | +71.5% |
| Driver peak RSS | 526 MB | 556 (+5.6%) | 612 (+16.2%) | 655 (+24.5%) | 563 (+7.0%) | +22.7% |
| Spans exported | 0 | 0 | 7 | 0 | 1,017 | |
| Telemetry sent, gzip | 0 KiB | 6 KiB | 22 KiB | 16 KiB | 62 KiB | |

#### Span cap raised, second session

| | off | agent | tasks-uncapped |
|---|---|---|---|
| **10,000 tasks** | | | |
| Job time | 10.8 s | 12.2 (+12.2%) | 13.1 (+21.2%) |
| Application time | 20.7 s | 26.3 (+27.1%) | 28.2 (+36.4%) |
| Driver CPU | 53.5 s | 72.0 (+34.5%) | 74.6 (+39.5%) |
| Cluster CPU | 174.0 s | 241.7 (+38.9%) | 260.3 (+49.6%) |
| Driver peak RSS | 589 MB | 609 (+3.4%) | 700 (+18.9%) |
| Spans exported | 0 | 0 | 10,017 |
| Telemetry sent, gzip | 0 KiB | 6 KiB | 402 KiB |
| **100,000 tasks** | | | |
| Job time | 41.5 s | 44.7 (+7.9%) | 48.6 (+17.3%) |
| Application time | 51.2 s | 58.5 (+14.1%) | 62.5 (+22.0%) |
| Driver CPU | 113.2 s | 136.1 (+20.2%) | 138.8 (+22.5%) |
| Cluster CPU | 397.0 s | 495.2 (+24.7%) | 536.0 (+35.0%) |
| Driver peak RSS | 898 MB | 923 (+2.8%) | 898 (+0.0%) |
| Spans exported | 0 | 0 | 100,017 |
| Telemetry sent, gzip | 0 KiB | 6 KiB | 3,679 KiB |

The small workloads show large percentages because a 3-second job is dwarfed by the fixed start-up
cost above; read them for the per-task and per-application figures, not as typical overhead.

### Not covered

- One million tasks: the harness runs it (`--workloads tasks-1000000`), but it was not run here.
- Heavy skew, retry storms, dynamic allocation churn and long-lived applications, all asked for in
  #89. The harness has no workloads for them yet.
- Other Spark versions, cluster managers and larger clusters.
- Log export, which was off. Its volume depends on Spark's log level, not on Flare.
