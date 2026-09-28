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
- Heavy skew, retry storms and dynamic allocation churn, all asked for in #89. The harness has no
  workloads for them yet.
- Larger clusters.
- Long-running applications, cloud platforms, log export and other job shapes are covered below.

## Long-running applications, Dataproc and Databricks

Run on 2026-09-28 for [#135](https://github.com/Neutrinic/flare/issues/135). The lab results above
are from applications of a few minutes, where a fixed start-up cost looks like a large percentage.
These runs keep one application going for 20 minutes, on managed platforms with server CPUs.

### Method

- **Workload.** TPC-H at scale factor 10, the 22 queries looped for 20 minutes (`tpch.py --minutes 20`),
  read from object storage. Each pass records its time, the CPU of every Spark JVM so far, and the
  driver's heap after a full GC.
- **CPU inside the application.** Managed platforms offer no SSH or `/usr/bin/time`, so each JVM's
  CPU is read from `/proc/<pid>/stat`: the driver's directly, each executor's through a small probe
  job. It counts every thread of the JVM since it started. On the lab it agreed with
  `/usr/bin/time` and `/proc/stat` to within 3%.
- **Fixed and steady cost.** [`report_long.py`](report_long.py) splits each run's JVM CPU into a
  steady rate, per second of query time from the second pass on, and a fixed part: everything above
  that rate, which is JVM start-up and the first pass's warm-up. Total CPU after `t` seconds is then
  about fixed + rate × t, which gives the overhead for any application length.
- **Configurations.** `off`, `agent`, `tasks` as above, and `tasks-lean`: everything traced, with
  the agent's own instrumentations off (see [Lean agent](#lean-agent)). Three repeats each, the
  order rotated.

| | Dataproc | Databricks on AWS |
|---|---|---|
| Runtime | Image 2.2: Spark 3.5, Scala 2.12, Java 11 | DBR 15.4 LTS: Spark 3.5, Scala 2.12, Java 8, Photon off |
| Nodes | Master and 2 workers, n2-standard-4 | Driver and 3 workers, m5d.xlarge |
| Executors | 4 of 2 cores (8 cores) | 3 of 4 cores (12 cores) |
| Data | GCS | Unity Catalog volume on S3 |
| Runs | [raw](results/2026-09-28-dataproc-2.2-tpch-sf10-20min.jsonl) | [raw](results/2026-09-28-databricks-15.4-tpch-sf10-20min.jsonl) |

### Databricks

| | off | agent | tasks | tasks-lean |
|---|---|---|---|---|
| Passes of 22 queries | 7 | 7 | 7 | 7 |
| First pass, s | 253.2 | 267.5 (+5.6%) | 261.3 (+3.2%) | 256.3 (+1.2%) |
| Later passes, mean s | 169.4 | 168.9 (-0.3%) | 172.4 (+1.8%) | 172.5 (+1.8%) |
| Fixed JVM CPU, s | 119 | 173 | 295 | 200 |
| Steady JVM CPU, cores busy | 8.16 | 8.18 (+0.2%) | 8.10 (-0.7%) | 8.09 (-0.8%) |
| Driver heap after GC, first and last pass, MB | 257, 295 | 279, 295 | 275, 309 | 258, 304 |
| Spans exported | 0 | 15,157 | 23,312 | 11,358 |
| Traces sent, gzip | 0 | 1.8 MB | 4.5 MB | 3.8 MB |
| Metrics sent, gzip | 0 | 0.4 MB | 31 MB | 29 MB |

JVM CPU overhead against `off`, for an application running `t` minutes of queries:

| | 2 min | 5 min | 20 min | 60 min |
|---|---|---|---|---|
| agent | +5.1% | +2.3% | +0.8% | +0.4% |
| tasks | +15.4% | +6.2% | +1.1% | about 0 |
| tasks-lean | +6.7% | +2.4% | about 0 | about 0 |

### Dataproc

| | off | agent | tasks | tasks-lean |
|---|---|---|---|---|
| Passes of 22 queries | 4 | 4 | 4 | 4 |
| First pass, s | 377.9 | 379.0 (+0.3%) | 380.3 (+0.6%) | 378.2 (+0.1%) |
| Later passes, mean s | 327.1 | 330.5 (+1.1%) | 329.9 (+0.9%) | 329.4 (+0.7%) |
| Fixed JVM CPU, s | 332 | 435 | 492 | 436 |
| Steady JVM CPU, cores busy | 5.23 | 5.30 (+1.3%) | 5.35 (+2.3%) | 5.26 (+0.6%) |
| Driver heap after GC, first and last pass, MB | 152, 177 | 166, 195 | 198, 226 | 192, 192 |
| Spans exported | 0 | 82,537 | 88,199 | 5,739 |
| Traces sent, gzip | 0 | 5.6 MB | 7.5 MB | 3.6 MB |
| Metrics sent, gzip | 0 | 0.5 MB | 28.6 MB | 25.2 MB |

JVM CPU overhead against `off`, for an application running `t` minutes of queries:

| | 2 min | 5 min | 20 min | 60 min | 240 min |
|---|---|---|---|---|---|
| agent | +11.5% | +6.4% | +2.7% | +1.8% | +1.4% |
| tasks | +18.1% | +10.3% | +4.6% | +3.1% | +2.5% |
| tasks-lean | +11.2% | +5.9% | +2.1% | +1.1% | +0.7% |

Here the steady rate is not free: the agent traces every GCS request as an HTTP `GET` span, about
60 a second while queries read data, and that costs CPU for as long as the application runs. The
lean agent does not trace them.

### Databricks with Photon

`off` and `tasks` only, the same cluster with Photon on, three repeats
([raw](results/2026-09-28-databricks-15.4-photon-tpch-sf10-20min.jsonl)).

| | off | tasks |
|---|---|---|
| Passes of 22 queries | 26 | 25 |
| Later passes, mean s (range over repeats) | 43.2 (42.9 to 46.0) | 45.4 (44.0 to 45.9) |
| Fixed JVM CPU, s | 284 | 427 |
| JVM CPU per pass once warm, core-seconds | 291 | 292 |
| Task spans exported | 0 | about 30,000, then the span cap |
| Metrics sent, gzip | 0 | 41 to 59 MB |

- **Photon runs the queries 3.5 times faster, and Flare's cost keeps the same shape:** a fixed
  start-up cost (143 s of CPU) and nothing measurable once warm. Pass times overlap between the two
  configurations.
- **The span cap ends task spans within 20 minutes.** Photon runs more tasks per minute, and each
  executor stops creating task spans at `FLARE_MAX_SPANS_PER_TRACE` (10,000 per executor, #101).
  For an application that runs for hours, such as an all-purpose cluster that is one trace for its
  whole life, task spans stop early in the run; job, stage and SQL spans continue.
- Metric volume is higher than without Photon, for the same reason: more stages per minute, each a
  new `stage.id` series before #136.

### What the long runs show

- **Most of the CPU overhead is a fixed cost per application.** It is start-up: the agent
  instrumenting classes as each JVM loads them, and the first pass's warm-up. `tasks` spends 176 s
  of CPU more than `off` before settling on Databricks and 160 s on Dataproc, across four and five
  JVMs. On Databricks that is 15% of a two-minute application and 1% of a twenty-minute one.
- **Once warm, Flare itself costs about nothing; the default agent can.** On Databricks, JVM CPU
  per second is within 1% of `off` for every configuration. On Dataproc the agent alone adds 1.3%
  and `tasks` 2.3%, because the agent traces every GCS request; with the lean agent it is 0.6%.
  CPU inside tasks does not change on either platform.
- **Query time once warm is within 2%** on both platforms.
- **Driver memory does not grow.** Heap after a full GC was flat across 20 minutes in every
  configuration.
- **The agent's own instrumentation is most of the trace volume.** With the default agent, Spark
  reading its input became spans: 8,900 `S3.GetObject` and 3,000 HTTP `PUT` spans per Databricks
  run, 82,000 HTTP `GET` spans per Dataproc run, outnumbering Flare's own. The lean agent drops
  them, removes about half of the fixed cost on Databricks (200 s of CPU against 295 s) and most
  of the steady cost on Dataproc.
- **Metric volume grew with the application's age**, 25 to 31 MB of metrics (gzip) in a 22-minute
  application on either platform. The cause was the `stage.id` label, fixed in
  [#136](https://github.com/Neutrinic/flare/issues/136); see [Metric volume](#metric-volume).
- **No listener-bus drops** in any run.

### Job shapes

TPC-H is SQL. These lab runs cover the other shapes an application takes: many small jobs, and
RDD code with no SQL at all. Spark 4.0.4 on the lab, three repeats, raw data
[here](results/2026-09-28-lab-spark-4.0.4-job-shapes.jsonl).

- **`many-jobs-2000`** ([`many_jobs.py`](many_jobs.py)): 2,000 DataFrame counts over a small cached
  table in one application, like a notebook or an ETL loop firing one action after another. Each
  is its own SQL execution, job and stage, so this is the heaviest case for Flare's driver side.
- **`rdd-groupby`** and **`rdd-tc`**: Spark's own Scala examples `GroupByTest` (a shuffle-heavy RDD
  pipeline) and `SparkTC` (a transitive closure, dozens of small iterative jobs). JVM only, with a
  Scala driver and no SQL executions.

| | off | agent | tasks | tasks-lean |
|---|---|---|---|---|
| **many-jobs-2000**, time for the 2,000 jobs | 135.8 s | 136.5 (+0.5%) | 135.9 (+0.1%) | 136.6 (+0.6%) |
| driver CPU | 217 s | 238 | 246 | 232 |
| cluster CPU | 435 s | 519 | 542 | 490 |
| spans | 0 | 0 | 20,017 | 20,017 |
| **rdd-groupby**, application time | 19.0 s | 27.1 | 27.6 | 23.9 |
| cluster CPU | 168 s | 233 | 238 | 208 |
| **rdd-tc**, application time | 17.7 s | 25.3 | 26.1 | 22.3 |
| cluster CPU | 154 s | 224 | 232 | 195 |

- **Per job, Flare costs about 4 ms of driver CPU** (246 s against 238 s with the agent alone,
  over 2,000 SQL executions, including plan capture) and **no measurable time**: the 2,000 jobs
  took as long traced as not.
- **RDD applications are traced the same way.** The two RDD examples are 20-second applications,
  so their overhead is the fixed start-up cost: about 65 s of CPU for the agent and 5 to 9 s more
  for Flare, of which the lean agent saves about 30 s.
- No listener-bus drops in any of the 36 runs.

### Lean agent

The agent instruments far more than Spark: HTTP clients, the AWS and GCS SDKs, JDBC, Kafka and
more. In a Spark JVM those mostly trace Spark reading its own files. To keep only Flare:

```text
-Dotel.instrumentation.common.default-enabled=false
-Dotel.instrumentation.opentelemetry-api.enabled=true
-Dotel.instrumentation.flare-spark.enabled=true
```

The second line is required: it bridges Flare's calls to the agent's SDK, and without it nothing at
all is exported. Add `-Dotel.instrumentation.log4j-appender.enabled=true` to keep log export, and
re-enable any instrumentation you want by name, such as `-Dotel.instrumentation.jdbc.enabled=true`.

### Metric volume

Up to #136, task and stage metrics carried `stage.id`, which is new for every stage. With
cumulative export every series is re-sent each time, so each export grew for as long as the
application ran: 25 to 31 MB of metrics (gzip) in a 22-minute application on either platform. With
#136, on a 10-minute lab TPC-H loop, each metric export stayed at 10 to 13 KB from start to end,
and the whole application sent 1.6 MB (gzip) of metrics
([raw](results/2026-09-28-lab-spark-4.0.4-fix136.jsonl)).

What remains is how often executors export. Each flushes about a second after its last task ends
(1.3.0), so it can end a run on a cluster that kills executors without warning. Between the stages
of a query, that is every few seconds: 461 metric exports in twelve minutes, against about 36 from
the 60-second periodic export alone. Each is small, but a backend that bills per data point per
minute sees each series many times a minute.

### Log export

The agent exports logs by default. The runs above had it off; these compare `tasks` with and
without it, at Spark's default `INFO` level:

| | Lab, Spark 4.0.4, SF5 | Databricks 15.4, SF10 |
|---|---|---|
| JVM CPU per second of queries, logs off and on | 6.28, 6.28 cores (+0.1%) | 8.25, 8.31 cores (+0.7%) |
| Later passes, logs off and on | 94.7 s, 95.5 s | 174.0 s, 170.1 s |
| Log records per minute | 12,800 | 7,000 |
| Size per record, uncompressed | 190 bytes | 755 bytes |
| Sent per hour, gzip (uncompressed) | 39 MB (140 MB) | 63 MB (304 MB) |
| Per day, always on, gzip | about 0.9 GB | about 1.5 GB |

Three 10-minute runs per configuration on the lab, three 20-minute runs on Databricks. Raw data:
[lab](results/2026-09-28-lab-spark-4.0.4-log-export.jsonl),
[Databricks](results/2026-09-28-databricks-15.4-log-export.jsonl). On the lab, the lean agent with
`log4j-appender` re-enabled exported the same logs (12,900 records a minute), which confirms that
flag. Databricks' records are four times larger: its Log4j configuration and MDC add many
attributes to each line.

Capturing logs costs no measurable CPU or query time. Their volume is the cost: at `INFO`, Spark
logs every task's start and end, so an always-on application sends gigabytes of logs a day, far
more than its traces or metrics. Raise Spark's log level to `WARN`, or filter in a collector.

### Reproducing on Dataproc and Databricks

- **Dataproc** ([`cloud/dataproc/`](cloud/dataproc/)): `init.sh` is an initialization action that
  installs the agent, Flare and the harness; `launch.py`, submitted as a PySpark job, starts
  `run.sh` detached on the master, which generates TPC-H into the bucket if needed and runs
  `run_matrix.py` in client mode. Progress and results are copied to `gs://<bucket>/bench/out/`.
- **Databricks** ([`cloud/databricks/`](cloud/databricks/)): `dbx_matrix.py` runs from any machine
  with the Databricks CLI and submits one job cluster per run, since JVM options are fixed when a
  cluster starts. `init.sh` installs the JARs and starts `otlp_sink.py` on the driver. Results land
  in a volume and are appended locally.
- Summarise either with `python report_long.py <results.jsonl>`.
