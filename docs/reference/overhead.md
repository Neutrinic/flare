# Overhead

What Flare costs a Spark application, measured against the same job with no agent at all. The
method, every table and the raw results are in
[`benchmarks/README.md`](https://github.com/Neutrinic/flare/blob/main/benchmarks/README.md).

## In short

- **Most of the cost is fixed, at start-up.** The agent instruments classes as each JVM loads them.
  With every task traced that was about 15 s of CPU per JVM on the lab (Spark 4.0.4), and more on
  the platforms with larger runtimes: 176 s across four JVMs on Databricks and 160 s across five on
  Dataproc, including the first queries' warm-up. It does not grow with the application, so it
  matters for short jobs and fades on long ones.
- **Once warm, Flare adds nothing measurable.** CPU inside tasks is unchanged, query time is within
  2%, and driver memory stays flat.
- **The agent's own instrumentation can cost more than Flare.** With all of it on, it traced every
  S3 or GCS request, about 2% CPU on Dataproc while data was read. Since 1.3.0 Flare turns it off
  by default; see [Agent instrumentation](../configuration/volume.md#agent-instrumentation).
- **No Spark listener events were dropped** in the 135 runs that checked for it, up to 100,000 tasks
  in one stage.

## By application length

JVM CPU across the cluster against no agent, every task traced, TPC-H at scale factor 10:

| Platform | 2 min | 20 min | 60 min |
|---|---|---|---|
| Databricks 15.4, all agent instrumentation | +15% | +1% | about 0 |
| Databricks 15.4, agent instrumentation off | +7% | about 0 | about 0 |
| Dataproc 2.2, all agent instrumentation | +18% | +4.6% | +3.1% |
| Dataproc 2.2, agent instrumentation off | +11% | +2.1% | +1.1% |

"Off" is close to 1.3.0's defaults, which also keep thread-pool context, `@WithSpan` and JVM
metrics on.

Photon runs the same queries 3.5 times faster, and the cost keeps the same shape: a fixed start-up
cost and nothing measurable once warm.

## Per task and per job

On a three-node lab cluster, Spark 4.0.4:

| | CPU | Wall time |
|---|---|---|
| Per task, a span for every task | about 0.6 ms | about 54 µs |
| Per task, default granularity (no task spans) | about 0.3 ms | about 26 µs |
| Per job, 2,000 small DataFrame jobs in one application | about 4 ms on the driver | none measurable |

Per-task figures vary by about 0.1 ms between sessions.

## Logs

Capturing logs costs no measurable CPU. Their volume is the cost; see
[Telemetry volume](../configuration/volume.md#logs).

## What was measured

- **Workloads:** TPC-H (22 queries, one pass and 20-minute loops), one stage of up to 100,000 tasks,
  2,000 small jobs, and two Scala RDD applications. The TPC-H workload is derived from TPC-H (data
  and queries generated with DuckDB's `tpch` extension). These figures measure Flare's overhead;
  they aren't TPC-H benchmark results and aren't comparable with published TPC-H results.
- **Platforms:** a three-node lab, Dataproc 2.2, and Databricks 15.4 on AWS with and without Photon.
- **Configurations:** no agent, the agent alone, Flare at its default granularity, Flare with every
  task traced, and every task traced with the agent's own instrumentation off.

Not yet measured: skew, retry storms, dynamic allocation churn and larger clusters.
