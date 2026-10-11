# Troubleshooting

Start by adding `-Dotel.traces.exporter=otlp,logging-otlp` to the driver and executor options. Every
export is then also written to standard error as OTLP JSON, which separates "Flare did not produce
it" from "the backend did not receive it".

## Nothing is exported at all

- **Java 8 or 11 with Flare 1.2.0 or earlier.** The agent could not load Flare's classes and
  disabled its SDK. The driver log shows `UnsupportedClassVersionError` for
  `io.flare.spark.config.FlareAutoConfig`. Upgrade; see [Upgrading](upgrading.md#java-8-and-11-work).
- **The agent is not attached.** Check the JVM's start-up log for
  `opentelemetry-javaagent - version:`. If it is missing, the `-javaagent` option did not reach
  that JVM, or the path was wrong on that machine.
- **The exporter cannot reach the backend.** The agent logs `Failed to export spans`. Check the
  endpoint, the protocol (`grpc` on 4317, `http/protobuf` on 4318) and, on private networks,
  outbound access.

## The driver fails at start-up

- **`NoClassDefFoundError: io/opentelemetry/context/ImplicitContextKeyed`**: Flare 1.2.0 or earlier
  without the three OpenTelemetry API JARs on the classpath. Upgrade to 1.3.0, which bundles them.
- **`ClassNotFoundException: io.flare.spark.plugin.FlareSparkPlugin`**: the Flare JAR is not on
  Spark's classpath. Check `extraClassPath`, or the platform's own JAR directory.
- **`NoClassDefFoundError: scala/Serializable`**: a Scala 2.12 JAR on a Scala 2.13 runtime. See
  [Choosing a JAR](getting-started/choosing-a-jar.md).
- **S3 access broke after adding Flare on EMR**: `extraClassPath` was replaced instead of appended
  to. See [EMR](deploying/emr.md#append-to-the-classpath).

## Task spans hang directly under `spark.application`

Every task should sit under its own `spark.stage.N`. If they are flattened onto the root, the
driver's scheduler hook did not run. Either `-Dotel.javaagent.extensions` is missing from the
driver's `extraJavaOptions` or its path is wrong there, or the Flare JAR is not on the driver's
classpath, where the hook loads its helper classes from. The driver log shows
`[Flare] DAGScheduler reflection initialized successfully` when it works.

## No task spans

- `FLARE_TRACE_GRANULARITY` must be `tasks` or `all`; the default `stages` stops at stage spans.
- The executors need `spark.plugins` and the Flare JAR on their classpath too. Look for
  `[Flare] Executor plugin initialized` in an executor log.

## The root span is missing, and SQL or job spans are orphaned

The `spark.application` span ends when the application stops. On Flare 1.2.0 and earlier it was
lost when the cluster manager stopped the driver with SIGTERM, which a Databricks job cluster does
after every run. Upgrade to 1.3.0.

On a long-lived cluster, such as a Databricks all-purpose cluster or a Thrift server, the root only
ends when the cluster stops, so it is not there yet while the cluster is running.

## Executors show no `flare.role`

`flare.role` comes from the agent extension. It is missing on any JVM where
`-Dotel.javaagent.extensions` was not set, which is expected with the
[driver-only set-up](getting-started/quick-start.md#if-the-extension-can-only-be-attached-on-the-driver).

## Many small traces next to Flare's

Upgrade to 1.3.0, which drops them by default; see [Noise](configuration/noise.md).

## When an executor's node dies

When a node fails without warning (a hardware fault, an OOM kill of the machine, a spot reclaim
with no notice), Spark recovers by rerunning the lost work elsewhere, and the trace stays whole:
the dead executors' task spans keep their parents, and the removal is recorded on
`flare.executor.removed` with reason `lost`. What the dead JVMs had not yet sent is lost:

- **Task spans of the last few seconds.** Spans leave in batches, every 5 seconds by default
  (`otel.bsp.schedule.delay`).
- **Task metrics since the last export.** Executors flush a second after their last task, but a
  busy executor never goes quiet, so a node dying mid-stage loses everything since its last export,
  up to the 60-second interval (`otel.metric.export.interval`). In the lab, two killed executors
  lost about half of their task metrics.
- **The tasks it was running.** They end with `ExecutorLostFailure`, which only the driver sees.
  Since 1.4 the driver counts them on `flare.task.lost`, labelled with the executor and why it was
  lost; add it to the failed tasks for a task error rate.

A graceful shutdown loses less, and how much depends on the kind:

- **Spark's decommissioning** (`spark.decommission.enabled`) finishes the executor's running tasks
  before it exits, then the executor flushes: nothing is lost. If
  `spark.executor.decommission.forceKillTimeout` is set and runs out first, the executor is killed
  with tasks still running, and loses what a SIGTERM does.
- **A SIGTERM** (`kill`, a Kubernetes pod eviction without decommissioning) flushes what the
  executor had finished, but not the tasks it was still running. Spark does not wait for them, so
  their spans and duration metrics are never recorded; the driver counts them on `flare.task.lost`.

To narrow the loss on sudden failures, lower `otel.bsp.schedule.delay` and
`otel.metric.export.interval` on the executors, at the cost of more, smaller exports.

## When the driver dies

- **Killed outright** (`kill -9`, an OOM kill of the driver, a lost node): the root span and the
  open job and stage spans are lost, so the trace has no root. Driver metrics since the last export
  are lost too. Since 1.4 the driver flushes a second after its last job ends (three seconds if it
  flushed in the previous 30), so a driver killed between jobs loses little; one killed mid-job loses
  up to the export interval, and one killed in its first minute may have exported nothing. The
  missed-run rule on the [Alerting](configuration/alerting.md#a-run-that-did-not-happen) page is
  what notices a run that never reported.
- **Stopped with SIGTERM** (an orchestrator's timeout, `yarn application -kill`, a deleted driver
  pod, a Databricks job cluster's teardown): the root span and everything else arrive. Since 1.4 a
  run killed while a job was still running records `application.result` as `FAILED`.

## When the collector is unreachable

Spark is unaffected: exports are asynchronous, and nothing waits on them.

- **Metrics arrive complete,** with the agent's default cumulative temporality: the first export
  after the collector comes back carries the totals. With delta temporality
  (`otel.exporter.otlp.metrics.temporality.preference=DELTA`, or `LOWMEMORY`, which uses it for
  synchronous counters and histograms), each export carries only its own interval, so what failed to
  export is lost.
- **Spans and logs sent while it is down are dropped.** The agent's exporters retry briefly, then
  drop. In the lab, a collector that accepted connections but never answered kept 8% of spans and
  18% of logs over 45 seconds; one that refused connections kept 33% and 27% over 30 seconds. A
  hanging collector is worse, because each export waits for its timeout. Some loss continues for
  about 15 seconds after the collector recovers.
- **The root span arrives** if the collector is back before the application ends.

The remedy is a collector close to Spark: an OpenTelemetry Collector on each node, or as a
Kubernetes DaemonSet, that accepts locally and queues what it cannot forward to the central
backend. Two settings on its exporter decide how long an outage it covers, and past either, data is
still dropped:

- `retry_on_failure.max_elapsed_time`: how long a batch is retried, five minutes by default. `0`
  retries without limit.
- `sending_queue.queue_size`: how much the queue holds. Size it for the longest outage to cover.

With the `file_storage` extension the queue is kept on disk, so it also survives the collector
restarting. See the Collector's [resiliency guide](https://opentelemetry.io/docs/collector/resiliency/).

These measurements come from a TPC-H-derived workload on Spark 4.0.4 (data and queries generated
with DuckDB's `tpch` extension). They describe Flare's behaviour, not TPC-H results.

## Checking logs on managed platforms

Some platforms make logs unreliable for checking spans: Databricks cluster log delivery to a volume
delivered nothing in one workspace, and Dataproc Serverless does not forward executor output at all.
On those, export over OTLP and check in the backend.
