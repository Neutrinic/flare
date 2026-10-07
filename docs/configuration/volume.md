# Telemetry volume

An always-on Spark application can send gigabytes of telemetry a day, and most of it is not Flare's.
Measured on 20-minute applications running a TPC-H-derived workload
([overhead](../reference/overhead.md#what-was-measured)), on Databricks with 4 nodes unless noted:

| Signal | What drives it | Per hour, gzip | Lever |
|---|---|---|---|
| Logs | Spark's log level | 63 MB at `INFO` | Log level, collector filters |
| Traces | Tasks, and the agent's own instrumentation | about 12 MB with all of the agent's instrumentation on | [Agent instrumentation](#agent-instrumentation), granularity, sampling |
| Metrics | Series count and export frequency | about 8 MB (lab, 3 nodes) | Nothing needed |

## Logs

The agent exports logs by default, and Spark at `INFO` writes a line for every task's start and end.
In the TPC-H benchmarks that was 7,000 to 13,000 records a minute on small clusters, or for an
application running all day about 0.9 GB a day on the lab and 1.5 GB on Databricks, gzipped. Other
workloads log more or less. Capturing them costs no measurable CPU; the volume is the cost.

- Raise Spark's log level to `WARN` for production jobs.
- Or filter in a collector, keeping `WARN` and above and the lines you need.
- Or turn log export off: `-Dotel.logs.exporter=none`.

See also [Logs](exporting.md#logs) for what else reaches your log backend.

## Traces

### Agent instrumentation

The agent instruments far more than Spark: HTTP clients, the AWS and Google Cloud SDKs, JDBC, Kafka.
In a Spark JVM these mostly trace Spark reading its own input, one span per S3 or GCS request,
nested under Flare's task spans. With all of it on, they outnumbered Flare's own spans:

| Platform, 20-minute TPC-H application | Agent spans | Flare spans |
|---|---|---|
| Databricks, S3 | 12,000 | 11,000 |
| Dataproc, GCS | 82,000 | 5,700 |

On Dataproc they also cost about 2% CPU for as long as the job read data.

Since 1.3.0, Flare turns the agent's own instrumentation off by default and keeps only this:

| Kept | Why |
|---|---|
| `opentelemetry-api` | Connects Flare to the agent's SDK. Without it nothing is exported |
| `opentelemetry-instrumentation-annotations` | Your own `@WithSpan` methods |
| `flare-spark` | Flare's own instrumentation |
| `executors` | Carries a task's context into thread pools and futures, so spans made there keep their parent. It makes no spans of its own |
| `log4j-appender` | Log export, from Spark's logger. `otel.logs.exporter` still decides whether logs are sent |
| `runtime-telemetry` | JVM metrics: heap, garbage collection, threads |

These are defaults for each setting: anything you set yourself for the same setting, as a system
property, environment variable or in the agent's configuration file, takes precedence.

- **Turn one instrumentation back on** by name, such as `-Dotel.instrumentation.jdbc.enabled=true`
  for database calls made from tasks, or `-Dotel.instrumentation.kafka.enabled=true`.
- **Restore everything the agent instruments** with
  `-Dotel.instrumentation.common.default-enabled=true`.
- **Turn off one of those kept above** by its own name, such as
  `-Dotel.instrumentation.runtime-telemetry.enabled=false`.
  `-Dotel.instrumentation.common.default-enabled=false` alone does not, because each is enabled by
  its own setting.

With the agent's instrumentation off, Flare's spans were the same on the lab, Dataproc and
Databricks, and start-up CPU on Databricks was about half. Those runs also turned off `executors`,
annotations and JVM metrics, which 1.3.0 keeps.

### Flare's own spans

- **Granularity.** The default, `FLARE_TRACE_GRANULARITY=stages`, has no task spans, which are most
  of Flare's own volume.
- **Sampling** traces a fraction of applications, whole or not at all. See
  [Sampling](index.md#sampling).
- **`FLARE_MAX_SPANS_PER_TRACE`** caps task spans per executor. On an application that runs for
  hours, task spans stop once each executor reaches it; job, stage and SQL spans continue. With
  Photon, which runs more tasks a minute, that was within 20 minutes.

## Metrics

Flare's metric labels are bounded by the code, not by how long the application runs, so metric
volume stays flat: on a lab TPC-H application each export stayed at 10 to 13 KB from start to end.
Up to 1.2.0 the task and stage metrics also carried `stage.id`, which grew every export for the life
of the application; see [Upgrading](../upgrading.md).

Executors and the driver also flush when they go quiet, so a job's last metrics can reach the
backend on clusters that kill them without warning, provided the JVM is still alive when the flush
completes; see [Metrics](exporting.md#metrics). Most gaps between stages and jobs are too short to
trigger it, so each JVM exports metrics a few times a minute rather than once.
