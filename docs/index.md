# Flare

Flare is an OpenTelemetry Java agent extension for Apache Spark. It gives you one trace per Spark
application, from the driver down to the individual task on the executor, plus task and stage
metrics and trace-linked logs. No code changes: you add two JARs and a few Spark settings.

![Dashboard](assets/dashboard.png)

## What you get

```text
spark.application                          (driver)
├── spark.sql.0                            (driver)
│   ├── spark.job.0                        (driver)
│   │   └── spark.stage.0                  (driver)
│   │       ├── spark.task.executor        (executor)
│   │       └── spark.task.executor        (executor)
│   └── spark.job.1                        (driver)
│       └── spark.stage.2                  (driver)
│           └── spark.task.executor        (executor)
└── spark.sql.1                            (driver)
    └── spark.job.2                        (driver)
        └── spark.stage.4                  (driver)
            └── spark.task.executor        (executor)
```

Most Spark observability stops at the driver. You get a stage with an aggregate duration, but not
which executor ran slow, which partition was skewed, or where a retry happened. Flare creates the
task spans on the executor itself, with the executor's own timing, and parents each one to its
stage across the JVM boundary.

- **Traces:** the full `application → sql → job → stage → task` hierarchy, with SQL plans, stage
  metrics and failure detail as attributes. See [Spans](reference/spans.md).
- **Metrics:** task duration and throughput histograms with exemplars back to the trace, shuffle
  and stage counters, executor lifecycle. See [Metrics](reference/metrics.md).
- **Logs:** driver and executor logs exported with the trace and span id of the task that wrote
  them. See [Exporting](configuration/exporting.md#logs).

Everything goes out over OTLP, so any OpenTelemetry backend works: Grafana (Tempo, Mimir, Loki),
Jaeger, Honeycomb, Datadog and others.

## How it works

Flare runs inside the [OpenTelemetry Java agent](https://opentelemetry.io/docs/zero-code/java/agent/).
On the driver it hooks the scheduler, so every stage gets its own W3C `traceparent`, which Spark
carries to the executors inside each task's properties. On the executor, Flare restores that
context before the task runs and opens the task span under it. Anything else the agent instruments
inside the task, a JDBC query or an S3 read, nests under the task span.

## Where it runs

Verified on real clusters: standalone, YARN, Kubernetes (including the Spark Operator), Amazon EMR
and EMR Serverless, Google Dataproc and Dataproc Serverless, and Databricks on AWS and Azure. See
[Deploying](deploying/index.md) for each, and [Platforms that cannot run Flare](deploying/unsupported.md)
for the ones where the agent cannot be attached.

## Start here

1. [Quick start](getting-started/quick-start.md): one `spark-submit` with Flare attached.
2. [Choosing a JAR](getting-started/choosing-a-jar.md): which of the seven artifacts matches your Spark.
3. [Deploying](deploying/index.md): the recipe for your platform.
