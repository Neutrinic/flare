# Flare

OpenTelemetry distributed tracing for Apache Spark — driver-to-executor context propagation.

```
spark.application  (driver, root)
├── spark.job.0    (driver)
│   └── spark.stage.0  (driver)
├── spark.job.1    (driver)
│   └── spark.stage.2  (driver)
├── spark.task.executor  (executor-1, partition 0)  ← Flare
├── spark.task.executor  (executor-1, partition 1)  ← Flare
├── spark.task.executor  (executor-2, partition 2)  ← Flare
└── spark.task.executor  (executor-2, partition 3)  ← Flare
```

> **Phase 1 note:** Task spans are children of the application span, not of their specific
> job or stage. The traceparent is injected once at SparkContext init, pointing to the
> application span. Phase 2 will inject per-job traceparent via ByteBuddy for full
> hierarchical nesting.

## Overview

Most Spark observability stops at the driver. You get a stage span with an aggregate duration but cannot see which executor ran slow, which partition was skewed, or whether a retry happened on a specific node.

Flare injects a W3C `traceparent` into Spark's `LocalProperty` mechanism at SparkContext init. Local properties serialize into `TaskDescription` and travel to the executor JVM. `ExecutorPlugin.onTaskStart()` extracts the context and creates a real executor-side span with accurate wall-clock timing and the driver trace as parent.

## Features

- **Executor task spans** — real spans on the executor thread, not driver-side approximations
- **W3C trace continuity** — `traceparent` propagated via Spark's local property channel
- **Zero code changes** — two JARs, two `--conf` lines on `spark-submit`
- **OTEL native** — OTLP export to any backend (Grafana Tempo, Jaeger, Honeycomb, Datadog)
- **Granularity control** — jobs, stages, tasks, or all; plus slow-task and retry-only filters
- **Sampling** — consistent across the JVM boundary via W3C traceparent flags

## Installation

```bash
spark-submit \
  --conf "spark.plugins=io.flare.spark.plugin.FlareSparkPlugin" \
  --conf "spark.driver.extraClassPath=/opt/flare/flare-spark.jar" \
  --conf "spark.executor.extraClassPath=/opt/flare/flare-spark.jar" \
  --conf "spark.driver.extraJavaOptions=\
    -javaagent:/opt/flare/opentelemetry-javaagent.jar \
    -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar \
    -Dotel.service.name=my-app-driver \
    -Dotel.exporter.otlp.protocol=grpc \
    -Dotel.exporter.otlp.endpoint=http://your-collector:4317" \
  --conf "spark.executor.extraJavaOptions=\
    -javaagent:/opt/flare/opentelemetry-javaagent.jar \
    -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar \
    -Dotel.service.name=my-app-executor \
    -Dotel.exporter.otlp.protocol=grpc \
    -Dotel.exporter.otlp.endpoint=http://your-collector:4317" \
  myapp.jar
```

Both JARs must be accessible on every node. On Kubernetes, bake them into your Spark image. On YARN/EMR, use `--files` and reference via `{{PWD}}`.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FLARE_TRACE_GRANULARITY` | `stages` | `jobs` / `stages` / `tasks` / `all` |
| `FLARE_SAMPLING_RATIO` | `0.1` | 0.0-1.0, validated at startup |
| `FLARE_SLOW_TASK_MS` | `0` (disabled) | Only emit task spans exceeding this ms |
| `FLARE_RETRY_TASKS_ONLY` | `false` | Only emit spans for retries and speculative tasks |
| `FLARE_MAX_SPANS_PER_TRACE` | `10000` | Circuit breaker for high-cardinality jobs |
| `FLARE_ENABLED` | `true` | Kill switch |

Set via `-DFLARE_*` in `extraJavaOptions` or as environment variables.

## Building

Requires Java 17+ and sbt.

```bash
sbt compile
sbt assembly  # fat JAR at target/scala-2.13/flare-spark-assembly-*.jar
```

## Docker

A Spark cluster with Grafana Tempo and Grafana is provided for local development:

```bash
sbt assembly
sbt "examples/assembly"

cd docker
docker compose up -d
```

This starts a Spark master, two workers, a history server, Alloy (OTLP collector), Tempo, and Grafana. Assembly JARs are bind-mounted from the build tree — just re-run `sbt assembly` and restart the job, no Docker rebuild needed.

Run the skewed partition example:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class io.flare.examples.SkewedJob \
  /opt/flare/flare-examples.jar
```

Open Grafana at `http://localhost:3000` — navigate to Explore, select Tempo, and search for recent traces.

## Architecture

```
FlareSparkPlugin
├── FlareDriverPlugin          # creates application span, injects traceparent,
│   └── TracingSparkListener   # registers listener for job/stage/SQL spans
└── FlareExecutorPlugin        # extracts traceparent on executor, creates task spans
```

Context propagation path:

```
Driver: SparkContext.setLocalProperty("traceparent", "00-{traceId}-{spanId}-01")
  → serialized into TaskDescription.properties
  → sent to executor JVM
Executor: TaskContext.getLocalProperty("traceparent")
  → W3C extract → parent context → span with accurate executor-side timing
```

## Stack

- Scala 2.12 / 2.13, Spark 3.5 / 4.0
- OpenTelemetry Java Agent 2.16.0 (extension mechanism)
- OpenTelemetry API 1.50.0
- munit (tests)

## License

Apache License 2.0
