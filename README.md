# Flare

Full-stack OpenTelemetry observability for Apache Spark — traces, metrics, and logs correlated across driver and executor JVMs.

![Dashboard](screenshots/dashboard.png)

```
spark.application                          (flare-driver)
├── spark.sql.0                            (flare-driver)
│   ├── spark.job.0                        (flare-driver)
│   │   └── spark.stage.0                  (flare-driver)
│   │       ├── spark.task.executor        (flare-executor)
│   │       └── spark.task.executor        (flare-executor)
│   └── spark.job.1                        (flare-driver)
│       └── spark.stage.2                  (flare-driver)
│           ├── spark.task.executor        (flare-executor)
│           └── spark.task.executor        (flare-executor)
└── spark.sql.1                            (flare-driver)
    └── spark.job.2                        (flare-driver)
        └── spark.stage.4                  (flare-driver)
            ├── spark.task.executor        (flare-executor)
            └── spark.task.executor        (flare-executor)
```

## Overview

Most Spark observability stops at the driver. You get a stage span with an aggregate duration but cannot see which executor ran slow, which partition was skewed, or whether a retry happened on a specific node.

Flare hooks `DAGScheduler.submitMissingTasks` via ByteBuddy to inject a per-stage W3C `traceparent` into task properties before tasks are created. On the executor, the traceparent is extracted and restored as OTEL context, creating task spans with accurate wall-clock timing nested under their specific stage span. The full hierarchy — `app → sql → job → stage → task` — spans two JVM services with zero orphan spans.

## Features

**Traces**
- **Full span hierarchy** — `app → sql → job → stage → task` across driver and executor JVMs
- **Executor task spans** — real spans on the executor thread, not driver-side approximations
- **Per-stage context** — each task inherits its specific stage span as parent, including AQE sub-jobs
- **W3C trace continuity** — `traceparent` propagated via Spark's local property channel
- **Kafka source correlation** — Spark execution stays primary while upstream producer contexts are linked
- **Granularity control** — jobs, stages, tasks, or all; plus slow-task and retry-only filters
- **Sampling** — consistent across the JVM boundary via W3C traceparent flags

![Traces](screenshots/traces.png)

**Metrics**
- **Task duration histograms** — with exemplar links back to the originating trace
- **Shuffle I/O counters** — read/write bytes per task and per stage
- **Stage aggregates** — executor run time, input/output bytes, shuffle totals
- **Records throughput** — histogram of records processed per second

![Metrics](screenshots/metrics.png)

**Logs**
- **Trace-correlated logs** — driver and executor logs linked to spans via OTLP
- **MDC enrichment** — trace ID and span ID injected into log context during task execution

![Logs](screenshots/logs.png)

**General**
- **Zero code changes** — two JARs, two `--conf` lines on `spark-submit`
- **OTEL native** — OTLP export to any backend (Grafana, Jaeger, Honeycomb, Datadog)
- **Provisioned Grafana dashboard** — task duration heatmaps, shuffle skew detection, executor comparison, logs, and trace links out of the box

## Installation

### Option 1: `--packages` (recommended)

```bash
spark-submit \
  --packages io.github.neutrinic:flare-spark-3-5_2.13:1.0.0 \
  --conf "spark.plugins=io.flare.spark.plugin.FlareSparkPlugin" \
  --conf "spark.driver.extraJavaOptions=\
    -javaagent:/opt/flare/opentelemetry-javaagent.jar \
    -Dotel.service.name=my-app-driver \
    -Dotel.exporter.otlp.protocol=grpc \
    -Dotel.exporter.otlp.endpoint=http://your-collector:4317" \
  --conf "spark.executor.extraJavaOptions=\
    -javaagent:/opt/flare/opentelemetry-javaagent.jar \
    -Dotel.service.name=my-app-executor \
    -Dotel.exporter.otlp.protocol=grpc \
    -Dotel.exporter.otlp.endpoint=http://your-collector:4317" \
  myapp.jar
```

Pick the artifact matching your Spark version:

```
io.github.neutrinic:flare-spark-3-3_2.12:1.0.0   # Spark 3.3, Scala 2.12
io.github.neutrinic:flare-spark-3-3_2.13:1.0.0   # Spark 3.3, Scala 2.13
io.github.neutrinic:flare-spark-3-4_2.12:1.0.0   # Spark 3.4, Scala 2.12
io.github.neutrinic:flare-spark-3-4_2.13:1.0.0   # Spark 3.4, Scala 2.13
io.github.neutrinic:flare-spark-3-5_2.12:1.0.0   # Spark 3.5, Scala 2.12
io.github.neutrinic:flare-spark-3-5_2.13:1.0.0   # Spark 3.5, Scala 2.13
io.github.neutrinic:flare-spark-4-0_2.13:1.0.0   # Spark 4.0, Scala 2.13
```

The OTEL Java agent JAR (`opentelemetry-javaagent.jar`) must still be placed on every node — `--packages` handles only Flare and its dependencies.

This mode loads Flare as a Spark plugin after the Java agent has initialized. For automatic
Kafka source correlation, use the agent-extension deployment below. If you need to stay with
`--packages`, add both properties shown in
[Kafka Structured Streaming](#kafka-structured-streaming) explicitly.

### Option 2: Agent extension deployment (recommended for Kafka)

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

Loading Flare through `otel.javaagent.extensions` makes its auto-configuration available during
JVM startup. With the supported OpenTelemetry Java agent 2.30.0, no additional Kafka tracing flag
is required.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FLARE_TRACE_GRANULARITY` | `stages` | `jobs` / `stages` / `tasks` / `all` |
| `FLARE_SAMPLING_RATIO` | `0.1` | 0.0-1.0, validated at startup |
| `FLARE_SLOW_TASK_MS` | `0` (disabled) | Only emit task spans exceeding this ms |
| `FLARE_RETRY_TASKS_ONLY` | `false` | Only emit spans for retries and speculative tasks |
| `FLARE_MAX_SPANS_PER_TRACE` | `10000` | Circuit breaker for high-cardinality jobs |
| `FLARE_METRICS_ENABLED` | `true` | Enable OTEL metrics (task duration, shuffle bytes, stage aggregates) |
| `FLARE_ENABLED` | `true` | Kill switch |

Set via `-DFLARE_*` in `extraJavaOptions` or as environment variables.

> **Note:** When a Spark job fails, exception messages are recorded as span attributes. Spark exceptions sometimes include snippets of the data being processed (e.g., parse errors, type mismatches). Ensure your telemetry backend is secured appropriately if your jobs handle sensitive data.

## Kafka Structured Streaming

Flare delegates Kafka spans and header extraction to the OpenTelemetry Java agent. It does not
wrap Kafka clients or create duplicate messaging spans. When Flare is loaded as an agent extension,
it enables this low-precedence OpenTelemetry default:

```properties
otel.instrumentation.messaging.experimental.receive-telemetry.enabled=true
```

With task tracing enabled, the resulting relationship is:

```text
spark.stage
└── spark.task.executor
    └── <topic> receive
        └── <topic> process
            ├── downstream HTTP/JDBC/etc.
            └── link → upstream producer context
```

With the default `stages` granularity, the task level is omitted:

```text
spark.stage
└── <topic> receive
    └── <topic> process
        └── link → upstream producer context
```

Flare restores the active stage context on the executor. With
`FLARE_TRACE_GRANULARITY=tasks` or `all`, the executor task span becomes the parent instead.

This model deliberately keeps record processing in the Spark execution trace and represents the
incoming producer context as a span link. A Spark task can process many records from different
producer traces, so there is no single correct upstream parent for the task.

Requirements:

- Flare pins and tests OpenTelemetry Java agent 2.30.0. Upstream version 2.21.0 introduced the
  [`records(TopicPartition).listIterator()` instrumentation](https://github.com/open-telemetry/opentelemetry-java-instrumentation/pull/14757)
  used by Spark Structured Streaming, but it is not a tested Flare runtime baseline.
- Flare must be supplied through `otel.javaagent.extensions` on both driver and executors for the
  automatic default to run during agent startup.
- Producers must inject a supported propagation header, such as W3C `traceparent`, into Kafka
  record headers.

An explicit OpenTelemetry setting always overrides Flare. Set the following on both executor and
driver JVMs to restore the agent's normal direct-parent behavior:

```properties
-Dotel.instrumentation.messaging.experimental.receive-telemetry.enabled=false
```

In direct-parent mode, Kafka process spans continue the producer trace but are no longer children
of the Flare stage/task trace.

When using `--packages` without `otel.javaagent.extensions`, the agent has already initialized
before Spark loads Flare, and TaskRunner context restoration is unavailable. Add the OpenTelemetry
property explicitly and enable task spans so a Flare context is active while Kafka records are
processed:

```text
-Dotel.instrumentation.messaging.experimental.receive-telemetry.enabled=true
-DFLARE_TRACE_GRANULARITY=tasks
```

The bridge covers Kafka source consumption inside the Spark task. It cannot preserve per-record
context after a shuffle, window, join, aggregation, or state-store boundary unless the application
carries that context as data. Per-record spans can also make long-running, high-throughput
streaming traces very large. Because parent-based sampling retains children of a sampled
long-running application trace, use collector-side span filtering or explicitly disable receive
telemetry when that trade-off is unsuitable. The OpenTelemetry setting is shared by messaging
instrumentations, so it also changes receive/process parenting for any other instrumented
messaging clients in the same JVM.

## OTEL Agent Compatibility

Flare is an OTEL Java agent **extension** — not a standalone library. It is loaded by the agent's `AgentClassLoader` and shares the agent's ByteBuddy and SDK classes at runtime. This means the agent version matters.

### Tested Versions

| Component | Version | Notes |
|-----------|---------|-------|
| OTEL Java Agent | 2.30.0 | Built and tested against this version |
| OTEL API / SDK | 1.64.0 | Matches agent 2.30.0's bundled SDK |
| ByteBuddy | 1.18.11 | Matches agent 2.30.0's `byte-buddy-dep` |

### Version Compatibility

**Same minor version (recommended):** Use the exact agent version Flare was built against. The extension API is published with an `-alpha` suffix, meaning it can break between minor releases.

**Newer agent:** May work if the extension API hasn't changed. The OTEL team generally maintains backward compatibility within the `2.x` line, but the extension API is explicitly unstable. Test before deploying.

**Older agent:** Agent 2.21.0 is where the required Spark `ListIterator` instrumentation first
appeared, but Flare's alpha extension API is compiled and tested against 2.30.0. Use the exact
2.30.0 baseline unless you maintain a separate compatibility test.

### Why Shading Doesn't Help

The agent's extension mechanism loads Flare into the same classloader as the agent's own SDK and ByteBuddy classes. If Flare bundled its own copies, class conflicts would cause `LinkageError` or `ClassCastException` at runtime. All OTEL and ByteBuddy dependencies must be `provided` scope — the agent supplies them.

### Checking Your Agent Version

```bash
java -javaagent:/path/to/opentelemetry-javaagent.jar -version
# Or check the JAR manifest:
unzip -p opentelemetry-javaagent.jar META-INF/MANIFEST.MF | grep Implementation-Version
```

If you see version mismatches at runtime, the most common symptom is a `NoSuchMethodError` or `ClassNotFoundException` in Flare's instrumentation modules during Spark startup.

## Building

Requires Java 17+ and sbt.

```bash
sbt compile
sbt assembly  # fat JAR at target/scala-2.13/flare-spark-3.5.jar
sbt -DsparkVersion=3.5.1 ++2.13.16 "AgentTest / test"  # real-agent ListIterator bridge smoke test
```

Cross-compile for a specific Spark version:

```bash
sbt -DsparkVersion=3.3.4 ++2.12.18 assembly   # Spark 3.3, Scala 2.12
sbt -DsparkVersion=4.0.0 ++2.13.16 assembly   # Spark 4.0, Scala 2.13
```

Supported matrix:

| Spark | Scala 2.12 | Scala 2.13 |
|-------|:----------:|:----------:|
| 3.3   | ✓          | ✓          |
| 3.4   | ✓          | ✓          |
| 3.5   | ✓          | ✓          |
| 4.0   |            | ✓          |

## Docker

A full observability stack is provided for local development — Spark cluster, Alloy (OTLP collector), Tempo (traces), Mimir (metrics), Loki (logs), and Grafana with a pre-built dashboard:

```bash
sbt assembly
sbt "examples/assembly"

cd docker
docker compose up -d
```

Assembly JARs are bind-mounted from the build tree — just re-run `sbt assembly` and restart the job, no Docker rebuild needed.

Run the skewed partition example:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class io.flare.examples.SkewedJob \
  /opt/flare/flare-examples.jar
```

Open Grafana at `http://localhost:3000`:
- **Dashboards > Flare — Spark Observability** — task duration heatmap, shuffle skew, executor comparison, stage summary, logs, and trace links
- **Explore > Tempo** — search traces by service name, drill into span details with linked metrics and logs
- **Explore > Mimir** — query flare_task_* and flare_stage_* metrics directly

## Architecture

```
ByteBuddy (OTEL agent extension)
├── SparkContextInstrumentation        # auto-registers listener + executor plugin
├── SubmitMissingTasksInstrumentation  # hooks DAGScheduler.submitMissingTasks
│   └── SubmitMissingTasksAdviceHelper # creates job/stage spans, injects traceparent
├── TaskRunnerInstrumentation          # hooks Executor$TaskRunner.run()
│   └── TaskRunnerAdviceHelper         # extracts traceparent, restores OTEL context
└── TracingSparkListener               # adopts pre-created spans, manages lifecycle

FlareSparkPlugin (backward compat)
├── FlareDriverPlugin                  # fallback when ByteBuddy is not active
└── FlareExecutorPlugin                # creates task spans on executor
```

Context propagation path:

```
Driver: DAGScheduler.submitMissingTasks(stage, jobId)
  → ByteBuddy advice creates stage span
  → injects traceparent into ActiveJob.properties
  → properties serialized into TaskDescription
  → sent to executor JVM
Executor: TaskRunner.run() / ExecutorPlugin.onTaskStart()
  → extracts traceparent from task properties
  → restores OTEL context → task span with executor-side timing
```

## Stack

- Scala 2.12 / 2.13, Spark 3.3–4.0
- OpenTelemetry Java Agent 2.30.0 (the required Kafka path first appeared in 2.21.0)
- OpenTelemetry API 1.64.0
- munit (tests)

## License

Apache License 2.0
