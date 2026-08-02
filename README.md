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

Flare is an OTEL Java agent **extension**. The agent loads it from a filesystem path given by
`-Dotel.javaagent.extensions`, so the JAR must sit at a stable, identical path on every node.
That requirement drives the two options below.

### Option 1: Manual JAR deployment (recommended)

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

Download the JAR matching your Spark version from
[Releases](https://github.com/Neutrinic/flare/releases), or pull it from Maven Central.

### Option 2: `--packages` (reduced fidelity)

```bash
spark-submit \
  --packages io.github.neutrinic:flare-spark-3-5_2.13:1.1.0 \
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
io.github.neutrinic:flare-spark-3-3_2.12:1.1.0   # Spark 3.3, Scala 2.12
io.github.neutrinic:flare-spark-3-3_2.13:1.1.0   # Spark 3.3, Scala 2.13
io.github.neutrinic:flare-spark-3-4_2.12:1.1.0   # Spark 3.4, Scala 2.12
io.github.neutrinic:flare-spark-3-4_2.13:1.1.0   # Spark 3.4, Scala 2.13
io.github.neutrinic:flare-spark-3-5_2.12:1.1.0   # Spark 3.5, Scala 2.12
io.github.neutrinic:flare-spark-3-5_2.13:1.1.0   # Spark 3.5, Scala 2.13
io.github.neutrinic:flare-spark-4-0_2.13:1.1.0   # Spark 4.0, Scala 2.13
```

The OTEL Java agent JAR (`opentelemetry-javaagent.jar`) must still be placed on every node — `--packages` handles only Flare and its dependencies.

> **`--packages` alone does not load the agent extension.** `-Dotel.javaagent.extensions` is read
> at premain, from a fixed path. Resolved packages land in an Ivy cache on the driver and are
> fetched into a per-application directory on executors at executor startup — in both cases too
> late, and at a path you cannot name in advance. Flare still runs through `spark.plugins` and
> still produces traces, but the agent SPI components are inactive:
>
> | Lost | Effect |
> |------|--------|
> | `SubmitMissingTasksInstrumentation` | No per-stage traceparent. Task spans parent to `spark.application` instead of their stage, so the hierarchy flattens to `app → task` alongside `app → job → stage`. AQE sub-jobs are missed |
> | `TaskRunnerInstrumentationModule` | No OTEL context inside task bodies. JDBC/HTTP/gRPC calls made by your task code are not linked into the trace |
> | `FlareAutoConfig` | No `flare.role` / `flare.version` resource attributes |

#### Recovering stage attribution with a driver-side JAR

The first row is the one that costs you most, and it is recoverable on its own. Both
`SparkContext` and `DAGScheduler` live on the driver, so attaching the extension **only there**
restores per-stage traceparent injection. Executors read it out of the task properties through
the plugin and parent correctly, without needing the extension themselves:

```bash
  --conf "spark.driver.extraJavaOptions=\
    -javaagent:/opt/flare/opentelemetry-javaagent.jar \
    -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar \
    ..."
```

Verified: `spark.task → spark.stage` is restored, while executors report no `flare.role` and get
no in-task context restoration. This only needs a stable path on the driver node, which is often
available even when a cluster-wide one is not — notebooks and `spark-shell` in particular.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FLARE_TRACE_GRANULARITY` | `stages` | `jobs` / `stages` / `tasks` / `all` |
| `FLARE_SAMPLING_RATIO` | `0.1` | 0.0-1.0, validated at startup |
| `FLARE_SLOW_TASK_MS` | `0` (disabled) | Only emit task spans exceeding this ms |
| `FLARE_RETRY_TASKS_ONLY` | `false` | Only emit spans for retries and speculative tasks |
| `FLARE_MAX_SPANS_PER_TRACE` | `10000` | Circuit breaker for high-cardinality jobs |
| `FLARE_METRICS_ENABLED` | `true` | Enable OTEL metrics (task duration, shuffle bytes, stage aggregates) |
| `FLARE_SQL_PLAN_MAX_CHARS` | `4096` | Cap on `spark.sql.plan`; `0` drops the attribute |
| `FLARE_SQL_DETAILS_MAX_CHARS` | `2048` | Cap on `spark.sql.details`; `0` drops the attribute |
| `FLARE_SQL_DESCRIPTION_MAX_CHARS` | `1024` | Cap on `spark.sql.description`; `0` drops the attribute |
| `FLARE_SQL_PLAN_INITIAL_MAX_CHARS` | `0` (dropped) | Cap on `spark.sql.plan.initial`, the pre-AQE plan |
| `FLARE_ENABLED` | `true` | Kill switch |

Set via `-DFLARE_*` in `extraJavaOptions` or as environment variables. System properties take
precedence. `FLARE_ENABLED` only disables Flare for the literal value `false` (case-insensitive);
any other value leaves it on.

The SQL caps exist because a physical plan is unbounded at the source — a wide query runs to tens
of kilobytes, which is enough to push an OTLP batch past a collector's message limit, dropping the
whole batch rather than just the plan. Raise `FLARE_SQL_PLAN_MAX_CHARS` if your collector accepts
larger payloads. When a plan is clipped, `spark.sql.plan.truncated=true` is set alongside it, so a
partial plan never reads as a complete one.

`spark.sql.plan` holds the plan that ran. Spark reports the physical plan when the execution
starts, which is before Adaptive Query Execution re-plans, so that first tree always ends
`isFinalPlan=false` and can describe a partitioning that never happened. Each AQE re-plan
overwrites the attribute, leaving the final tree. Set `FLARE_SQL_PLAN_INITIAL_MAX_CHARS` above `0`
to also retain the pre-AQE tree as `spark.sql.plan.initial` — the AQE decision (skew splits,
broadcast conversion, partition coalescing) is only visible by diffing the two. It is off by
default because it doubles the worst-case plan payload on every SQL span.

### Resource Attributes

Flare adds two attributes to the OTEL `Resource`, so they appear on every span, metric and log
record the JVM emits. These come from the agent extension, so they are present with Option 1 and
absent with Option 2:

| Attribute | Example | Description |
|-----------|---------|-------------|
| `flare.version` | `1.1.0` | Flare build version, or `unknown` if the build metadata is unreadable |
| `flare.role` | `driver`, `executor-3` | Which side of the cluster the JVM is |

`flare.role` comes from `SPARK_EXECUTOR_ID` where it exists (Kubernetes) and otherwise from the
executor backend's `--executor-id` argument (standalone, YARN). It identifies the *individual*
executor, so under dynamic allocation the set of values grows as executors churn. Most backends
keep resource attributes off the metric series themselves — Prometheus-compatible stores expose
them through `target_info` — but Loki promotes them, so weigh this against log stream cardinality
if you run large elastic clusters.

> **Note:** When a Spark job fails, exception messages are recorded as span attributes. Spark exceptions sometimes include snippets of the data being processed (e.g., parse errors, type mismatches). Ensure your telemetry backend is secured appropriately if your jobs handle sensitive data.

## Telemetry Reference

Everything below is what Flare actually emits. Attributes marked *conditional* are absent rather
than zero when the underlying value was never observed — a missing attribute means "not measured",
which is different from a measured zero.

### `spark.application` — SpanKind SERVER, trace root

Opened when `SparkContext` initialises, closed at `SparkContext.stop()`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `spark.application.id` | string | `SparkContext.applicationId` |
| `spark.application.name` | string | `SparkContext.appName` |
| `spark.master.url` | string | `SparkContext.master` |
| `flare.version` | string | Flare build version |
| `flare.trace.granularity` | string | Effective `FLARE_TRACE_GRANULARITY` |

### `spark.sql.N` — SpanKind INTERNAL, child of `spark.application`

One per `SparkListenerSQLExecutionStart`. `N` is the execution id.

| Attribute | Type | Description |
|-----------|------|-------------|
| `spark.sql.execution.id` | long | Matches the `N` in the span name |
| `spark.sql.description` | string | Conditional — omitted when Spark reports it empty. Capped by `FLARE_SQL_DESCRIPTION_MAX_CHARS` |
| `spark.sql.details` | string | Conditional — the call-site stack. Capped by `FLARE_SQL_DETAILS_MAX_CHARS` |
| `spark.sql.plan` | string | The physical plan **that ran**, post-AQE. Capped by `FLARE_SQL_PLAN_MAX_CHARS` |
| `spark.sql.plan.truncated` | bool | Conditional — set only when the cap clipped the plan |
| `spark.sql.plan.initial` | string | Conditional — the pre-AQE plan. Off unless `FLARE_SQL_PLAN_INITIAL_MAX_CHARS > 0` |
| `spark.sql.plan.initial.truncated` | bool | Conditional — as above, for the initial plan |

### `spark.job.N` — SpanKind INTERNAL, child of `spark.sql.N` or `spark.application`

| Attribute | Type | Description |
|-----------|------|-------------|
| `spark.job.id` | long | |
| `spark.job.stage.count` | long | Number of stages the job was planned with |
| `spark.job.description` | string | Conditional — from `spark.job.description` local property |
| `spark.job.result` | string | `SUCCESS` or `FAILED` |
| `error.message` | string | Conditional — present only on `FAILED` |

### `spark.stage.N` — SpanKind INTERNAL, child of `spark.job.N`

Metrics below are Spark's own sums across every task in the stage, so they are directly comparable
with each other. All are set at `onStageCompleted` from `StageInfo.taskMetrics`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `spark.stage.id` | long | |
| `spark.stage.attempt.id` | long | |
| `spark.stage.name` | string | Spark's `RDD.creationSite` — the same string the Spark UI shows |
| `spark.stage.task.count` | long | |
| `spark.stage.executor.run_time_ms` | long | |
| `spark.stage.executor.cpu_time_ms` | long | Converted from Spark's nanoseconds |
| `spark.stage.jvm.gc_time_ms` | long | GC time rivalling run time is the answer to "why is this stage slow" |
| `spark.stage.executor.deserialize_time_ms` | long | |
| `spark.stage.executor.deserialize_cpu_time_ms` | long | Converted from nanoseconds |
| `spark.stage.result.serialization_time_ms` | long | |
| `spark.stage.scheduler.delay_ms` | long | Conditional — derived, see below |
| `spark.stage.input.bytes` / `.records` | long | |
| `spark.stage.output.bytes` / `.records` | long | |
| `spark.stage.shuffle.read_bytes` | long | |
| `spark.stage.shuffle.write_bytes` | long | |
| `spark.stage.memory.spilled_bytes` | long | |
| `spark.stage.disk.spilled_bytes` | long | |
| `spark.stage.failure_reason` | string | Conditional — first 500 chars |

`spark.stage.scheduler.delay_ms` is the only stage attribute Spark does not report. It is derived
per task as `duration − executorRunTime − deserializeTime − resultSerializationTime −
resultFetchTime` and summed over the stage, matching Spark's own `AppStatusUtils.schedulerDelay`.
The clamp at zero is applied per task, never to the sum, because driver and executor clocks differ
and a fast task can report more run time than its own wall clock. The attribute is **omitted** when
no task ends were observed — a `0` there would read as "no queueing" rather than "not measured".

### `spark.task.executor` — SpanKind INTERNAL, child of `spark.stage.N`, emitted on the executor JVM

Subject to `FLARE_TRACE_GRANULARITY`, `FLARE_SLOW_TASK_MS`, `FLARE_RETRY_TASKS_ONLY` and
`FLARE_MAX_SPANS_PER_TRACE`. This span is built from `TaskContext`, so driver-only `TaskInfo`
fields (host, locality, speculative) are not available to it.

| Attribute | Type | Description |
|-----------|------|-------------|
| `spark.task.partition.id` | long | |
| `spark.task.attempt.id` | long | `> 0` means a retry |
| `spark.stage.id` | long | Conditional — only under `FLARE_TRACE_GRANULARITY=all`. The parent span already identifies the stage; this repeats it so you can filter without a join |
| `spark.task.sql.execution_id` | long | Conditional — present when the task belongs to a SQL execution |
| `spark.task.result` | string | `SUCCESS`, `FAILED`, or `SHUTDOWN` if the JVM went down mid-task |
| `error.message` | string | Conditional — present only on `FAILED` |
| `spark.task.duration_ms` | long | Wall clock on the executor thread |
| `spark.task.input.bytes` / `output.bytes` | long | |
| `spark.task.shuffle.read_bytes` / `write_bytes` | long | |
| `spark.task.peak_memory_bytes` | long | |

The four byte counts and peak memory come from `TaskContext.taskMetrics()`, which is
`private[spark]` and can throw from outside `org.apache.spark` or in barrier mode. Flare logs at
debug and emits the span without them rather than failing the task, so all five are absent together
when that happens.

### Metrics

Nine instruments, all under the `io.flare.spark` meter, disabled wholesale by
`FLARE_METRICS_ENABLED=false`.

| Instrument | Kind | Unit | Labels |
|------------|------|------|--------|
| `flare.task.duration` | histogram | `ms` | `executor.id`, `stage.id`, `task.result` |
| `flare.task.records_throughput` | histogram | `{records}/s` | `executor.id`, `stage.id`, `task.result` |
| `flare.task.shuffle.read_bytes` | counter | `By` | `executor.id`, `stage.id`, `task.result` |
| `flare.task.shuffle.write_bytes` | counter | `By` | `executor.id`, `stage.id`, `task.result` |
| `flare.stage.executor.run_time` | histogram | `ms` | `stage.id`, `stage.name` |
| `flare.stage.input.bytes` | counter | `By` | `stage.id`, `stage.name` |
| `flare.stage.output.bytes` | counter | `By` | `stage.id`, `stage.name` |
| `flare.stage.shuffle.read_bytes` | counter | `By` | `stage.id`, `stage.name` |
| `flare.stage.shuffle.write_bytes` | counter | `By` | `stage.id`, `stage.name` |

The counters are only incremented for non-zero values, so a stage that read nothing produces no
`flare.stage.input.bytes` series rather than a flat zero one.

`flare.task.*` are recorded on the executor while the task span's scope is still open, so the SDK's
default `trace_based` exemplar filter attaches an exemplar linking each measurement back to its
trace. Under `FLARE_SLOW_TASK_MS` the metric is recorded *after* the suppressed span's scope closes,
so a fast task still contributes to the histogram but carries no exemplar pointing at a trace that
was never exported. Note that some backends drop exemplars by default — Mimir's
`max_global_exemplars_per_user` is `0` unless you set it.

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

**Older agent:** Likely to fail. Flare uses `InstrumentationModule` and `TypeInstrumentation` from
the extension API, which have evolved across agent releases.

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
sbt -DsparkVersion=3.5.1 ++2.13.16 "AgentTest / test"  # assembled real-agent SPI smoke test
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
- OpenTelemetry Java Agent 2.30.0 (extension mechanism)
- OpenTelemetry API 1.64.0
- munit (tests)

## License

Apache License 2.0
