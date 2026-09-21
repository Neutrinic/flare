# Flare

[![Maven Central](https://img.shields.io/maven-central/v/io.github.neutrinic/flare-spark-3-5_2.13?label=maven%20central)](https://central.sonatype.com/artifact/io.github.neutrinic/flare-spark-3-5_2.13)
[![CI](https://github.com/Neutrinic/flare/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Neutrinic/flare/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

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

Flare hooks `DAGScheduler.submitMissingTasks` via ByteBuddy to inject a per-stage W3C `traceparent` into task properties before tasks are created. On the executor, the traceparent is extracted and restored as OTEL context, creating task spans with accurate wall-clock timing nested under their specific stage span. The full hierarchy — `app → sql → job → stage → task` — spans two JVM services with no orphan spans on the default configuration. One exception is currently known: enabling `FLARE_SLOW_TASK_MS` (off by default) can leave spans created *inside* a suppressed task pointing at a parent that is never exported — see [#100](https://github.com/Neutrinic/flare/issues/100).

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
- **Zero code changes** — five JARs on every node, five `--conf` lines on `spark-submit`
- **OTEL native** — OTLP export to any backend (Grafana, Jaeger, Honeycomb, Datadog)
- **Provisioned Grafana dashboard** — task duration heatmaps, shuffle skew detection, executor comparison, logs, and trace links out of the box

## Installation

Flare is an OTEL Java agent **extension**. The agent loads it from a filesystem path given by
`-Dotel.javaagent.extensions`, so the JAR must sit at a stable, identical path on every node.
Five JARs go onto every node: the OTEL agent, the Flare extension, and the three
OpenTelemetry API JARs that Flare needs on the application classpath. The agent is
attached with `-javaagent`; the other four go on `extraClassPath`.

### Deploying the JARs

```bash
spark-submit \
  --conf "spark.plugins=io.flare.spark.plugin.FlareSparkPlugin" \
  --conf "spark.driver.extraClassPath=/opt/flare/flare-spark.jar:/opt/flare/opentelemetry-api.jar:/opt/flare/opentelemetry-context.jar:/opt/flare/opentelemetry-common.jar" \
  --conf "spark.executor.extraClassPath=/opt/flare/flare-spark.jar:/opt/flare/opentelemetry-api.jar:/opt/flare/opentelemetry-context.jar:/opt/flare/opentelemetry-common.jar" \
  --conf "spark.driver.extraJavaOptions=\
    -javaagent:/opt/flare/opentelemetry-javaagent.jar \
    -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar \
    -Dotel.service.name=my-app-driver \
    -Dotel.exporter.otlp.protocol=grpc \
    -Dotel.exporter.otlp.endpoint=http://your-collector:4317 \
    -Dotel.exporter.otlp.compression=gzip" \
  --conf "spark.executor.extraJavaOptions=\
    -javaagent:/opt/flare/opentelemetry-javaagent.jar \
    -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar \
    -Dotel.service.name=my-app-executor \
    -Dotel.exporter.otlp.protocol=grpc \
    -Dotel.exporter.otlp.endpoint=http://your-collector:4317 \
    -Dotel.exporter.otlp.compression=gzip" \
  myapp.jar
```

> **`otel.exporter.otlp.compression` defaults to `none`.** It is set to `gzip` above because
> Spark telemetry is unusually repetitive: every OTLP export carries a full copy of the resource
> block, which on Spark is dominated by `process.command_args` — the whole command line including
> the classpath, around 2.9 kB. A single trace spans several export batches per JVM, so that
> string goes over the wire many times.
>
> Measured on the dev stack, three matched runs each way, counting bytes actually received by the
> collector: **244,660 bytes uncompressed against 106,480 with gzip — 56% less traffic, 2.3x
> smaller.** Span data is unaffected; the same run still produced identical traces.

All four JARs must sit at the same absolute path on every node. On Kubernetes, bake them into your Spark image. On YARN/EMR, use `--files` and reference via `{{PWD}}`. On Databricks, fetch them in a cluster init script.

Download the Flare JAR matching your Spark version from
[Releases](https://github.com/Neutrinic/flare/releases), or pull it from Maven Central:

```
io.github.neutrinic:flare-spark-3-3_2.12:1.2.0   # Spark 3.3, Scala 2.12
io.github.neutrinic:flare-spark-3-3_2.13:1.2.0   # Spark 3.3, Scala 2.13
io.github.neutrinic:flare-spark-3-4_2.12:1.2.0   # Spark 3.4, Scala 2.12
io.github.neutrinic:flare-spark-3-4_2.13:1.2.0   # Spark 3.4, Scala 2.13
io.github.neutrinic:flare-spark-3-5_2.12:1.2.0   # Spark 3.5, Scala 2.12
io.github.neutrinic:flare-spark-3-5_2.13:1.2.0   # Spark 3.5, Scala 2.13
io.github.neutrinic:flare-spark-4-0_2.13:1.2.0   # Spark 4.0, Scala 2.13
```

The three OpenTelemetry JARs are `opentelemetry-api`, `opentelemetry-context` and
`opentelemetry-common`, all at 1.64.0, from Maven Central.

> **The OTEL JARs are not optional.** Flare's Spark-side half is loaded by `spark.plugins` into
> Spark's own classloader. The published Flare JAR bundles no dependencies, the OTEL agent does
> not put an API on the application classpath (it shades its own copy and bridges to one you
> supply), and Spark ships none. Without them the driver dies at `SparkContext` init with
> `NoClassDefFoundError: io/opentelemetry/context/ImplicitContextKeyed`.
>
> `extraClassPath` is also what makes the ByteBuddy advice work, since its Scala helper classes
> are resolved from there. Dropping it costs per-stage traceparent injection, which silently
> flattens task spans onto `spark.application`.

### If you can only place JARs on the driver

Some environments let you stage files on the driver but not cluster-wide, notebooks and
`spark-shell` in particular. Both `SparkContext` and `DAGScheduler` live on the driver, so
attaching the extension **only there** still restores per-stage traceparent injection. Executors
read it out of the task properties through the plugin and parent correctly without needing the
extension themselves.

Only the **extension** is driver-side. `spark.plugins` and `extraClassPath` — the Flare JAR plus
the three OpenTelemetry JARs — are still required on the driver *and* every executor. The plugin
is what creates task spans at all; `-Dotel.javaagent.extensions` loads the agent extension and
puts nothing on the Spark classpath. Drop the executor classpath and you get no task spans rather
than badly parented ones.

So relative to the full install above, change only the executor `extraJavaOptions`, dropping
`-Dotel.javaagent.extensions` from it:

```bash
  --conf "spark.driver.extraJavaOptions=\
    -javaagent:/opt/flare/opentelemetry-javaagent.jar \
    -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar \
    ..."
```

Verified: `spark.task` parents to `spark.stage` again. Executors report no `flare.role` and get no
in-task context restoration, so JDBC and HTTP calls inside task bodies are not linked into the
trace.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FLARE_TRACE_GRANULARITY` | `stages` | `jobs` / `stages` / `tasks` / `all` |
| `FLARE_SAMPLING_RATIO` | `0.1` | 0.0-1.0, validated at startup |
| `FLARE_SLOW_TASK_MS` | `0` (disabled) | Only emit task spans exceeding this ms |
| `FLARE_RETRY_TASKS_ONLY` | `false` | Only emit spans for retries and speculative tasks |
| `FLARE_MAX_SPANS_PER_TRACE` | `10000` | Circuit breaker for high-cardinality jobs |
| `FLARE_METRICS_ENABLED` | `true` | Enable OTEL metrics (task duration, shuffle bytes, stage aggregates) |
| `FLARE_TRACK_BLOCK_UPDATES` | `false` | Per-block storage totals. Off by default — `SparkListenerBlockUpdated` fires once per block, which on a large cached dataset is a firehose on the listener bus thread |
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

**`spark.sql.plan.fingerprint`** hashes the plan's *shape* into 16 hex characters, so the same
query groups across executions and across applications — something the Spark UI cannot do, having
no cross-application view. Catalyst expression ids (`#133`, `#522L`), `plan_id=142` and
`[codegen id : 1]` are stripped before hashing, because they vary without the shape changing.

That normalisation is not needed for `spark-submit` batch: Catalyst's id counter is JVM-global and
monotonic, so a fresh JVM replays identical ids and the raw plans of two runs are byte-identical.
It matters in a long-lived JVM — Thrift server, notebook, streaming — where one query shape gets
different ids on its 2nd execution than its 50th, and raw hashing would fragment it into unusable
cardinality.

AQE **runtime statistics** are stripped for a different and more important reason. Once AQE
materialises query stages, the final plan carries what it observed:

```
ResultQueryStage (11), Statistics(sizeInBytes=8.0 EiB)
+- ShuffleQueryStage (9), Statistics(sizeInBytes=32.0 B, rowCount=2)
```

Those numbers track the *data*, not the query, so leaving them in would fingerprint the same
query differently on a busy day than a quiet one — defeating the grouping entirely.

The fingerprint is emitted **independently of the character caps, including when they are `0`**
and no plan text is exported at all. That is deliberate: Tempo's binding limit is `max_bytes_per_trace`
(5 MB on the pinned 2.6.1) — per *trace*, not per span — so a 100 kB plan overflows it after
roughly 50 SQL executions in one application, and raising the cap makes that worse rather than
better. 16 bytes of grouping is the right lever there.

Because the caps do not reach it, the fingerprint is computed from the full plan, so two
deployments with different caps still group the same query identically.

`spark.sql.plan.initial.fingerprint` is set from the pre-AQE tree and left alone by re-plans, so
the pair is **"shape as planned" versus "shape as executed"**. It is emitted even when
`spark.sql.plan.initial` itself is off.

Do **not** read the two differing as "AQE made an optimisation". Under AQE the final tree always
gains a `== Final Plan ==` section and `QueryStage` wrappers, so the two differ for essentially
every AQE-enabled query — all three executions in a `PipelineJob` run differ. Of the two,
`spark.sql.plan.initial.fingerprint` is the more stable grouping key, since it predates both the
query stages and their statistics; `spark.sql.plan.fingerprint` additionally reflects the
decisions AQE actually made, which can legitimately vary between runs of the same query.

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
| `spark.sql.plan.fingerprint` | string | 16 hex chars. Hash of the plan's *shape*. Always emitted when a plan exists, **including when the caps are `0`** |
| `spark.sql.plan.initial.fingerprint` | string | As above for the pre-AQE plan. The more stable of the two — see below |

### `spark.job.N` — SpanKind INTERNAL, child of `spark.sql.N` or `spark.application`

| Attribute | Type | Description |
|-----------|------|-------------|
| `spark.job.id` | long | |
| `spark.job.stage.count` | long | Number of stages the job was planned with |
| `spark.job.description` | string | Conditional — from `spark.job.description` local property |
| `spark.job.result` | string | `SUCCESS` or `FAILED` |
| `error.type` | string | Conditional — the exception class, e.g. `java.lang.ArithmeticException` |
| `error.message` | string | Conditional — present only on `FAILED`, first 500 chars |

### `spark.stage.N` — SpanKind INTERNAL, child of `spark.job.N`

Metrics below are Spark's own sums across every task in the stage, so they are directly comparable
with each other. All are set at `onStageCompleted` from `StageInfo.taskMetrics`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `spark.stage.id` | long | |
| `spark.stage.attempt.id` | long | |
| `spark.stage.name` | string | Spark's `RDD.creationSite` — the same string the Spark UI shows. Frequently useless for async stages; see below |
| `spark.stage.sql.execution_id` | long | Conditional — present when the stage's job belongs to a SQL execution |
| `spark.stage.sql.description` | string | Conditional — the SQL execution's description, e.g. `show at SkewedJob.scala:48`. This is the attribute that names user code when `spark.stage.name` cannot |
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
| `error.type` | string | Conditional — see the failure note below; **omitted** more often here than on job or task spans |
| `error.message` | string | Conditional — present only on failure, first 500 chars |

**Why `spark.stage.name` is often useless, and what to use instead.** Spark derives it from
`RDD.creationSite`. For an async subquery or broadcast, the RDD is created on a Spark thread-pool
thread, so the name resolves to something like
`$anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768` — the Spark UI shows the same
string. In one four-stage `SkewedJob` run, three of the four stages looked like that.

`StageInfo.details` cannot recover it either: that stack was captured on the same pool thread and
contains no user frame at all, only `org.apache.spark.*` and `java.base/*`. The stages whose names
*are* useful are the only ones whose stacks contain user code, so walking the stack adds nothing
where it is needed.

`spark.stage.sql.description` is the answer: for any stage under a SQL execution, Spark's own
execution description names the user code exactly. `spark.stage.name` is left untouched so
Spark UI correlation still works. A pure-RDD stage belonging to no SQL execution gets neither
attribute — there is no source for the information in that case.

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
| `error.type` | string | Conditional — exception class for a user exception, otherwise the Spark failure reason class, e.g. `org.apache.spark.TaskResultLost` |
| `error.message` | string | Conditional — present only on `FAILED`, first 500 chars |
| `spark.task.duration_ms` | long | Wall clock on the executor thread |
| `spark.task.input.bytes` / `output.bytes` | long | |
| `spark.task.shuffle.read_bytes` / `write_bytes` | long | |
| `spark.task.peak_memory_bytes` | long | |

### Failures — `error.type`, `error.message` and the `exception` event

A failed job, stage or task span carries `error.type` and `error.message`, and status `ERROR`.
Where a stack trace is available it is attached as an OTEL `exception` span event with
`exception.type`, `exception.message` and `exception.stacktrace` (capped at 8000 chars).

Spark reports failures three different ways, which is why the detail differs by span:

| Span | Source | `error.type` | `exception` event |
|------|--------|--------------|-------------------|
| `spark.job.N` | live `Throwable` from `JobFailed` | Exception class | Yes — full driver-side stack trace |
| `spark.stage.N` | formatted string only | Recovered by pattern; **omitted** if none matches | No |
| `spark.task.executor` | `TaskFailedReason` | `ExceptionFailure.className`, else the reason class | Yes, for `ExceptionFailure` |

`error.type` is the key you group failures by, so it is left **unset** rather than guessed. On a
stage span Spark hands Flare only a formatted string, and that string names the wrapper before it
names the cause:

```
org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 1.0
failed 4 times, most recent failure: Lost task 0.3 in stage 1.0 (TID 7) (executor 1):
java.lang.ArithmeticException: / by zero
```

Reading the first class name here would report `SparkException` for practically every failed
stage. Flare instead searches after the last `most recent failure:` — or `Caused by:` when that is
absent, taking the innermost — so the reported type is `java.lang.ArithmeticException`. When
neither anchor is present it falls back to the first fully-qualified name ending in `Exception`,
`Error` or `Throwable`.

Spark 3.4+ also formats many failures as an **error class** with no exception name anywhere in the
text:

```
[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 …
== SQL (line 1, position 1) ==
id div divisor
```

When no class name is found, that leading error class is used instead — so this stage reports
`error.type = DIVIDE_BY_ZERO`. Spark error classes are stable, low-cardinality identifiers, which
makes them a good grouping key rather than a lesser one. An exception class still wins when both
are present. Only when neither is available is the attribute omitted.

A missing `error.type` means "not recoverable here", not "no error" — `error.message` and
`spark.stage.failure_reason` are still populated. Under `FLARE_TRACE_GRANULARITY=tasks` or `all`
the task span for the same failure carries the structured version, read from
`ExceptionFailure`'s own fields rather than from text.

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
| `flare.stage.executor.run_time` | histogram | `ms` | `stage.id`, `stage.name`, `sql.description` |
| `flare.stage.input.bytes` | counter | `By` | `stage.id`, `stage.name`, `sql.description` |
| `flare.stage.output.bytes` | counter | `By` | `stage.id`, `stage.name`, `sql.description` |
| `flare.stage.shuffle.read_bytes` | counter | `By` | `stage.id`, `stage.name`, `sql.description` |
| `flare.stage.shuffle.write_bytes` | counter | `By` | `stage.id`, `stage.name`, `sql.description` |
| `flare.executor.count` | updowncounter | `{executor}` | `executor.id` |
| `flare.executor.removed` | counter | `{executor}` | `executor.id`, `reason` |
| `flare.executor.excluded` | counter | `{executor}` | `executor.id` |
| `flare.block_manager.count` | updowncounter | `{block_manager}` | `executor.id` |
| `flare.rdd.unpersisted` | counter | `{rdd}` | — |
| `flare.storage.memory.bytes` | updowncounter | `By` | `executor.id` |
| `flare.storage.disk.bytes` | updowncounter | `By` | `executor.id` |
| `flare.storage.blocks` | updowncounter | `{block}` | `executor.id` |

The counters are only incremented for non-zero values, so a stage that read nothing produces no
`flare.stage.input.bytes` series rather than a flat zero one.

**Cluster lifecycle.** The `flare.executor.*`, `flare.block_manager.*` and `flare.storage.*`
instruments describe the cluster rather than any one query, and are deliberately metrics rather
than spans: an executor's lifetime is a level over time, not an operation, and an executor alive
for the whole application would otherwise be a span longer than every trace it overlaps.

They are up-down counters because they go both ways — an increment-only counter would tell you
how many executors were ever created, never how many exist now.

`flare.executor.removed` carries a `reason` tag, which is the point of it: on a dynamically
allocated cluster a routine scale-down and a crash both reduce the executor count, and only the
reason separates them. Spark's reason string is free text that sometimes embeds ids or hostnames,
so it is bucketed into a fixed set (`idle_or_decommissioned`, `preempted`, `heartbeat_timeout`,
`lost`, `killed`, `exited`, `other`, `unknown`) rather than passed through — an unbounded tag on
a counter is the cardinality problem these instruments exist to avoid.

`flare.block_manager.count` includes the **driver's** block manager, not just executors', because
Spark registers one there too. Expect it to sit one above the executor count.

The `flare.storage.*` instruments require `FLARE_TRACK_BLOCK_UPDATES=true`. They track running
totals per executor; block ids are never used as tags. Spark signals a block being dropped by
sending an invalid `StorageLevel` carrying the sizes it had, so a drop is recorded as a negative
delta rather than a separate event.

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
├── SparkContextInstrumentation        # hooks SparkContext init; app span + listener
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
