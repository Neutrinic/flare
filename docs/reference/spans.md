# Spans

Everything below is what Flare actually emits. Attributes marked *conditional* are absent rather
than zero when the underlying value was never observed. A missing attribute means "not measured",
which is different from a measured zero.

## `spark.application`

*SpanKind SERVER, trace root.*

Opened when `SparkContext` initialises, closed at `SparkContext.stop()`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `spark.application.id` | string | `SparkContext.applicationId` |
| `spark.application.name` | string | `SparkContext.appName` |
| `spark.master.url` | string | `SparkContext.master` |
| `flare.version` | string | Flare build version |
| `flare.trace.granularity` | string | Effective `FLARE_TRACE_GRANULARITY` |

## `spark.sql.N`

*SpanKind INTERNAL, child of `spark.application`.*

One per `SparkListenerSQLExecutionStart`. `N` is the execution id.

| Attribute | Type | Description |
|-----------|------|-------------|
| `spark.sql.execution.id` | long | Matches the `N` in the span name |
| `spark.sql.description` | string | Conditional: omitted when Spark reports it empty. Capped by `FLARE_SQL_DESCRIPTION_MAX_CHARS` |
| `spark.sql.details` | string | Conditional: the call-site stack. Capped by `FLARE_SQL_DETAILS_MAX_CHARS` |
| `spark.sql.plan` | string | The physical plan **that ran**, post-AQE. Capped by `FLARE_SQL_PLAN_MAX_CHARS` |
| `spark.sql.plan.truncated` | bool | Conditional: set only when the cap clipped the plan |
| `spark.sql.plan.initial` | string | Conditional: the pre-AQE plan. Off unless `FLARE_SQL_PLAN_INITIAL_MAX_CHARS > 0` |
| `spark.sql.plan.initial.truncated` | bool | Conditional: as above, for the initial plan |
| `spark.sql.plan.fingerprint` | string | 16 hex chars. Hash of the plan's *shape*. Always emitted when a plan exists, **including when the caps are `0`** |
| `spark.sql.plan.initial.fingerprint` | string | As above for the pre-AQE plan. The more stable of the two, see [Plan fingerprints](../configuration/index.md#plan-fingerprints) |

## `spark.job.N`

*SpanKind INTERNAL, child of `spark.sql.N` or `spark.application`.*

| Attribute | Type | Description |
|-----------|------|-------------|
| `spark.job.id` | long | |
| `spark.job.stage.count` | long | Number of stages the job was planned with |
| `spark.job.description` | string | Conditional: from `spark.job.description` local property |
| `databricks.job.id` | string | Conditional: the Lakeflow job that submitted the Spark job, on Databricks |
| `databricks.job.run_id` | string | Conditional: that job's run. Every task of one run shares it |
| `databricks.task.run_id` | string | Conditional: the task run. A retried task gets a new one |
| `spark.job.result` | string | `SUCCESS` or `FAILED` |
| `error.type` | string | Conditional: the exception class, e.g. `java.lang.ArithmeticException` |
| `error.message` | string | Conditional: present only on `FAILED`, first 500 chars |

## `spark.stage.N`

*SpanKind INTERNAL, child of `spark.job.N`.*

Metrics below are Spark's own sums across every task in the stage, so they are directly comparable
with each other. All are set at `onStageCompleted` from `StageInfo.taskMetrics`, except
`spark.stage.scheduler.delay_ms`, which Spark does not report and Flare derives from task ends; see
below.

| Attribute | Type | Description |
|-----------|------|-------------|
| `spark.stage.id` | long | |
| `spark.stage.attempt.id` | long | |
| `spark.stage.name` | string | Spark's `RDD.creationSite`, the same string the Spark UI shows. Frequently useless for async stages; see below |
| `spark.stage.sql.execution_id` | long | Conditional: present when the stage's job belongs to a SQL execution |
| `spark.stage.sql.description` | string | Conditional: the SQL execution's description, e.g. `show at SkewedJob.scala:48`. This is the attribute that names user code when `spark.stage.name` cannot |
| `spark.stage.task.count` | long | |
| `spark.stage.executor.run_time_ms` | long | |
| `spark.stage.executor.cpu_time_ms` | long | Converted from Spark's nanoseconds |
| `spark.stage.jvm.gc_time_ms` | long | GC time rivalling run time is the answer to "why is this stage slow" |
| `spark.stage.executor.deserialize_time_ms` | long | |
| `spark.stage.executor.deserialize_cpu_time_ms` | long | Converted from nanoseconds |
| `spark.stage.result.serialization_time_ms` | long | |
| `spark.stage.scheduler.delay_ms` | long | Conditional: derived, see below |
| `spark.stage.input.bytes` / `.records` | long | |
| `spark.stage.output.bytes` / `.records` | long | |
| `spark.stage.shuffle.read_bytes` | long | |
| `spark.stage.shuffle.write_bytes` | long | |
| `spark.stage.memory.spilled_bytes` | long | |
| `spark.stage.disk.spilled_bytes` | long | |
| `spark.stage.failure_reason` | string | Conditional: first 500 chars |
| `error.type` | string | Conditional: see the failure note below; **omitted** more often here than on job or task spans |
| `error.message` | string | Conditional: present only on failure, first 500 chars |

**Why `spark.stage.name` is often useless, and what to use instead.** Spark derives it from
`RDD.creationSite`. For an async subquery or broadcast, the RDD is created on a Spark thread-pool
thread, so the name resolves to something like
`$anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768`. The Spark UI shows the same
string. In one four-stage `SkewedJob` run, three of the four stages looked like that.

`StageInfo.details` cannot recover it either: that stack was captured on the same pool thread and
contains no user frame at all, only `org.apache.spark.*` and `java.base/*`. The stages whose names
*are* useful are the only ones whose stacks contain user code, so walking the stack adds nothing
where it is needed.

`spark.stage.sql.description` is the answer: for any stage under a SQL execution, Spark's own
execution description names the user code exactly. `spark.stage.name` is left untouched so
Spark UI correlation still works. A pure-RDD stage belonging to no SQL execution gets neither
attribute: there is no source for the information in that case.

`spark.stage.scheduler.delay_ms` is the only stage attribute Spark does not report. It is derived
per task as `duration − executorRunTime − deserializeTime − resultSerializationTime −
resultFetchTime` and summed over the stage, matching Spark's own `AppStatusUtils.schedulerDelay`.
The clamp at zero is applied per task, never to the sum, because driver and executor clocks differ
and a fast task can report more run time than its own wall clock. The attribute is **omitted** when
no task ends were observed, because a `0` there would read as "no queueing" rather than "not measured".

## `spark.task.executor`

*SpanKind INTERNAL, child of `spark.stage.N`, emitted on the executor JVM.*

Subject to `FLARE_TRACE_GRANULARITY`, `FLARE_SLOW_TASK_MS`, `FLARE_RETRY_TASKS_ONLY` and
`FLARE_MAX_SPANS_PER_TRACE`. This span is built from `TaskContext`, so driver-only `TaskInfo`
fields (host, locality, speculative) are not available to it.

Under `FLARE_SLOW_TASK_MS`, spans made inside a task, such as a JDBC query, and the task's log
lines are children of the stage span, not the task span. Whether a task span is kept is only known
when the task ends, and anything pointing at a dropped one would point at nothing.

| Attribute | Type | Description |
|-----------|------|-------------|
| `spark.task.partition.id` | long | |
| `spark.task.attempt.id` | long | `> 0` means a retry |
| `spark.stage.id` | long | Conditional: only under `FLARE_TRACE_GRANULARITY=all`. The parent span already identifies the stage; this repeats it so you can filter without a join |
| `spark.task.sql.execution_id` | long | Conditional: present when the task belongs to a SQL execution |
| `spark.task.result` | string | `SUCCESS`, `FAILED`, `KILLED` if Spark killed the task (a losing speculative attempt, or a cancelled job or stage), or `SHUTDOWN` if the JVM went down mid-task. Only `FAILED` and `SHUTDOWN` set an error status |
| `spark.task.kill_reason` | string | Conditional: why Spark killed a `KILLED` task, such as `another attempt succeeded` |
| `error.type` | string | Conditional: exception class for a user exception, otherwise the Spark failure reason class, e.g. `org.apache.spark.TaskResultLost` |
| `error.message` | string | Conditional: present only on `FAILED`, first 500 chars |
| `spark.task.duration_ms` | long | Wall clock on the executor thread |
| `spark.task.input.bytes` / `output.bytes` | long | |
| `spark.task.shuffle.read_bytes` / `write_bytes` | long | |
| `spark.task.peak_memory_bytes` | long | |

## Failures

`error.type`, `error.message` and the `exception` event.

A failed job, stage or task span carries `error.message` and status `ERROR`, and `error.type` when
the failure's type can be recovered, which on stage spans it sometimes cannot (see below).
Where a stack trace is available it is attached as an OTEL `exception` span event with
`exception.type`, `exception.message` and `exception.stacktrace` (capped at 8000 chars).

Spark reports failures three different ways, which is why the detail differs by span:

| Span | Source | `error.type` | `exception` event |
|------|--------|--------------|-------------------|
| `spark.job.N` | live `Throwable` from `JobFailed` | Exception class | Yes, the full driver-side stack trace |
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
stage. Flare instead searches after the last `most recent failure:`, or after the innermost
`Caused by:` when that is absent, so the reported type is `java.lang.ArithmeticException`. When
neither anchor is present it falls back to the first fully-qualified name ending in `Exception`,
`Error` or `Throwable`.

Spark 3.4+ also formats many failures as an **error class** with no exception name anywhere in the
text:

```
[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 …
== SQL (line 1, position 1) ==
id div divisor
```

When no class name is found, that leading error class is used instead, so this stage reports
`error.type = DIVIDE_BY_ZERO`. Spark error classes are stable, low-cardinality identifiers, which
makes them a good grouping key rather than a lesser one. An exception class still wins when both
are present. Only when neither is available is the attribute omitted.

A missing `error.type` means "not recoverable here", not "no error": `error.message` and
`spark.stage.failure_reason` are still populated. Under `FLARE_TRACE_GRANULARITY=tasks` or `all`
the task span for the same failure carries the structured version, read from
`ExceptionFailure`'s own fields rather than from text.

The four byte counts and peak memory come from `TaskContext.taskMetrics()`, which is
`private[spark]` and can throw from outside `org.apache.spark` or in barrier mode. Flare logs at
debug and emits the span without them rather than failing the task, so all five are absent together
when that happens.
