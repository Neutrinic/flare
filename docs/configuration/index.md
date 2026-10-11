# Configuration

Flare's own settings are `FLARE_*` keys. Set them as system properties (`-DFLARE_*` in
`extraJavaOptions`) or as environment variables; system properties win. Exporter settings are the
agent's own `otel.*` keys, see [Exporting](exporting.md).

| Key | Default | Description |
|---|---|---|
| `FLARE_TRACE_GRANULARITY` | `stages` | `jobs`, `stages`, `tasks` or `all`. Task spans need `tasks` or `all` |
| `FLARE_SAMPLING_RATIO` | | **Removed** in 1.3.0: it never had an effect ([#130](https://github.com/Neutrinic/flare/issues/130)). Ignored with a warning. See [Sampling](#sampling) |
| `FLARE_SLOW_TASK_MS` | `0` (off) | Only emit task spans that took longer than this. Spans and log lines from inside tasks then sit under the stage span; see [Spans](../reference/spans.md) |
| `FLARE_RETRY_TASKS_ONLY` | `false` | Only emit task spans for retries and speculative tasks |
| `FLARE_TASK_STAGE_PATTERN` | none | Only emit task spans for stages whose name matches this regex, such as `.*Etl\.scala.*`, or `.*/etl[.]py:.*` for a PySpark script. The regex must match the whole name, which is the call site, `collect at Etl.scala:42`. Set it on the **driver**: the driver decides as each stage is submitted, and the executors follow. See [Choosing stages](#choosing-stages-for-task-spans) |
| `FLARE_TASK_STAGES` | none | Only emit task spans for these stage ids, comma-separated, such as `7,12`. Read by the executors. Ids restart in every application and move when the code changes, so prefer the pattern |
| `FLARE_MAX_SPANS_PER_TRACE` | `10000` | Circuit breaker for task spans, counted **per executor**, not per trace ([#101](https://github.com/Neutrinic/flare/issues/101)): each executor stops creating task spans once it has made this many in a trace, so a trace can hold up to this many per executor. Job and stage spans continue. On an application that runs for hours task spans stop partway; see [Telemetry volume](volume.md#flares-own-spans) |
| `FLARE_METRICS_ENABLED` | `true` | Flare's metrics: task duration, shuffle bytes, stage aggregates |
| `FLARE_TRACK_BLOCK_UPDATES` | `false` | Per-block storage totals. Off by default: Spark reports every block, which on a large cached dataset floods the listener thread |
| `FLARE_SQL_PLAN_MAX_CHARS` | `4096` | Cap on `spark.sql.plan`; `0` drops the attribute |
| `FLARE_SQL_DETAILS_MAX_CHARS` | `2048` | Cap on `spark.sql.details`; `0` drops the attribute |
| `FLARE_SQL_DESCRIPTION_MAX_CHARS` | `1024` | Cap on `spark.sql.description` and the `sql.description` metric label; `0` drops both |
| `FLARE_SQL_PLAN_INITIAL_MAX_CHARS` | `0` (dropped) | Cap on `spark.sql.plan.initial`, the plan before Adaptive Query Execution |
| `FLARE_ERROR_MESSAGE_MAX_CHARS` | `500` | Cap on a failure's message, wherever it goes: `error.message`, the span status, the `exception` event and `spark.stage.failure_reason`; `0` drops it everywhere, and the stack trace with it, since a printed stack trace opens with the message |
| `FLARE_STACKTRACE_MAX_CHARS` | `8000` | Cap on a failure's stack trace, on the `exception` event; `0` drops it |
| `FLARE_DROP_NON_SPARK_ROOTS` | `true` | Drop spans with no parent that are not Flare's, such as the platform's own HTTP calls. See [Noise](noise.md) |
| `FLARE_ENABLED` | `true` | Kill switch: `false` stops Flare's spans, metrics and resource attributes. The agent stays as quiet as with Flare on: its own instrumentation stays off and non-Spark root spans are still dropped. To have the agent's full instrumentation back, set `otel.instrumentation.common.default-enabled=true` and `FLARE_DROP_NON_SPARK_ROOTS=false` as well |

`FLARE_ENABLED` and `FLARE_DROP_NON_SPARK_ROOTS` turn off only for the literal value `false`, in any
case. Any other value leaves them on.

The `*_MAX_CHARS` caps limit size. A positive cap keeps the first characters, whatever they
contain, so it is not a privacy control; `0` is, because it stops Flare emitting the field at all.
See [Privacy](../privacy.md#limiting-what-flare-sends).

### Choosing stages for task spans

Task spans are the most numerous Flare emits, so on a large job it can pay to keep them for the
stages being investigated only. `FLARE_TASK_STAGE_PATTERN` selects stages by name, the call site that
created them, as the Spark UI shows it: `collect at Etl.scala:42`, `map at Ingest.scala:17`. A name
survives between runs unless its line of code moves, which makes it the setting to reach for;
some stages that adaptive execution adds are named after Spark's own call sites instead.
`FLARE_TASK_STAGES` selects by stage id, which is only good for one run.

With both set, a task needs both. Tasks the filters leave out still count in the task metrics; only
their spans are dropped.

Find the name in the Spark UI's Stages tab. The regex must match all of it, so start it with `.*`.
Writing `[.]` for a dot, rather than `\.`, saves escaping the backslash inside
`extraJavaOptions` and the shell.

#### PySpark

A stage carries your Python line when the action brings rows back to Python: DataFrame actions
such as `collect`, `take`, `head`, `first`, `tail`, `toLocalIterator` and `toPandas`, and RDD
actions. Actions that do not, such as writes, `count()` and `show()`, are named after the JVM side
of the call, and a pattern on your script's name cannot select their stages:

| Action | Stage name |
|---|---|
| `df.collect()`, `df.toPandas()`, `rdd.sum()` | The action and your line: `collect at /opt/jobs/etl.py:42` |
| `df.write.parquet(...)` | `parquet at NativeMethodAccessorImpl.java:0` |
| `df.count()`, `df.show()` | Spark's own frames, such as `$anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768` |

Match the script's file name, not its path: `.*/etl[.]py:.*`. The path is wherever the driver
runs the script from, which changes between client and cluster mode and between schedulers. Add a
line, `.*/etl[.]py:42`, to select one action; editing the script moves it.

Selecting by job description, which covers these stages, is planned
([#237](https://github.com/Neutrinic/flare/issues/237)).

#### Jupyter

In a Jupyter notebook, the name is the cell's: `collect at /tmp/ipykernel_141779/2448266874.py:1`.
The directory changes with every kernel. The file name is a hash of the cell's code: it is the same
in a new kernel, and changes when the cell is edited. `.*/2448266874[.]py:.*` selects that cell's
stages until then. Set the pattern in `spark.driver.extraJavaOptions` on the session builder,
before the first `getOrCreate()`, since the driver reads it at start-up.

#### Databricks

Databricks wraps PySpark's actions, so every Python stage is named `wrapper`, with your file and
line after it. As [elsewhere](#pyspark), actions that bring rows back to Python carry them;
`count()` and writes are named after Databricks' frames, such as
`$anonfun$withThreadLocalCaptured$10 at LexicalThreadLocal.scala:63`.

| Task | Stage name | Pattern |
|---|---|---|
| Python script | `wrapper at /Workspace/Users/me/etl.py:16` | `.*/etl[.]py:.*` |
| Notebook | `wrapper at /root/.ipykernel/2188/command-8170623636178775-1586284934:1` | `.*/command-8170623636178775-.*` |

In a notebook, the number after `command-` identifies the cell. It was the same on every cluster
and runtime the cell ran on (DBR 15.4 and 17.3), while the directory changes with each cluster.
Set the pattern in the cluster's `spark.driver.extraJavaOptions`.

## Sampling

The agent's sampler decides what is traced. Its default, `parentbased_always_on`, traces every
application. To trace a fraction of applications, use the agent's ratio sampler:

```text
-Dotel.traces.sampler=parentbased_traceidratio -Dotel.traces.sampler.arg=0.1
```

Every Flare span descends from the `spark.application` root, and the executors inherit the root's
decision through the trace context, so this samples whole applications, never parts of one. Set it
on the driver; on the executors the parent decides. Ten lab runs at `0.5` gave seven complete traces
and three with nothing exported from the driver or the executors, and none partial.

## SQL plans

A physical plan has no size limit at the source: a wide query runs to tens of kilobytes, which can
push an OTLP batch past a collector's message limit and lose the whole batch. The caps prevent
that. When a plan is clipped, `spark.sql.plan.truncated=true` is set with it, so a partial plan
never reads as a complete one. Raise `FLARE_SQL_PLAN_MAX_CHARS` if your collector accepts larger
payloads.

`spark.sql.plan` is the plan that **ran**. Spark reports the physical plan when the execution
starts, before Adaptive Query Execution (AQE) re-plans, and each re-plan replaces the attribute, so
the final tree is what remains. Set `FLARE_SQL_PLAN_INITIAL_MAX_CHARS` above `0` to keep the pre-AQE
tree as `spark.sql.plan.initial` too. AQE's decisions (skew splits, broadcast conversion, partition
coalescing) are only visible by comparing the two. It is off by default because it doubles the
worst-case plan payload on every SQL span.

### Plan fingerprints

`spark.sql.plan.fingerprint` hashes the plan's **shape** into 16 hex characters, so the same query
groups across executions and across applications, which the Spark UI cannot do. Expression ids
(`#133`), `plan_id=142`, codegen ids, AQE's query-stage ids and its runtime statistics are removed
before hashing, because they change without the shape changing. AQE numbers its query stages in
the order it creates them, which differs from run to run. The statistics track the data, not the query, and
would fingerprint the same query differently on a busy day than a quiet one.

Literal values and input paths are removed too: the value compared with a column in a filter
(`dt = 2024-01-01`, `id > 100`, a string value even when it contains parentheses), the literal values
in an `IN` list, however many there are, the values of pushed-down filters, and the paths a scan
reads from. A daily job filtering on the day's date is the same query every day. What still makes
two plans different is their shape: the operators, the columns, which columns a filter compares and
how, and any operand that names a column, such as a join key or a column in an `IN` list.

The fingerprint is emitted even when the caps are `0` and no plan text is exported. That is the
lever for backends with a per-trace size limit, such as Tempo's `max_bytes_per_trace`: a large plan
on every SQL span fills a trace quickly, while 16 bytes of grouping does not.

`spark.sql.plan.initial.fingerprint` is taken from the pre-AQE tree and not changed by re-plans, so
the pair is "shape as planned" against "shape as executed". Under AQE the two differ for almost
every query, because the final tree always gains query-stage wrappers, so a difference does not
mean AQE changed anything. The initial fingerprint is the more stable grouping key: the final
fingerprint can still differ between two runs of the same query on the same data, when AQE reuses a
different copy of an exchange the query computes twice. Over TPC-DS's queries, run twice in one
application on the lab, the initial fingerprint was the same for every query and the final one
differed for 6 of 102. Group by the initial fingerprint to tell when a query changed, and use the
final one to see what AQE made of it.

## Resource attributes

Flare adds two attributes to the OpenTelemetry resource, so they are on every span, metric point and
log record the JVM exports:

| Attribute | Example | Description |
|---|---|---|
| `flare.version` | `1.3.0` | Flare build version, or `unknown` if the build metadata cannot be read |
| `flare.role` | `driver`, `executor-3` | Which side of the cluster the JVM is on |

They come from the agent extension, so they are missing on a JVM where
`-Dotel.javaagent.extensions` was not set.

`flare.role` identifies the individual executor, so under dynamic allocation its set of values
grows as executors come and go. Prometheus-style metric stores keep resource attributes off the
series (they appear on `target_info`), but Loki turns them into stream labels, so weigh that against
log stream cardinality on large elastic clusters.

!!! warning "Sensitive data in failures"
    When a job fails, the exception message is recorded on the span. Spark exceptions sometimes
    quote the data being processed, in parse errors or type mismatches for example. Secure your
    backend accordingly if your jobs handle sensitive data.
