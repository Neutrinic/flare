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
| `FLARE_MAX_SPANS_PER_TRACE` | `10000` | Circuit breaker for task spans, counted **per executor**, not per trace ([#101](https://github.com/Neutrinic/flare/issues/101)): each executor stops creating task spans once it has made this many in a trace, so a trace can hold up to this many per executor. Job and stage spans continue. On an application that runs for hours task spans stop partway; see [Telemetry volume](volume.md#flares-own-spans) |
| `FLARE_METRICS_ENABLED` | `true` | Flare's metrics: task duration, shuffle bytes, stage aggregates |
| `FLARE_TRACK_BLOCK_UPDATES` | `false` | Per-block storage totals. Off by default: Spark reports every block, which on a large cached dataset floods the listener thread |
| `FLARE_SQL_PLAN_MAX_CHARS` | `4096` | Cap on `spark.sql.plan`; `0` drops the attribute |
| `FLARE_SQL_DETAILS_MAX_CHARS` | `2048` | Cap on `spark.sql.details`; `0` drops the attribute |
| `FLARE_SQL_DESCRIPTION_MAX_CHARS` | `1024` | Cap on `spark.sql.description` and the `sql.description` metric label; `0` drops both |
| `FLARE_SQL_PLAN_INITIAL_MAX_CHARS` | `0` (dropped) | Cap on `spark.sql.plan.initial`, the plan before Adaptive Query Execution |
| `FLARE_ERROR_MESSAGE_MAX_CHARS` | `500` | Cap on a failure's message, wherever it goes: `error.message`, the span status, the `exception` event and `spark.stage.failure_reason`; `0` drops it everywhere |
| `FLARE_STACKTRACE_MAX_CHARS` | `8000` | Cap on a failure's stack trace, on the `exception` event; `0` drops it |
| `FLARE_DROP_NON_SPARK_ROOTS` | `true` | Drop spans with no parent that are not Flare's, such as the platform's own HTTP calls. See [Noise](noise.md) |
| `FLARE_ENABLED` | `true` | Kill switch |

`FLARE_ENABLED` and `FLARE_DROP_NON_SPARK_ROOTS` turn off only for the literal value `false`, in any
case. Any other value leaves them on.

The `*_MAX_CHARS` caps limit size. A positive cap keeps the first characters, whatever they
contain, so it is not a privacy control; `0` is, because it stops Flare emitting the field at all.
See [Privacy](../privacy.md#limiting-what-flare-sends).

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
(`#133`), `plan_id=142`, codegen ids and AQE's runtime statistics are removed before hashing,
because they change without the shape changing. The statistics track the data, not the query, and
would fingerprint the same query differently on a busy day than a quiet one.

The fingerprint is emitted even when the caps are `0` and no plan text is exported. That is the
lever for backends with a per-trace size limit, such as Tempo's `max_bytes_per_trace`: a large plan
on every SQL span fills a trace quickly, while 16 bytes of grouping does not.

`spark.sql.plan.initial.fingerprint` is taken from the pre-AQE tree and not changed by re-plans, so
the pair is "shape as planned" against "shape as executed". Under AQE the two differ for almost
every query, because the final tree always gains query-stage wrappers, so a difference does not
mean AQE changed anything. The initial fingerprint is the more stable grouping key.

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
