# Metrics

Seventeen instruments, all under the `io.flare.spark` meter, all turned off by
`FLARE_METRICS_ENABLED=false`.

| Instrument | Kind | Unit | Labels |
|------------|------|------|--------|
| `flare.task.duration` | histogram | `ms` | `executor.id`, `task.result` |
| `flare.task.records_throughput` | histogram | `{records}/s` | `executor.id`, `task.result` |
| `flare.task.shuffle.read_bytes` | counter | `By` | `executor.id`, `task.result` |
| `flare.task.shuffle.write_bytes` | counter | `By` | `executor.id`, `task.result` |
| `flare.stage.executor.run_time` | histogram | `ms` | `stage.name`, `sql.description` |
| `flare.stage.input.bytes` | counter | `By` | `stage.name`, `sql.description` |
| `flare.stage.output.bytes` | counter | `By` | `stage.name`, `sql.description` |
| `flare.stage.shuffle.read_bytes` | counter | `By` | `stage.name`, `sql.description` |
| `flare.stage.shuffle.write_bytes` | counter | `By` | `stage.name`, `sql.description` |
| `flare.executor.count` | updowncounter | `{executor}` | `executor.id` |
| `flare.executor.removed` | counter | `{executor}` | `executor.id`, `reason` |
| `flare.executor.excluded` | counter | `{executor}` | `executor.id` |
| `flare.block_manager.count` | updowncounter | `{block_manager}` | `executor.id` |
| `flare.rdd.unpersisted` | counter | `{rdd}` | none |
| `flare.storage.memory.bytes` | updowncounter | `By` | `executor.id` |
| `flare.storage.disk.bytes` | updowncounter | `By` | `executor.id` |
| `flare.storage.blocks` | updowncounter | `{block}` | `executor.id` |

No label is new for every stage. `stage.name` and `sql.description` are call sites, such as
`collect at Job.scala:42`, so stages from the same line of code share a series, and the number of
series depends on the code, not on how long the application runs. A single stage is on its
`spark.stage` span. Up to 1.2.0 the task and stage metrics also carried `stage.id`, which made one
series per stage ([#136](https://github.com/Neutrinic/flare/issues/136)).

The counters are only incremented for non-zero values, so a stage that read nothing produces no
`flare.stage.input.bytes` series rather than a flat zero one.

**Cluster lifecycle.** The `flare.executor.*`, `flare.block_manager.*` and `flare.storage.*`
instruments describe the cluster rather than any one query, and are deliberately metrics rather
than spans: an executor's lifetime is a level over time, not an operation, and an executor alive
for the whole application would otherwise be a span longer than every trace it overlaps.

They are up-down counters because they go both ways. An increment-only counter would tell you
how many executors were ever created, never how many exist now.

`flare.executor.removed` carries a `reason` tag, which is the point of it: on a dynamically
allocated cluster a routine scale-down and a crash both reduce the executor count, and only the
reason separates them. Spark's reason string is free text that sometimes embeds ids or hostnames,
so it is bucketed into a fixed set (`idle_or_decommissioned`, `preempted`, `heartbeat_timeout`,
`lost`, `killed`, `exited`, `other`, `unknown`) rather than passed through: an unbounded tag on
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
was never exported. Some backends drop exemplars by default: Mimir's
`max_global_exemplars_per_user` is `0` unless you set it.

## In Prometheus

Prometheus-compatible backends rename the instruments: dots become underscores, and most add the
unit, so `flare.task.duration` is stored as `flare_task_duration_milliseconds_bucket` and
`flare.stage.input.bytes` as `flare_stage_input_bytes_total`. Mimir's OTLP ingestion leaves the
unit off by default (`flare_task_duration_bucket`, `flare_stage_input_bytes`). Labels follow the
same rule: `executor.id` becomes `executor_id`, and `service.name` becomes `job`.

## Dashboard

The repository ships a Grafana dashboard,
[`flare-spark.json`](https://github.com/Neutrinic/flare/blob/main/docker/grafana/provisioning/dashboards/flare-spark.json).
Import it under **Dashboards → New → Import**, then pick your metrics, logs and traces data
sources and the services to show at the top. It reads the metric names with or without the unit,
so it works on Prometheus, Mimir and Grafana Cloud alike.

The job and stage counts come from Tempo's span metrics, which need its
[metrics generator](https://grafana.com/docs/tempo/latest/metrics-from-traces/metrics-generator/);
without it, those two panels stay empty and the rest still work.
