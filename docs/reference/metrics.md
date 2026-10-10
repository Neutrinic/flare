# Metrics

Nineteen instruments, all under the `io.flare.spark` meter, all turned off by
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
| `flare.job.duration` | histogram | `ms` | `job.result`, `sql.description` |
| `flare.application.duration` | histogram | `s` | `application.result` |
| `flare.application.end_time` | gauge | `s` | `application.result` |
| `flare.executor.count` | updowncounter | `{executor}` | `executor.id` |
| `flare.executor.removed` | counter | `{executor}` | `executor.id`, `reason` |
| `flare.executor.excluded` | counter | `{executor}` | `executor.id` |
| `flare.block_manager.count` | updowncounter | `{block_manager}` | `executor.id` |
| `flare.rdd.unpersisted` | counter | `{rdd}` | none |
| `flare.storage.memory.bytes` | updowncounter | `By` | `executor.id` |
| `flare.storage.disk.bytes` | updowncounter | `By` | `executor.id` |
| `flare.storage.blocks` | updowncounter | `{block}` | `executor.id` |

## Histogram buckets

| Histogram | Bucket boundaries |
|---|---|
| `flare.task.duration` | 5, 10, 25, 50, 100, 250, 500 ms; 1, 2.5, 5, 10, 30 s; 1, 2, 5, 10, 30, 60 min |
| `flare.stage.executor.run_time` | 100 ms; 1, 5, 10, 30 s; 1, 5, 10, 30 min; 1, 3, 10, 24 h. Summed across the stage's tasks, so far longer than its wall-clock time |
| `flare.task.records_throughput` | 10 to 100,000,000 records/s, one boundary per power of ten |

They are set as advice on each instrument, so a view configured in the SDK or the agent overrides them.

No label is new for every stage. `stage.name` and `sql.description` are call sites, such as
`collect at Job.scala:42`, so stages from the same line of code share a series, and the number of
series depends on the code, not on how long the application runs. A single stage is on its
`spark.stage` span. Up to 1.2.0 the task and stage metrics also carried `stage.id`, which made one
series per stage ([#136](https://github.com/Neutrinic/flare/issues/136)).

`executor.id` is the one label that grows with the application's lifetime. Spark never reuses an
executor id, and with cumulative temporality, the default, the SDK keeps every series it has seen
and re-sends it on each export, including those of executors long gone. Delta temporality
(`OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta`) sends only what changed since the last
export, though the backend still stores a series per id. On a fixed cluster that is a handful of series. Under dynamic
allocation or autoscaling, an application that runs for days adds tens to hundreds of ids a day,
and past 2,000 series per instrument the SDK folds new ones into a single overflow series, losing
the per-executor breakdown. Restarting the application starts the count again
([#144](https://github.com/Neutrinic/flare/issues/144)).

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

Each executor is counted once on removal, with the reason Spark gave first. Spark removes a
decommissioned executor twice, once as decommissioned and again when its process exits; the second
is ignored, so a scale-down by decommissioning reads `idle_or_decommissioned`, not also `exited`.

`flare.block_manager.count` includes the **driver's** block manager, not just executors', because
Spark registers one there too. Expect it to sit one above the executor count.

The `flare.storage.*` instruments require `FLARE_TRACK_BLOCK_UPDATES=true`. They track running
totals per executor; block ids are never used as tags. Spark reports each block's state, again
whenever it changes, such as moving from memory to disk, so Flare keeps each block's last reported
sizes on the driver and records only the difference. An invalid `StorageLevel` means the block is
gone; when a block manager is removed, its blocks are taken out of the totals with it.

`flare.task.*` are recorded on the executor while the task span's scope is still open, so the SDK's
default `trace_based` exemplar filter attaches an exemplar linking each measurement back to its
trace. Under `FLARE_SLOW_TASK_MS` a fast task's span is dropped, so its measurement's exemplar
names the task's stage instead, which is always exported, when the stage's context is current on
the executor thread. A task that carries no trace context, such as one from an unsampled
application, gets no exemplar. A slow task's exemplar names the task span. Some backends drop exemplars by default: Mimir's
`max_global_exemplars_per_user` is `0` unless you set it.

## Outcomes

`flare.job.duration` and `flare.application.duration` record one point per job and one per
application, from the driver, so a scheduled pipeline can be alerted on from metrics: whether a
run happened, whether it failed, and whether a query took longer than its last runs. The same
facts are on the spans, which alert rules cannot read.

- **`flare.job.duration`** runs from the job's submission to its end, by Spark's own clock.
  `job.result` is `SUCCESS` or `FAILED`; `sql.description` is set when the job belongs to a query,
  so one query's runs share a series.
- **`flare.application.duration`** is recorded once, when the application ends: on a normal stop,
  or at shutdown when the JVM goes down without one. Spark reports no result for an application,
  so `application.result` is `FAILED` when **any of its jobs failed**, or when a job was still
  running as it ended: an application killed mid-run, by an orchestrator's timeout or a SIGTERM,
  reads `FAILED`. An application that catches a failed job and carries on still reads `FAILED`, and
  one that fails outside any job, in driver code or before its first query runs, reads `SUCCESS`.
- **`flare.application.end_time`** is set alongside it, to when the application ended in seconds
  since the Unix epoch, with the same `application.result`. It puts runs in order, which the
  example rule that alerts only on a service's latest run needs
  ([#202](https://github.com/Neutrinic/flare/issues/202)).

Neither carries an application or run id, so successive runs of the same service can be compared.
`sql.description`, here and on the stage metrics, is the query's call site, such as
`collect at Job.scala:42`, or the job description when you set one with `setJobDescription`. Keep
those descriptions stable: one that embeds a date, a batch id or any value that changes per run
makes a new series every time.

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

The job count comes from Tempo's span metrics, which need its
[metrics generator](https://grafana.com/docs/tempo/latest/metrics-from-traces/metrics-generator/);
without it, that panel stays empty and the rest still work.
