# Alerting

A scheduled pipeline has an expected shape: it runs on a cadence, reads about as much as last time,
and takes about as long. The useful alerts compare each run with the runs before it. Flare's
metrics carry no run or application id, so successive runs of a service share series and can be
compared.

The repository ships example Prometheus rules,
[`alerting/flare-rules.yml`](https://github.com/Neutrinic/flare/blob/main/alerting/flare-rules.yml),
for Prometheus, the Mimir ruler or Grafana alerting. They are **starting points**. Every threshold
below depends on the workload, and a rule that fires constantly is worse than none, so read why
each one is set the way it is before relying on it.

## Outcomes

These use the [outcome metrics](../reference/metrics.md#outcomes): one point per job and one per
application.

| Alert | Fires when | Notes |
|---|---|---|
| `FlareApplicationFailed` | A run ended in the last hour with a failed job | Spark reports no result for an application, so Flare's `application.result` is `FAILED` when any job failed. An application that fails outside any job is not caught |
| `FlareJobFailed` | A job failed in the last hour, per query | For pipelines that catch a failed job and carry on |
| `FlareQuerySlowerThanUsual` | A query's p95 job duration in its latest runs is more than twice its p95 over the previous week | Needs a week of history. Queries are told apart by `sql.description`, so give yours descriptions with `setJobDescription` |
| `FlareInputVolumeDropped` | A query read less than half the bytes it read at the same time the day before | The classic silent failure: an upstream that delivered a partial or empty drop, and a load that succeeded on it. Assumes a daily schedule; change `offset 1d` for other cadences |

### A run that did not happen

The first question of all, whether last night's run happened, needs the pipeline's service name,
so it is not in the rules file: shipped with a placeholder, it would fire as soon as it was loaded.
Add one per pipeline, with the absence window a little longer than the schedule:

```yaml
- alert: NightlyEtlDidNotRun
  expr: |
    absent_over_time({__name__=~"flare_application_duration(_seconds)?_count", job="nightly-etl-driver"}[26h])
  labels:
    severity: critical
  annotations:
    summary: "nightly-etl has not finished a run in 26 hours"
```

`job` is the driver's `service.name`. The metric is recorded when the application ends, so this
fires for a run that never started and for one that is still running past its window.

## Diagnoses

| Alert | Fires when |
|---|---|
| `FlareTaskSkew` | The slowest 1% of tasks take more than ten times the median, over more than 100 tasks in 30 minutes. The ratio is the dashboard's skew panel |
| `FlareExecutorsLost` | More than two executors were removed in 15 minutes for a reason other than a dynamic-allocation scale-down |

## On the spans

Several diagnoses are only on the spans, as stage attributes, which alert rules cannot read. They
are TraceQL searches in Tempo; Grafana can alert on TraceQL metrics queries where Tempo's metrics
generator is set up.

| Diagnosis | TraceQL |
|---|---|
| Stages that spilled to disk | `{ span.spark.stage.disk.spilled_bytes > 0 }` |
| GC-bound stages | `{ span.spark.stage.jvm.gc_time_ms > 60000 }`, then compare with `spark.stage.executor.run_time_ms` on the span |
| Executor starvation | `{ span.spark.stage.scheduler.delay_ms > 30000 }`: the stage's tasks waited for executor slots |
| Retried tasks | `{ name = "spark.task.executor" && span.spark.task.attempt.id > 0 }` (needs task spans) |
| Failed stages | `{ name =~ "spark.stage.*" && status = error }`, with the reason in `spark.stage.failure_reason` |
| A query whose plan changed | Below: more than one fingerprint for one query means it started planning differently |

The plan-change search is a TraceQL metrics query, counting one query's executions by plan shape:

```text
{ span.spark.sql.description = "load orders" } | count_over_time() by (span.spark.sql.plan.fingerprint)
```

## How the rules read the metrics

- **Both naming styles.** Names are matched with or without the unit
  (`flare_job_duration_milliseconds_bucket` or `flare_job_duration_bucket`), as the dashboard does;
  see [In Prometheus](../reference/metrics.md#in-prometheus).
- **One set of series per run.** Each run is a new JVM with its own `instance`, from
  `service.instance.id`, so its series start with the first value they report. `increase()` reads
  zero on a series that starts that way, so the outcome rules test whether a series appeared in the
  window, with `max_over_time`, instead. If you set a fixed `service.instance.id`, runs share series
  and `increase()` is the right function.
- **Tested.** [`alerting/flare-rules.test.yml`](https://github.com/Neutrinic/flare/blob/main/alerting/flare-rules.test.yml)
  holds promtool unit tests, which CI runs:
  `promtool test rules alerting/flare-rules.test.yml`.
