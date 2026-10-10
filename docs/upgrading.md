# Upgrading

## 1.3 to 1.4

### Histograms have new bucket boundaries

`flare.task.duration`, `flare.stage.executor.run_time` and `flare.task.records_throughput` used
the SDK's default boundaries, which stop at 10,000: every task or stage longer than ten seconds
landed in the overflow bucket, and percentiles stopped at 10 s
([#180](https://github.com/Neutrinic/flare/issues/180)). They now reach an hour per task, a day of
summed stage time, and 100 million records a second; see
[Histogram buckets](reference/metrics.md#histogram-buckets).

- `histogram_quantile` queries need no change, and resolve long tasks for the first time.
- A query or alert that names an `le` value the new lists dropped must change; one that names a
  boundary both lists share, such as `le="10000"`, keeps working. Dropped boundaries, whose series
  stop at the upgrade:

    | Histogram | No longer a boundary |
    |---|---|
    | `flare.task.duration` | 0, 75, 750, 7500 |
    | `flare.stage.executor.run_time` | 0, 5, 10, 25, 50, 75, 250, 500, 750, 2500, 7500 |
    | `flare.task.records_throughput` | 0, 5, 25, 50, 75, 250, 500, 750, 2500, 5000, 7500 |

### Tasks Spark killed are `KILLED`, not `FAILED`

With speculation on, Spark kills the slower copy of a task once another succeeds, and it kills the
running tasks of a cancelled job or stage. These were recorded as `task.result=FAILED`, with error
spans, so a healthy run with speculation showed task errors
([#198](https://github.com/Neutrinic/flare/issues/198)). They are now `KILLED`, with no error
status and why in `spark.task.kill_reason`: `another_attempt_succeeded`, `stage_finished`,
`cancelled` or `other`. Under speculation, the original is usually `stage_finished`: the copy that
wins finishes the stage, and Spark stops the original with it.

- A query or alert on `task_result="FAILED"` now counts real failures only, and will read lower on
  runs with speculation or cancelled jobs.
- One that sums every `task_result` value, such as a task count, is unchanged. One that lists the
  values explicitly needs `KILLED` added.

### `FLARE_ENABLED=false` keeps the agent quiet

Up to 1.3, turning Flare off also dropped the defaults it gives the agent, so the agent's full
instrumentation came back, with a span per S3 or GCS request and the platform's HTTP traces
([#206](https://github.com/Neutrinic/flare/issues/206)). Turning Flare off to cut overhead produced
more telemetry than leaving it on. Now the agent's own instrumentation stays off and non-Spark root
spans are still dropped. To have the agent's full instrumentation back with Flare off, set
`otel.instrumentation.common.default-enabled=true` and `FLARE_DROP_NON_SPARK_ROOTS=false`.

### Code reading the stage context should read the stage's own key

When a job ran two stages at once, such as an RDD `join`, one stage's tasks ran under the other
stage's span ([#204](https://github.com/Neutrinic/flare/issues/204)). Flare now also writes each
stage's context under its own key, `flare.stage.<stage id>.traceparent`, and its own spans use it.
The plain `traceparent` is still written, but holds whichever stage of the job was submitted last.
Code that reads it from the task's local properties, such as the
[PySpark example](getting-started/python.md#adding-your-own-spans-from-python), should read the
stage's key first and fall back to `traceparent`.

## 1.2 to 1.3

### One Flare JAR instead of four

The published JAR now bundles the OpenTelemetry API. Up to 1.2.0 the install needed
`opentelemetry-api`, `opentelemetry-context` and `opentelemetry-common` on
`spark.{driver,executor}.extraClassPath` next to the Flare JAR; without them the driver failed at
`SparkContext` start-up with `NoClassDefFoundError: io/opentelemetry/context/ImplicitContextKeyed`.

**Remove the three OpenTelemetry JARs from `extraClassPath` and from your node set-up.** The install
is now the agent plus one Flare JAR on every platform.

If you resolve Flare through Maven or sbt, its POM no longer lists the OpenTelemetry API, because
the API is inside the JAR. Flare belongs on the cluster, not inside your application JAR: if your
build shades Flare into a fat JAR that also contains your own `opentelemetry-api`, the fat JAR ends
up with two copies of the same classes.

### Java 8 and 11 work

Up to 1.2.0, Flare's agent-side classes were compiled for Java 17. On Java 8 or 11 the agent could
not load them, and that disabled the agent's whole SDK: **nothing was exported from that JVM**,
while the Spark job itself succeeded. This affected every Spark 3.x artifact, including Databricks
Runtime 14 and 15 (Java 8 by default) and Dataproc 2.x (Java 11). If you run 1.2.0 or earlier on
those and see no telemetry at all, this is why.

### Traces that are not Spark's are dropped

Flare now drops spans that have no parent and are not Flare's own, on the driver and executors: the
platform's HTTP calls, the Kubernetes API client, executors fetching the application JAR. See
[Noise](configuration/noise.md). If you disabled instrumentations to get the same effect, such as
`-Dotel.instrumentation.okhttp.enabled=false` on Kubernetes or
`-Dotel.instrumentation.http-url-connection.enabled=false` on Dataproc, you can remove those flags.

To keep the previous behaviour, set `FLARE_DROP_NON_SPARK_ROOTS=false`.

### Nothing is lost when the cluster is torn down

When a cluster manager stops the driver with SIGTERM, as a Databricks job cluster does after every
run, the root `spark.application` span used to be lost, and executor task metrics from the run's
last minute with it. Both now arrive.

### The agent's own instrumentation is off

Flare now turns off the OpenTelemetry agent's own instrumentation by default
([#145](https://github.com/Neutrinic/flare/issues/145)). In a Spark JVM it mostly traced Spark
reading its own input, one span per S3 or GCS request, and outnumbered Flare's spans.

- **Spans that disappear:** HTTP clients and servers, the AWS and Google Cloud SDKs, JDBC, Kafka
  and the agent's other library spans, including any nested under task spans.
- **What stays on:** Flare, the OpenTelemetry API bridge, your own `@WithSpan` methods, context
  across thread pools, Log4j capture for log export, and JVM metrics. See
  [Agent instrumentation](configuration/volume.md#agent-instrumentation).
- **To keep a library's spans,** turn its instrumentation back on by name, such as
  `-Dotel.instrumentation.jdbc.enabled=true`. To restore everything,
  `-Dotel.instrumentation.common.default-enabled=true`. Anything you set yourself for the same
  setting takes precedence over Flare's default.
- **Logs from Logback or `java.util.logging`** are no longer captured by default; Spark's own logs
  go through Log4j and still are. See [Logs](configuration/exporting.md#logs).
- **Flags you added to silence the agent,** such as
  `-Dotel.instrumentation.http-url-connection.enabled=false`, are no longer needed.

### Metrics no longer carry `stage.id`

The task metrics (`flare.task.*`) and stage metrics (`flare.stage.*`) have lost their `stage.id`
label ([#136](https://github.com/Neutrinic/flare/issues/136)). It made a new series for every
stage, which grew each export for as long as the application ran and, past 2,000 stages, pushed
task metrics into the SDK's overflow series.

- **Queries grouping by `stage_id` now see one series** where they saw many. Group stage metrics by
  `stage_name` and `sql_description` instead, which are call sites and stable across runs. Task
  metrics have no stage label; group them by `executor_id`.
- **Per-stage figures** are on the `spark.stage` spans, which carry the same metrics as attributes.
- **The shipped dashboard** is updated. If you copied an earlier version, re-import it or apply
  the same change to the Task Duration, Shuffle and Stage Metrics panels.

### `FLARE_SAMPLING_RATIO` is removed

It never had an effect ([#130](https://github.com/Neutrinic/flare/issues/130)): every application
was traced whatever its value. It is now ignored with a warning, and no longer fails start-up on a
value it used to reject. Use the agent's sampler instead; see
[Sampling](configuration/index.md#sampling).
