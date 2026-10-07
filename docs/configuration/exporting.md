# Exporting

Flare exports through the OpenTelemetry agent, so exporting is configured with the agent's standard
`otel.*` settings. The full list is in the
[agent configuration docs](https://opentelemetry.io/docs/zero-code/java/agent/configuration/); this
page covers what matters for Spark.

## Endpoint and protocol

```text
-Dotel.exporter.otlp.endpoint=http://your-collector:4317
-Dotel.exporter.otlp.protocol=grpc
-Dotel.exporter.otlp.compression=gzip
```

The agent's default protocol is `http/protobuf` on port 4318. Use `grpc` with port 4317 if your
collector expects gRPC.

Flare works with any OTLP backend, not only Grafana:

- **Backends that take OTLP directly** with a key in a header, such as the OTLP endpoints Datadog
  and Dynatrace document, are set up like [Grafana Cloud](#grafana-cloud): their endpoint, and the
  header in the agent's configuration file.
- **Backends that need signed requests,** such as the cloud providers' own observability services,
  take OTLP through a collector that signs for them: point `otel.exporter.otlp.endpoint` at it.

Each vendor documents its endpoint, authentication and collector; that setup is not covered here.

`http://` endpoints are unencrypted. Use them only for a collector on the same node or a private
network, and `https://` for anything else: the telemetry carries SQL plans, failure messages and,
with log export, your logs.

Turn on `gzip`. Every export repeats the resource block, and on Spark that includes the whole
command line and classpath. On the local stack, three matched runs each way sent 244,660 bytes
uncompressed against 106,480 with gzip, 56% less, with identical traces.

### Grafana Cloud

Grafana Cloud's OTLP gateway takes `http/protobuf`, not gRPC:

```properties
otel.exporter.otlp.endpoint=https://otlp-gateway-<region>.grafana.net/otlp
otel.exporter.otlp.protocol=http/protobuf
otel.exporter.otlp.headers=Authorization=Basic%20<base64 of instanceId:token>
```

## Credentials

**Do not put exporter credentials in Spark config.** `-Dotel.exporter.otlp.headers=…`, `--conf`,
`spark.executorEnv.*` and environment variables all end up somewhere readable: the Spark UI's
Environment page, the Spark event log, or YARN's container launch scripts, which some platforms ship
to object storage. Spark's default `spark.redaction.regex` does not match
`OTEL_EXPORTER_OTLP_HEADERS`.

Put them in an **agent configuration file** instead, written onto each node by the same step that
installs the JARs, and pass only its path:

```text
-Dotel.javaagent.configuration-file=/opt/flare/otel.properties
```

```properties
otel.exporter.otlp.endpoint=https://otlp-gateway-<region>.grafana.net/otlp
otel.exporter.otlp.protocol=http/protobuf
otel.exporter.otlp.headers=Authorization=Basic%20<...>
```

The platform pages show where the secret comes from: SSM Parameter Store on
[EMR](../deploying/emr.md#exporting-to-a-hosted-backend), a secret scope on
[Databricks](../deploying/databricks.md#credentials). Both were checked for leaks: the credential
appeared in none of the logs, event logs or command lines.

On serverless platforms with no init step, point the agent at a collector inside your network that
holds the credential.

## Logs

The agent exports logs by default (`otel.logs.exporter=otlp`). Flare's defaults keep its capture of
Log4j, which Spark logs through, so Spark's own logs go out with the trace and span id of the task
that wrote them, and a log line links to its task span. If your own code logs through Logback or
`java.util.logging`, turn their capture back on with
`-Dotel.instrumentation.logback-appender.enabled=true` or
`-Dotel.instrumentation.java-util-logging.enabled=true`. On Databricks a three-minute run exported about 1,700
lines this way.

Before leaving it on:

- **Volume.** Spark logs a line for every task's start and end at `INFO`. In the TPC-H benchmarks
  that was 7,000 to 13,000 lines a minute on small clusters, or for an application running all day
  about 0.9 GB a day on the lab and 1.5 GB on Databricks, gzipped. Other workloads differ.
  Raise Spark's log level to `WARN`, or filter in a collector, on real workloads. See
  [Telemetry volume](volume.md#logs).
- **Spark logs its whole configuration at start-up.** With log export on, anything secret in Spark
  config reaches your log backend. Another reason to keep credentials in the agent configuration
  file.
- **`flare.role` becomes a Loki stream label**, one per executor. Fine at small scale; on large
  elastic clusters it multiplies streams.

To turn logs off: `-Dotel.logs.exporter=none`.

## Metrics

Metrics are exported every 60 seconds by default (`otel.metric.export.interval`). Flare also flushes
at shutdown, and when things go quiet: each executor a second after its last task ends, and the
driver a second after its last job ends ([#199](https://github.com/Neutrinic/flare/issues/199)).
Either waits three seconds instead if it already flushed in the previous 30, so that pauses between
stages and jobs do not flush every few seconds
([#139](https://github.com/Neutrinic/flare/issues/139)). A short job, or a JVM that is later killed
without a shutdown call, still reports its final values: a Databricks job cluster starts tearing
down about 8.6 seconds after its last job. A JVM killed sooner than that, or while its tasks or jobs
are still running, can lose the metrics it had not exported yet. To narrow that window, lower
`otel.metric.export.interval`.

## Checking what is exported

To see the raw output without a backend, add the logging exporter:

```text
-Dotel.traces.exporter=otlp,logging-otlp
```

Each export is then also written to the JVM's standard error as OTLP JSON. On most platforms that
is the quickest way to check a first run.
