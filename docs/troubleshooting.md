# Troubleshooting

Start by adding `-Dotel.traces.exporter=otlp,logging-otlp` to the driver and executor options. Every
export is then also written to standard error as OTLP JSON, which separates "Flare did not produce
it" from "the backend did not receive it".

## Nothing is exported at all

- **Java 8 or 11 with Flare 1.2.0 or earlier.** The agent could not load Flare's classes and
  disabled its SDK. The driver log shows `UnsupportedClassVersionError` for
  `io.flare.spark.config.FlareAutoConfig`. Upgrade; see [Upgrading](upgrading.md#java-8-and-11-work).
- **The agent is not attached.** Check the JVM's start-up log for
  `opentelemetry-javaagent - version:`. If it is missing, the `-javaagent` option did not reach
  that JVM, or the path was wrong on that machine.
- **The exporter cannot reach the backend.** The agent logs `Failed to export spans`. Check the
  endpoint, the protocol (`grpc` on 4317, `http/protobuf` on 4318) and, on private networks,
  outbound access.

## The driver fails at start-up

- **`NoClassDefFoundError: io/opentelemetry/context/ImplicitContextKeyed`**: Flare 1.2.0 or earlier
  without the three OpenTelemetry API JARs on the classpath. Upgrade to 1.3.0, which bundles them.
- **`ClassNotFoundException: io.flare.spark.plugin.FlareSparkPlugin`**: the Flare JAR is not on
  Spark's classpath. Check `extraClassPath`, or the platform's own JAR directory.
- **`NoClassDefFoundError: scala/Serializable`**: a Scala 2.12 JAR on a Scala 2.13 runtime. See
  [Choosing a JAR](getting-started/choosing-a-jar.md).
- **S3 access broke after adding Flare on EMR**: `extraClassPath` was replaced instead of appended
  to. See [EMR](deploying/emr.md#append-to-the-classpath).

## Task spans hang directly under `spark.application`

Every task should sit under its own `spark.stage.N`. If they are flattened onto the root, the
driver's scheduler hook did not run. Either `-Dotel.javaagent.extensions` is missing from the
driver's `extraJavaOptions` or its path is wrong there, or the Flare JAR is not on the driver's
classpath, where the hook loads its helper classes from. The driver log shows
`[Flare] DAGScheduler reflection initialized successfully` when it works.

## No task spans

- `FLARE_TRACE_GRANULARITY` must be `tasks` or `all`; the default `stages` stops at stage spans.
- The executors need `spark.plugins` and the Flare JAR on their classpath too. Look for
  `[Flare] Executor plugin initialized` in an executor log.

## The root span is missing, and SQL or job spans are orphaned

The `spark.application` span ends when the application stops. On Flare 1.2.0 and earlier it was
lost when the cluster manager stopped the driver with SIGTERM, which a Databricks job cluster does
after every run. Upgrade to 1.3.0.

On a long-lived cluster, such as a Databricks all-purpose cluster or a Thrift server, the root only
ends when the cluster stops, so it is not there yet while the cluster is running.

## Executors show no `flare.role`

`flare.role` comes from the agent extension. It is missing on any JVM where
`-Dotel.javaagent.extensions` was not set, which is expected with the
[driver-only set-up](getting-started/quick-start.md#if-the-extension-can-only-be-attached-on-the-driver).

## Many small traces next to Flare's

Upgrade to 1.3.0, which drops them by default; see [Noise](configuration/noise.md).

## Checking logs on managed platforms

Some platforms make logs unreliable for checking spans: Databricks cluster log delivery to a volume
delivered nothing in one workspace, and Dataproc Serverless does not forward executor output at all.
On those, export over OTLP and check in the backend.
