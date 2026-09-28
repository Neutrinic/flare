# Upgrading

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

### `FLARE_SAMPLING_RATIO`

It never had an effect ([#130](https://github.com/Neutrinic/flare/issues/130)): every application
was traced whatever its value. Use the agent's sampler instead; see
[Sampling](configuration/index.md#sampling).
