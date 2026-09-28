# Quick start

Flare needs two JARs on every machine that runs a Spark JVM, driver and executors:

- the **OpenTelemetry Java agent**, `opentelemetry-javaagent.jar`, unmodified, from the
  OpenTelemetry project;
- the **Flare JAR** for your Spark and Scala version, see [Choosing a JAR](choosing-a-jar.md).

Both must be at the **same path on every machine**, and they must be there **before the JVM
starts**, because the agent loads at JVM start-up (`premain`). That rules out anything Spark fetches
after start-up, such as `--jars` or `--packages`. How to get them there is the platform-specific
part; [Deploying](../deploying/index.md) has a recipe for each platform.

## Download

```bash
mkdir -p /opt/flare
curl -fsSL -o /opt/flare/opentelemetry-javaagent.jar \
  https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/2.30.0/opentelemetry-javaagent-2.30.0.jar
curl -fsSL -o /opt/flare/flare-spark.jar \
  https://repo1.maven.org/maven2/io/github/neutrinic/flare-spark-3-5_2.12/<version>/flare-spark-3-5_2.12-<version>.jar
```

Replace `3-5_2.12` with the coordinate for your cluster and `<version>` with the
[latest release](https://github.com/Neutrinic/flare/releases/latest). Every release also attaches
each Flare JAR as a download.

## Submit

```bash
spark-submit \
  --conf "spark.plugins=io.flare.spark.plugin.FlareSparkPlugin" \
  --conf "spark.driver.extraClassPath=/opt/flare/flare-spark.jar" \
  --conf "spark.executor.extraClassPath=/opt/flare/flare-spark.jar" \
  --conf "spark.driver.extraJavaOptions=\
    -javaagent:/opt/flare/opentelemetry-javaagent.jar \
    -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar \
    -Dotel.service.name=my-app-driver \
    -Dotel.exporter.otlp.endpoint=http://your-collector:4317 \
    -Dotel.exporter.otlp.protocol=grpc \
    -Dotel.exporter.otlp.compression=gzip" \
  --conf "spark.executor.extraJavaOptions=\
    -javaagent:/opt/flare/opentelemetry-javaagent.jar \
    -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar \
    -Dotel.service.name=my-app-executor \
    -Dotel.exporter.otlp.endpoint=http://your-collector:4317 \
    -Dotel.exporter.otlp.protocol=grpc \
    -Dotel.exporter.otlp.compression=gzip" \
  my-app.jar
```

Each setting has a job:

| Setting | What it does |
|---|---|
| `-javaagent:…opentelemetry-javaagent.jar` | Starts the OpenTelemetry agent in the JVM |
| `-Dotel.javaagent.extensions=…flare-spark.jar` | Loads Flare into the agent. On the driver this is what gives every stage its own trace context |
| `spark.plugins=io.flare.spark.plugin.FlareSparkPlugin` | Loads Flare into Spark: the listener that creates driver spans, and the executor plugin that creates task spans |
| `spark.{driver,executor}.extraClassPath` | Puts the Flare JAR where `spark.plugins` can find it |
| `-Dotel.service.name` | How the driver and executors are named in your backend |
| `-Dotel.exporter.otlp.*` | Where the telemetry goes. See [Exporting](../configuration/exporting.md) |

Some platforms already put a directory on Spark's classpath, such as `/databricks/jars` on
Databricks or `/usr/lib/spark/jars` in an EMR Serverless image. Copying the Flare JAR there
replaces the two `extraClassPath` settings. The platform pages say where.

!!! tip "Compression"
    `otel.exporter.otlp.compression` defaults to `none`. Spark telemetry is unusually repetitive:
    every export repeats the resource block, which on Spark carries the whole command line and
    classpath. On the local stack, `gzip` cut the bytes sent by 56% with identical traces.

## Check it worked

In your backend, search for the service `my-app-driver`. There should be one trace whose root span
is `spark.application`, with `spark.task.executor` spans from `my-app-executor` under their stages.

If the trace is missing or looks wrong, see [Troubleshooting](../troubleshooting.md).

## If the extension can only be attached on the driver

The extension matters most on the driver, where the scheduler runs: that is what gives every stage
its own trace context. If you cannot pass `-Dotel.javaagent.extensions` to the executors, attach it
on the driver only and drop it from the executor `extraJavaOptions`. Task spans still parent to
their stage.

Everything else is unchanged on the executors: the agent (`-javaagent`), the Flare JAR on the
classpath and `spark.plugins`, because the executor plugin is what creates task spans at all.

What you lose on the executors: the `flare.role` resource attribute, and in-task context
restoration, so calls made inside a task (JDBC, HTTP) are not linked into the trace.
