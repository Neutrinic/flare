# Dataproc Serverless

Verified on Serverless for Apache Spark runtime 2.3 (2.3.41) with a driver and two executors,
exporting to Grafana Cloud over OTLP through Cloud NAT.

Like EMR Serverless there are no init scripts, so a **custom container image** is the way in.
Google's rules for that image differ from EMR's.

## Image

```dockerfile
FROM debian:12-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends procps tini \
 && rm -rf /var/lib/apt/lists/*
RUN groupadd -g 1099 spark && useradd -u 1099 -g 1099 -d /home/spark -m spark
COPY opentelemetry-javaagent.jar flare-spark.jar /opt/flare/
RUN chmod 755 /opt/flare && chmod 644 /opt/flare/*.jar
ENV SPARK_EXTRA_CLASSPATH=/opt/flare/flare-spark.jar
USER spark
```

`opentelemetry-javaagent.jar` and `flare-spark.jar` are the two JARs from [Downloading the JARs](../getting-started/download.md), saved next to the Dockerfile under those names.

- Do **not** put Spark or a JDK in the image. The service mounts both at runtime.
- The container runs as `spark`, UID and GID 1099, and needs `procps` and `tini`.
- `SPARK_EXTRA_CLASSPATH` is the documented way to add JARs to the driver and executor classpath in
  a custom container.
- **Runtime 2.3 is Scala 2.13**, unlike Dataproc on Compute Engine 2.3. Use
  `flare-spark-3-5_2.13`. A 2.12 JAR fails with `NoClassDefFoundError: scala/Serializable`.

Push the image to Artifact Registry in the same region as the batch, and grant the batch's service
account `roles/artifactregistry.reader` on the repository.

## Batch

```bash
gcloud dataproc batches submit spark --region <region> --version 2.3 \
  --container-image <region>-docker.pkg.dev/<project>/<repo>/<image>:<tag> \
  --class com.example.Main --jars gs://<bucket>/app.jar \
  --properties "^#^spark.plugins=io.flare.spark.plugin.FlareSparkPlugin#spark.driver.extraJavaOptions=-javaagent:/opt/flare/opentelemetry-javaagent.jar -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar -Dotel.service.name=my-app-driver#spark.executor.extraJavaOptions=-javaagent:/opt/flare/opentelemetry-javaagent.jar -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar -Dotel.service.name=my-app-executor"
```

`^#^` makes `#` the property separator, since the JVM options contain spaces.

## Networking and credentials

- Batch workers have internal IPs only. Reaching an external collector needs **Cloud NAT** on the
  subnet (what was verified) or a collector inside the VPC.
- Exporter headers can be passed as `spark.dataproc.driverEnv.OTEL_EXPORTER_OTLP_HEADERS` and
  `spark.executorEnv.OTEL_EXPORTER_OTLP_HEADERS`. They are stored in the batch's configuration and
  visible to anyone who can view the batch. Prefer a collector in the VPC that holds the credential,
  or delete the batch after it runs.

## Checking the trace

Executor stdout and stderr are not forwarded to Cloud Logging, and no batch property changes that,
so the `logging-otlp` exporter shows driver spans only. The driver's output file in GCS also dropped
part of a long line. Export over OTLP to see the real trace.
