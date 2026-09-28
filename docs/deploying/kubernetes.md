# Kubernetes

Verified on a three-node k3s cluster: Spark 3.5.9 and 4.0.4, Java 17 and 21, `spark-submit` in
cluster mode; and the Kubeflow Spark Operator 2.5.2 with Spark 4.0.4. Driver and executors both run
in pods, and `flare.role` is detected correctly on every executor.

## Getting the JARs into the pods

The JARs must be in the driver and executor containers when they start. Either:

- **bake them into your Spark image**, which is the simplest and needs nothing at submit time:

    ```dockerfile
    FROM <your spark image>
    COPY opentelemetry-javaagent.jar flare-spark.jar /opt/flare/
    ```

- or **mount them from a volume**. Spark mounts volumes declared in its config itself:

    ```properties
    spark.kubernetes.driver.volumes.hostPath.flare.options.path=/srv/flare
    spark.kubernetes.driver.volumes.hostPath.flare.mount.path=/opt/flare
    spark.kubernetes.executor.volumes.hostPath.flare.options.path=/srv/flare
    spark.kubernetes.executor.volumes.hostPath.flare.mount.path=/opt/flare
    ```

    Any volume type Spark supports works the same way; `hostPath` is what was run.

`--jars` and `spark.jars` fetch too late for `-javaagent`.

## spark-submit

The [Quick start](../getting-started/quick-start.md#submit) settings, with `/opt/flare/...` paths,
alongside your usual `--master k8s://...` settings.

## Spark Operator

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: my-app
spec:
  type: Scala
  mode: cluster
  image: <your spark image>
  mainClass: com.example.Main
  mainApplicationFile: local:///opt/app/app.jar
  sparkConf:
    spark.plugins: io.flare.spark.plugin.FlareSparkPlugin
    spark.driver.extraClassPath: /opt/flare/flare-spark.jar
    spark.executor.extraClassPath: /opt/flare/flare-spark.jar
  driver:
    serviceAccount: spark
    javaOptions: >-
      -javaagent:/opt/flare/opentelemetry-javaagent.jar
      -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar
      -Dotel.exporter.otlp.endpoint=http://otel-collector:4317
      -Dotel.service.name=my-app-driver
  executor:
    instances: 2
    javaOptions: >-
      -javaagent:/opt/flare/opentelemetry-javaagent.jar
      -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar
      -Dotel.exporter.otlp.endpoint=http://otel-collector:4317
      -Dotel.service.name=my-app-executor
```

- `driver.javaOptions` and `executor.javaOptions` become `spark.driver.extraJavaOptions` and
  `spark.executor.extraJavaOptions`; setting those in `sparkConf` works the same.
- If the JARs come from a volume, declare it in `sparkConf` as above. `spark-submit` then mounts it
  itself and the operator's mutating webhook is not needed. With the webhook enabled,
  `driver.volumeMounts` and `executor.volumeMounts` work too.

## Spark's own API calls

The agent also traces the driver's calls to the Kubernetes API server: pod creation, polling and
cleanup. Flare drops these by default, see [Noise](../configuration/noise.md). Before 1.3.0 each was
a separate trace, about ten per run.
