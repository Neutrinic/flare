# Google Dataproc

Verified on Dataproc on Compute Engine (now "Managed Service for Apache Spark"), one master and two
workers:

| Image | Spark | Scala | Java | JAR |
|---|---|---|---|---|
| 2.3 | 3.5.3 | 2.12 | 11 | `flare-spark-3-5_2.12` |
| 3.0 | 4.1.2 | 2.13 | 21 | `flare-spark-4-0_2.13` |

Image 2.3 runs Java 11: Flare releases before 1.3.0 export nothing there. See
[Upgrading](../upgrading.md).

## Stage the JARs in GCS

Clusters are internal-IP only by default. Nodes reach Google services through Private Google Access
but not the public internet, so an initialization action cannot download from GitHub or Maven
Central. Put both JARs in a bucket and copy them from there.

## Initialization action

The jobs API runs the driver on the master in client mode, so `--files` never reaches it. An
initialization action puts both JARs at a fixed path on every node:

```bash
#!/bin/bash
set -euo pipefail
mkdir -p /opt/flare
gcloud storage cp gs://<bucket>/opentelemetry-javaagent.jar /opt/flare/
gcloud storage cp gs://<bucket>/flare-spark.jar /opt/flare/
chmod 644 /opt/flare/*.jar
```

## Submit

There is no classpath to append to: image 2.3 sets no `extraClassPath` or `extraJavaOptions`, and
3.0 sets only `spark.executor.defaultJavaOptions`, which Spark prepends on its own.

`--properties-file` does not parse values that contain spaces and `=`. Use `--properties` with a
custom delimiter instead; `^#^` makes `#` the separator:

```bash
gcloud dataproc jobs submit spark --cluster <cluster> --region <region> \
  --class com.example.Main --jars gs://<bucket>/app.jar \
  --properties "^#^spark.plugins=io.flare.spark.plugin.FlareSparkPlugin#spark.driver.extraClassPath=/opt/flare/flare-spark.jar#spark.executor.extraClassPath=/opt/flare/flare-spark.jar#spark.driver.extraJavaOptions=-javaagent:/opt/flare/opentelemetry-javaagent.jar -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar -Dotel.service.name=my-app-driver#spark.executor.extraJavaOptions=-javaagent:/opt/flare/opentelemetry-javaagent.jar -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar -Dotel.service.name=my-app-executor"
```

## Notes

- **Service account.** Organisations that enforce `iam.automaticIamGrantsForDefaultServiceAccounts`,
  the default for newer ones, leave the default compute service account with no roles. It needs
  `roles/dataproc.worker`.
- **Dataproc's own HTTP calls.** The agent traced the driver's calls to the metadata server and the
  GCS connector's requests, 63 to 85 separate traces per job. Flare drops these by default; see
  [Noise](../configuration/noise.md).
- **Reaching an external collector** needs Cloud NAT on the subnet, or a collector inside the VPC.
