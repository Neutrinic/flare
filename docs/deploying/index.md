# Deploying

Every platform needs the same three things. Only how you achieve them differs.

1. **Both JARs at a fixed path on every machine, before the JVM starts.** The agent loads at JVM
   start-up, so anything Spark fetches after that (`--jars`, `--packages`, `spark.jars`) is too late
   for it.
2. **The Flare JAR on Spark's classpath**, on the driver and every executor, for `spark.plugins`.
3. **The JVM options** `-javaagent` and `-Dotel.javaagent.extensions` on the driver and executors,
   plus `spark.plugins`.

| Platform | How the JARs get there | Verified |
|---|---|---|
| [Standalone](standalone.md) | Copy onto every node | Spark 3.5, 4.0; Java 17, 21 |
| [YARN](yarn.md) | `--files` from a URL; nothing on the nodes | Spark 3.5, 4.0; Java 17, 21 |
| [Kubernetes](kubernetes.md) | In the image, or a volume | Spark 3.5, 4.0; `spark-submit` and Spark Operator |
| [Amazon EMR](emr.md) | `--files` (cluster mode) or a bootstrap action (client mode) | EMR 7.14 |
| [EMR Serverless](emr-serverless.md) | Custom image | EMR 7.14 |
| [Google Dataproc](dataproc.md) | Initialization action, JARs from GCS | Images 2.3 and 3.0 |
| [Dataproc Serverless](dataproc-serverless.md) | Custom container image | Runtime 2.3 |
| [Databricks](databricks.md) | Cluster init script | AWS and Azure; DBR 15.4, 17.3; Photon; Dedicated and Standard access |
| [HDInsight](hdinsight.md) | Script action | Not run |
| [Serverless platforms](unsupported.md) | Not possible | |

"Verified" means the platform ran a real job with Flare attached, and the whole trace was checked:
the root span present, every task span under its own stage across hosts, no orphans, and task
metrics matching the task spans.

## Order of work on a new platform

1. Find where the platform lets you put files on a node before Spark starts: an init script, a
   bootstrap or initialization action, an image, a volume.
2. Find out whether the platform already sets `extraClassPath` or `extraJavaOptions`. If it does,
   **append** to them. Replacing EMR's `extraClassPath`, for example, breaks `s3://` access.
3. Decide how the exporter credentials reach the JVM without appearing in Spark config. See
   [Credentials](../configuration/exporting.md#credentials).
4. Run one job and check the trace as described in the [Quick start](../getting-started/quick-start.md#check-it-worked).
