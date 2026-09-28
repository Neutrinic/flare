# Choosing a JAR

Flare is published once per Spark line and Scala version. The wrong one fails at class-load time,
often with an unhelpful error, so match it to what the cluster runs.

| Spark | Scala 2.12 | Scala 2.13 |
|---|---|---|
| 3.3 | `flare-spark-3-3_2.12` | `flare-spark-3-3_2.13` |
| 3.4 | `flare-spark-3-4_2.12` | `flare-spark-3-4_2.13` |
| 3.5 | `flare-spark-3-5_2.12` | `flare-spark-3-5_2.13` |
| 4.0 and later | | `flare-spark-4-0_2.13` |

All are under the Maven group `io.github.neutrinic`, for example
`io.github.neutrinic:flare-spark-3-5_2.12:<version>`. The JAR is self-contained: it bundles the
OpenTelemetry API it needs and nothing else.

## Finding your versions

```bash
spark-submit --version
```

prints both, for example `version 3.5.1` and `Using Scala version 2.12.18`. On a managed platform,
the runtime's release notes list them. The ones that catch people out:

| Platform | Runtime | Spark | Scala | JAR |
|---|---|---|---|---|
| Databricks | DBR 14.x, 15.x | 3.5 | 2.12 | `flare-spark-3-5_2.12` |
| Databricks | DBR 16.x (Scala 2.13 variant), 17.x | 3.5 / 4.0 | 2.13 | `3-5_2.13` / `4-0_2.13` |
| EMR | 7.x | 3.5 | 2.12 | `flare-spark-3-5_2.12` |
| Dataproc on Compute Engine | image 2.3 | 3.5 | 2.12 | `flare-spark-3-5_2.12` |
| Dataproc on Compute Engine | image 3.0 | 4.1 | 2.13 | `flare-spark-4-0_2.13` |
| Dataproc Serverless | runtime 2.3 | 3.5 | **2.13** | `flare-spark-3-5_2.13` |

Dataproc Serverless 2.3 is Scala 2.13 while Dataproc on Compute Engine 2.3 is 2.12. A 2.12 JAR on a
2.13 runtime fails with `NoClassDefFoundError: scala/Serializable`.

## Spark 4.1 and later

There is no separate artifact yet. The `4-0_2.13` JAR has been run on Spark 4.1.2 (Dataproc 3.0)
with a complete trace.

## Java

Every Spark 3.x artifact runs on Java 8 and later, and the 4.0 artifact on Java 17 and later, the
same floors as Spark itself. This matters on Databricks Runtime 14 and 15, which default to Java 8,
and on Dataproc 2.x, which runs Java 11. Releases before 1.3.0 exported nothing on Java 8 or 11; see
[Upgrading](../upgrading.md).
