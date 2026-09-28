# Databricks

Verified on classic compute, exporting to Grafana Cloud:

| Cloud | Runtime | Java | Also covered |
|---|---|---|---|
| AWS | DBR 15.4 LTS (Spark 3.5.0) | 8 | Photon on and off; Dedicated and Standard access modes |
| AWS | DBR 17.3 LTS (Spark 4.0.0) | 17 | |
| Azure | DBR 15.4 LTS | 8 | Init script from workspace files |
| Azure | DBR 17.3 LTS | 17 | |

The runtime is the same on every cloud, so the recipe is too. Only storage paths and node types
differ. Serverless compute cannot run Flare; see [below](#serverless).

## Init script

A cluster-scoped init script runs on every node before the Spark JVMs start. Store it in workspace
files or a Unity Catalog volume and add it to the cluster.

```bash
#!/bin/bash
set -euo pipefail
AGENT=https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/2.30.0/opentelemetry-javaagent-2.30.0.jar
FLARE=https://repo1.maven.org/maven2/io/github/neutrinic/flare-spark-3-5_2.12/<version>/flare-spark-3-5_2.12-<version>.jar

mkdir -p /opt/flare
wget -q -O /opt/flare/opentelemetry-javaagent.jar "$AGENT"
wget -q -O /opt/flare/flare-spark.jar "$FLARE"

# /databricks/jars is on the driver and executor classpath, which spark.plugins needs.
cp /opt/flare/flare-spark.jar /databricks/jars/flare-spark.jar
```

Use `flare-spark-4-0_2.13` on DBR 17.x. See [Choosing a JAR](../getting-started/choosing-a-jar.md).

## Cluster Spark config

```text
spark.plugins io.flare.spark.plugin.FlareSparkPlugin
spark.driver.extraJavaOptions -javaagent:/opt/flare/opentelemetry-javaagent.jar -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar -Dotel.javaagent.configuration-file=/opt/flare/otel.properties -Dotel.service.name=my-app-driver
spark.executor.extraJavaOptions -javaagent:/opt/flare/opentelemetry-javaagent.jar -Dotel.javaagent.extensions=/opt/flare/flare-spark.jar -Dotel.javaagent.configuration-file=/opt/flare/otel.properties -Dotel.service.name=my-app-executor
```

No `extraClassPath` is needed. Databricks keeps its own settings and merges yours in:

- it **prepends** its own JVM options to `extraJavaOptions` (the `--add-opens` set, temp directory,
  GC flags);
- it **appends** its own plugins to `spark.plugins`, such as `SparkConnectPlugin`.

Databricks ships no OpenTelemetry libraries, so there is no version to clash with Flare's.

## Credentials

Keep the backend token in a secret scope, out of Spark config, which Spark logs in full at start-up.

```bash
databricks secrets create-scope flare
databricks secrets put-secret flare otlp-token
```

Add a cluster environment variable `FLARE_OTLP_TOKEN={{secrets/flare/otlp-token}}`. Databricks
resolves it and redacts it in its UI and logs. The init script turns it into the agent's
configuration file:

```bash
if [ -n "${FLARE_OTLP_TOKEN:-}" ]; then
  T=$(printf '%s' "$FLARE_OTLP_TOKEN" | tr -d '\r\n')
  {
    echo "otel.exporter.otlp.endpoint=https://otlp-gateway-<region>.grafana.net/otlp"
    echo "otel.exporter.otlp.protocol=http/protobuf"
    echo "otel.exporter.otlp.headers=Authorization=Basic%20$(printf '<instance-id>:%s' "$T" | base64 -w0)"
  } > /opt/flare/otel.properties
  chmod 644 /opt/flare/otel.properties
  unset T
fi
```

Checked with log export on: no token, `glc_` string or auth header in any of 1,752 exported log lines,
and the JVM command line carries only the configuration file path.

## Access modes

| Mode | Works | Notes |
|---|---|---|
| Dedicated (single user) | Yes | |
| Standard (shared, Unity Catalog) | Yes | A metastore admin must allowlist the init script's location: **Catalog → Metastore → Allowed JARs/Init Scripts**, type *Init Script*, the volume or path as a prefix. `extraJavaOptions` and `spark.plugins` are accepted as they are |

## Job clusters and all-purpose clusters

- **Job clusters** are created for one run and torn down when it ends. The driver is stopped with
  SIGTERM, so Spark never stops the SparkContext, and executors get no shutdown call at all. Flare
  1.3.0 and later end the root span inside the agent's own shutdown and flush executors when they
  go idle, so nothing is lost. Earlier releases lost the root span on every run.
- **All-purpose clusters** keep one SparkContext for their whole life, shared by every notebook and
  job attached. Flare's root span covers the cluster's lifetime, so everything lands in one trace.
  Tracked in [#124](https://github.com/Neutrinic/flare/issues/124).

## Photon

Photon does not zero Spark's task metrics. With Photon on and off, the same scan of
`samples.tpch.lineitem` reported identical record counts (29,999,795), byte counts within a few
percent, and populated run time, CPU time and shuffle metrics. Photon's shuffle writes are smaller,
which fits its own shuffle format. The plan differs, so stage and task counts differ between the two
modes. Details in [#94](https://github.com/Neutrinic/flare/issues/94).

## Azure

- **Init script from workspace files:**
  `{"workspace": {"destination": "/Workspace/Users/<user>/flare/flare-init.sh"}}` works on a
  Dedicated cluster. A Unity Catalog volume works as on AWS.
- **Node type:** `Standard_DS3_v2` (4 vCPU) is the smallest general-purpose node. A new pay-as-you-go
  subscription gets 10 vCPUs per region.
- **Networking:** with secure cluster connectivity on (the default, "no public IP"), Azure adds a
  NAT gateway to the workspace's managed resource group, billed by the hour. Nodes need outbound
  internet to download the JARs and reach an external collector either way.

## Serverless

Serverless compute, including Free Edition, has no init scripts and no JVM options, so the agent
cannot be attached. See [Platforms that cannot run Flare](unsupported.md).

## Logs on Databricks

Cluster log delivery to a Unity Catalog volume delivered nothing in the verified workspace, with no
error, and DBFS log delivery fails the run outright when the public DBFS root is disabled, which is
the default on new workspaces. Export over OTLP rather than relying on log delivery to check
Flare's output.
