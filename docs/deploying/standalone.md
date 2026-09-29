# Standalone

Verified on a three-node Spark standalone cluster: Spark 3.5.9 and 4.0.4, Java 17 and 21, client
deploy mode.

## Put the JARs on every node

Standalone executors fetch `--files` and `--jars` after their JVM has started, too late for the
agent. Copy both JARs onto every node at the same path instead, for example with your configuration
management or a one-off loop:

```bash
--8<-- "urls.sh"

for host in node1 node2 node3; do
  ssh "$host" "sudo mkdir -p /opt/flare \
    && sudo curl -fsSL -o /opt/flare/opentelemetry-javaagent.jar $AGENT \
    && sudo curl -fsSL -o /opt/flare/flare-spark.jar $FLARE"
done
```

Replace `3-5_2.12` with the coordinate for your cluster; see
[Downloading the JARs](../getting-started/download.md).

Writing to `/opt` needs `sudo` without a password prompt on each node. Where you do not have that,
use a directory the SSH user can write to, at the same path on every node, such as
`/home/spark/flare`, drop `sudo`, and use that path in the settings below.

## Submit

Use the settings from the [Quick start](../getting-started/quick-start.md#submit) unchanged, with
absolute `/opt/flare/...` paths. Standalone sets no `extraClassPath` or `extraJavaOptions` of its
own, so there is nothing to append to.
