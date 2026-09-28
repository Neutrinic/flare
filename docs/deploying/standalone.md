# Standalone

Verified on a three-node Spark standalone cluster: Spark 3.5.9 and 4.0.4, Java 17 and 21, client
deploy mode.

## Put the JARs on every node

Standalone executors fetch `--files` and `--jars` after their JVM has started, too late for the
agent. Copy both JARs onto every node at the same path instead, for example with your configuration
management or a one-off loop:

```bash
for host in node1 node2 node3; do
  ssh "$host" 'mkdir -p /opt/flare \
    && curl -fsSL -o /opt/flare/opentelemetry-javaagent.jar <agent-url> \
    && curl -fsSL -o /opt/flare/flare-spark.jar <flare-url>'
done
```

## Submit

Use the settings from the [Quick start](../getting-started/quick-start.md#submit) unchanged, with
absolute `/opt/flare/...` paths. Standalone sets no `extraClassPath` or `extraJavaOptions` of its
own, so there is nothing to append to.
