# YARN

Verified on a three-node YARN cluster: Spark 3.5.9 and 4.0.4, Java 17 and 21, cluster deploy mode.
The driver landed on a different node on each run.

YARN is the one cluster manager where Flare needs **no node preparation at all**. YARN downloads
`--files` into each container's working directory before it starts the JVM, so the agent is in
place at start-up, for the driver (the ApplicationMaster, in cluster mode) and for every executor.

## Submit

Pass both JARs as URLs in `--files`, and refer to them by relative path. Each file is named after
the last segment of its URL, which `${AGENT##*/}` and `${FLARE##*/}` give:

```bash
--8<-- "urls.sh"

spark-submit --master yarn --deploy-mode cluster \
  --files "$AGENT,$FLARE" \
  --conf "spark.plugins=io.flare.spark.plugin.FlareSparkPlugin" \
  --conf "spark.driver.extraClassPath=./${FLARE##*/}" \
  --conf "spark.executor.extraClassPath=./${FLARE##*/}" \
  --conf "spark.driver.extraJavaOptions=-javaagent:./${AGENT##*/} \
    -Dotel.javaagent.extensions=./${FLARE##*/} -Dotel.service.name=my-app-driver \
    -Dotel.exporter.otlp.endpoint=http://your-collector:4318" \
  --conf "spark.executor.extraJavaOptions=-javaagent:./${AGENT##*/} \
    -Dotel.javaagent.extensions=./${FLARE##*/} -Dotel.service.name=my-app-executor \
    -Dotel.exporter.otlp.endpoint=http://your-collector:4318" \
  my-app.jar
```

Replace `3-5_2.12` with the coordinate for your cluster. An internal mirror works the same way; see
[Downloading the JARs](../getting-started/download.md).

The file name in the container is the last segment of the URL. If your URL ends in a versioned
name such as `flare-spark-3-5_2.12-1.3.0.jar`, use that name in the relative paths. `--files` also
accepts `hdfs://` and `s3://` URLs; only HTTPS was run here.

## Client mode

In client mode the driver runs where you call `spark-submit`, outside YARN, so `--files` never
reaches it. Put both JARs on that machine at a fixed path and use absolute paths. The verified
client-mode recipe is EMR's, where a bootstrap action puts the JARs on every node: see
[EMR client mode](emr.md#client-mode).

## Check the existing classpath

Hadoop distributions often set `spark.{driver,executor}.extraClassPath` in `spark-defaults.conf`.
If yours does, append Flare's JAR to the existing value rather than replacing it. See
[EMR](emr.md#append-to-the-classpath) for a worked example.
