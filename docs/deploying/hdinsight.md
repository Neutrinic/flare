# HDInsight

!!! warning "Not run"
    This recipe has not been run on HDInsight. The smallest HDInsight Spark cluster (two head nodes,
    three ZooKeeper nodes and workers) needs about 20 vCPUs, more than a new subscription's quota.

HDInsight 5.1 is Spark 3.3 on YARN with Java 8, so it follows the [EMR](emr.md) recipe:

- a **script action** in place of EMR's bootstrap action puts both JARs at a fixed path on every
  node, such as `/opt/flare`;
- `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions` go in the cluster's Spark
  configuration;
- use `flare-spark-3-3_2.12`, and Flare 1.3.0 or later, which runs on Java 8;
- **append** to the cluster's existing `spark.{driver,executor}.extraClassPath` rather than replacing
  it, as on EMR.

If you run this, the result would be welcome in
[#91](https://github.com/Neutrinic/flare/issues/91).
