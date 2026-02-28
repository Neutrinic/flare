package io.flare.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Simple batch job — baseline example showing the complete Flare span hierarchy.
 *
 * In Grafana you should see:
 *   spark.application
 *   └── spark.job.0
 *       ├── spark.stage.0  (generate + cache)
 *       └── spark.stage.1  (aggregate)
 *           ├── spark.task.executor (partition 0 — executor-1)
 *           ├── spark.task.executor (partition 1 — executor-1)
 *           ├── spark.task.executor (partition 2 — executor-2)
 *           └── spark.task.executor (partition 3 — executor-2)
 *
 * The task spans are created on the executor JVM with the driver trace as parent.
 */
object SimpleJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flare Example — Simple Batch")
      .getOrCreate()

    println("=== Flare Simple Job ===")
    println("Generating 100k records across 4 partitions")

    val data = spark.range(100000)
      .select(
        col("id"),
        (rand() * 100).cast("int").alias("value"),
        when(col("id") % 3 === 0, "A")
          .when(col("id") % 3 === 1, "B")
          .otherwise("C").alias("category"),
      )
      .repartition(4)
      .cache()

    val total = data.count()
    println(s"Total records: $total")

    val aggregated = data
      .groupBy("category")
      .agg(
        count("*").alias("count"),
        avg("value").alias("avg_value"),
        max("value").alias("max_value"),
      )

    println("\n--- Aggregation results ---")
    aggregated.show()

    println("\n=== Job complete. Open Grafana at http://localhost:3000 ===")

    spark.stop()
  }
}
