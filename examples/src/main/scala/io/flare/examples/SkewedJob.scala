package io.flare.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Skewed partition example — the hero demo for Flare.
 *
 * Intentionally creates severe partition skew: one partition gets ~90% of the data,
 * the rest share 10%. In a driver-side-only tool (Spot, metrics-only APMs), the stage
 * span shows a single aggregate duration — you cannot see which partition is slow.
 *
 * With Flare, each executor task span shows its actual wall-clock duration on the
 * executor thread. In Grafana you immediately see one span taking 10x longer than
 * its siblings, on a specific executor, with the partition ID as an attribute.
 *
 * This is the trace you should screenshot for the README.
 */
object SkewedJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flare Example — Skewed Partitions")
      .getOrCreate()

    println("=== Flare Skewed Job ===")
    println("Generating skewed dataset — partition 0 gets 90% of records")
    println("Watch Grafana: executor task spans will show dramatic duration skew")

    // Create 1M records where partition key 0 appears 900k times
    // and keys 1-9 share the remaining 100k records equally.
    // Uses pure DataFrame API for Spark 3.5 / 4.0 binary compatibility.
    val skewedData = spark.range(1000000).select(
      when(col("id") < 900000, lit(0))
        .otherwise(((col("id") - 900000) % 9 + 1).cast("int"))
        .alias("partition_key"),
      (col("id") * 31 + 17).cast("double").alias("value"),
    )

    println("\n--- Stage 1: Skewed shuffle (watch task span durations) ---")
    val aggregated = skewedData
      .groupBy("partition_key")
      .agg(
        count("*").alias("record_count"),
        avg("value").alias("avg_value"),
        sum("value").alias("total_value"),
      )
      .cache()

    aggregated.show()

    println("\n--- Stage 2: Post-shuffle processing (balanced, fast) ---")
    aggregated
      .filter(col("record_count") > 100)
      .orderBy(desc("record_count"))
      .show()

    println("\n=== Job complete. Open Grafana at http://localhost:3000 ===")
    println("Find the trace for 'Flare Example — Skewed Partitions'")
    println("Observe: one task span will be ~10x longer than its siblings")

    spark.stop()
  }
}
