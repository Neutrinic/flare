package io.flare.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Multi-job pipeline — demonstrates trace continuity across multiple Spark actions.
 *
 * Three chained jobs all share a single trace ID:
 *   Job 0: Ingest + validate
 *   Job 1: Transform + enrich
 *   Job 2: Aggregate + write
 *
 * In Grafana the entire pipeline appears as one trace, making it trivial to see
 * the cumulative latency and which job/stage/executor is the bottleneck.
 *
 * Without executor-side spans, Job 2's aggregate would show a flat stage duration.
 * With Flare, you see individual partition processing times on each executor —
 * useful when one executor is slower due to GC pressure or node contention.
 */
object PipelineJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flare Example — Multi-Job Pipeline")
      .getOrCreate()

    println("=== Flare Pipeline Job ===")
    println("Three-stage pipeline — all jobs share one trace in Grafana")

    // ── Job 0: Ingest ─────────────────────────────────────────────────────────
    // Uses pure DataFrame API for Spark 3.5 / 4.0 binary compatibility.
    println("\n--- Job 0: Ingest and validate ---")
    val raw = spark.range(500000).select(
      (col("id") % 1000).cast("int").alias("customer_id"),
      (col("id") % 500 + 1).cast("double").alias("amount"),
      when(col("id") % 3 === 0, lit("APAC"))
        .when(col("id") % 3 === 1, lit("EMEA"))
        .otherwise(lit("AMER")).alias("region"),
      (col("id") % 10 =!= 0).alias("valid"),
    )

    val validated = raw.filter(col("valid")).cache()
    val ingestCount = validated.count()
    println(s"Ingested $ingestCount valid records (dropped ${500000 - ingestCount})")

    // ── Job 1: Transform ──────────────────────────────────────────────────────
    println("\n--- Job 1: Transform and enrich ---")
    val enriched = validated
      .withColumn("tier",
        when(col("amount") > 400, "platinum")
          .when(col("amount") > 200, "gold")
          .otherwise("standard"))
      .withColumn("adjusted_amount", col("amount") * 1.1)
      .cache()

    val tierCounts = enriched.groupBy("tier").count()
    println("Tier distribution:")
    tierCounts.show()

    // ── Job 2: Aggregate ──────────────────────────────────────────────────────
    println("\n--- Job 2: Regional aggregation ---")
    val regional = enriched
      .groupBy("region", "tier")
      .agg(
        count("*").alias("customers"),
        sum("adjusted_amount").alias("revenue"),
        avg("adjusted_amount").alias("avg_order"),
        max("adjusted_amount").alias("max_order"),
      )
      .orderBy(desc("revenue"))

    println("Regional summary:")
    regional.show()

    println("\n=== Pipeline complete. All 3 jobs visible as one trace in Grafana ===")
    println("URL: http://localhost:3000")
    println("Service: flare-driver | Search: 'Flare Example — Multi-Job Pipeline'")

    spark.stop()
  }
}
