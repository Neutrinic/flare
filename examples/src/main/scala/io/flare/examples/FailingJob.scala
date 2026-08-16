package io.flare.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Deliberate task failure — the exercise for Flare's failure path.
 *
 * Every other example job succeeds, so the error attributes and the `exception` span event were
 * never actually produced in the dev stack. This job throws on exactly one partition, so a single
 * task fails while the rest of the stage completes normally.
 *
 * What to look for in Grafana:
 *   - the `spark.task.executor` span for the failing partition is red, with `error.type` set to
 *     the arithmetic exception class and the message on `error.message`
 *   - that span has an `exception` event carrying `exception.stacktrace` — the whole point is
 *     that a non-zero error rate no longer means grepping executor logs
 *   - the enclosing `spark.stage.N` and `spark.job.N` spans are also red. The stage's
 *     `error.type` is recovered from Spark's formatted failure string; the job's comes from the
 *     live Throwable, so only the job span carries a full driver-side stack trace
 *
 * Spark retries a failed task before giving up on the stage, so expect several failed task spans
 * for the same partition. That is real Spark behaviour, not double reporting.
 *
 * Stays on the DataFrame API like the other examples. That is not stylistic: the dev stack image
 * runs Spark 4.0 while these examples are built against 3.5, and `Dataset.map` needs
 * `SparkSession.implicits`, whose signature changed in 4.0 — it fails with `NoSuchMethodError`
 * under that skew. Integer division under ANSI mode throws inside the task with no such
 * dependency.
 *
 * ANSI is set explicitly because it is off by default before Spark 4.0. The exception class is
 * `org.apache.spark.SparkArithmeticException` on Spark 3.4+ (a subclass of
 * `java.lang.ArithmeticException`), which is what `error.type` will show.
 *
 * The job exits non-zero. That is the point; do not "fix" it.
 */
object FailingJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flare Example — Deliberate Failure")
      .getOrCreate()

    // Off by default before 4.0; without it division by zero yields null instead of throwing.
    spark.conf.set("spark.sql.ansi.enabled", "true")

    println("=== Flare Failing Job ===")
    println("One partition divides by zero. This job is EXPECTED to fail.")

    // divisor is 0 only for ids below 1000, which all land on the first of 8 partitions.
    val data = spark.range(0, 100000, 1, numPartitions = 8).select(
      col("id"),
      when(col("id") < 1000, lit(0)).otherwise(lit(7)).alias("divisor"),
    )

    // `div` is integer division, which throws under ANSI. `/` returns a double and would give
    // Infinity rather than failing.
    val doomed = data.select(expr("id div divisor").alias("ratio"))

    try {
      println("\n--- Running (expect an arithmetic exception on partition 0) ---")
      println(s"count = ${doomed.filter(col("ratio") >= 0L).count()}")
      println("\nUNEXPECTED: the job succeeded. The failure path was NOT exercised.")
      spark.stop()
      sys.exit(2)
    } catch {
      case e: Exception =>
        println(s"\n--- Failed as intended: ${e.getClass.getName} ---")
        println("\n=== Open Grafana at http://localhost:3000 ===")
        println("Find the trace for 'Flare Example — Deliberate Failure'")
        println("Observe: a red task span with error.type and an exception event carrying a stack trace")
        // Stop inside the handler so the span processor flushes before the JVM exits.
        spark.stop()
        sys.exit(1)
    }
  }
}
