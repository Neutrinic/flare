package io.flare.examples

import org.apache.spark.sql.SparkSession

/**
 * Deliberate task failure — the exercise for Flare's failure path.
 *
 * Every other example job succeeds, so the error attributes and the `exception` span event were
 * never actually produced in the dev stack. This job throws on exactly one partition, so a single
 * task fails while the rest of the stage completes normally.
 *
 * What to look for in Grafana:
 *   - the `spark.task.executor` span for the failing partition is red, with
 *     `error.type = java.lang.ArithmeticException` and the message on `error.message`
 *   - that span has an `exception` event carrying `exception.stacktrace` — the whole point is
 *     that a non-zero error rate no longer means grepping executor logs
 *   - the enclosing `spark.stage.N` and `spark.job.N` spans are also red. The stage's
 *     `error.type` is recovered from Spark's formatted failure string; the job's comes from the
 *     live Throwable, so only the job span carries a full driver-side stack trace
 *
 * Spark retries a failed task before giving up on the stage, so expect several failed task spans
 * for the same partition. That is real Spark behaviour, not double reporting.
 *
 * Unlike the other examples this one uses `Dataset.map` rather than staying on the DataFrame API.
 * That is deliberate: a column expression like `id / divisor` yields null on divide-by-zero
 * instead of throwing unless ANSI mode is on, and under ANSI the class is
 * `org.apache.spark.SparkArithmeticException`, which varies across the support matrix. A plain
 * Scala closure throws `java.lang.ArithmeticException` on every version, which is what makes this
 * a stable demonstration of `error.type`.
 *
 * The job exits non-zero. That is the point; do not "fix" it.
 */
object FailingJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flare Example — Deliberate Failure")
      .getOrCreate()

    import spark.implicits._

    println("=== Flare Failing Job ===")
    println("One partition divides by zero. This job is EXPECTED to fail.")

    // divisor is 0 only for ids below 1000, which all land on the first of 8 partitions.
    // Computed per row so the compiler cannot fold it into a constant.
    val doomed = spark.range(0, 100000, 1, numPartitions = 8).map { id =>
      val divisor = if (id < 1000) 0L else 7L
      id / divisor // java.lang.ArithmeticException: / by zero, on the executor thread
    }

    try {
      println("\n--- Running (expect ArithmeticException on partition 0) ---")
      println(s"count = ${doomed.count()}")
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
