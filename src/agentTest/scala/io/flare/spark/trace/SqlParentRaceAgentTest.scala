package io.flare.spark.trace

import io.flare.spark.instrumentation.SubmitMissingTasksAdviceHelper
import org.apache.spark.TaskContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.{Encoders, SparkSession}
import munit.FunSuite

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A SQL execution's span exists before its first job runs, however late the listener bus is (#178).
 *
 * The listener creates SQL spans from the execution start event, on the asynchronous listener
 * bus; the scheduler advice parents each job under its SQL span when it schedules the job. When
 * the bus lagged, the advice found no SQL span and parented the job to the application span for
 * good, so SQL and job became siblings.
 *
 * The lag is made deterministic: a listener on the same queue blocks on the first job's start until
 * the query's task has looked for its span, holding back every later event, including the query's
 * execution start, while the scheduler goes ahead and runs the query's job. `local[1]` runs the task in this JVM, so it can look for
 * its execution's span in the advice's map while the job is running.
 */
class SqlParentRaceAgentTest extends FunSuite {

  test("a SQL job finds its execution's span even when the listener bus lags") {
    StubCollector.during {
      val spark = SparkSession.builder()
        .master("local[1]")
        .appName("flare-sql-race-probe")
        .config("spark.ui.enabled", "false")
        .config("spark.plugins", "io.flare.spark.plugin.FlareSparkPlugin")
        .getOrCreate()
      try {
        // The first query in a JVM spends seconds starting Spark SQL, long enough for the bus to
        // catch up; run one first so the measured query starts promptly.
        spark.range(1).count()

        val armed = new AtomicBoolean(true)
        val releasedByProbe = new AtomicBoolean(false)
        spark.sparkContext.addSparkListener(new SparkListener {
          override def onJobStart(event: SparkListenerJobStart): Unit =
            if (armed.compareAndSet(true, false)) releasedByProbe.set(SqlParentRaceAgentTest.probed.await(15, TimeUnit.SECONDS))
        })
        spark.sparkContext.parallelize(Seq(1), 1).count()

        val seen = spark.range(1).map { _ =>
          val id = TaskContext.get().getLocalProperty("spark.sql.execution.id")
          val hasSpan = SubmitMissingTasksAdviceHelper.activeSQLSpans.containsKey(id.toLong)
          SqlParentRaceAgentTest.probed.countDown()
          s"$id $hasSpan"
        }(Encoders.STRING).collect().head

        // Without this the lag could have ended before the job ran, and the check would prove nothing.
        assert(releasedByProbe.get(), "the listener bus was released by its timeout, not by the probe")
        assert(seen.endsWith("true"),
          s"no span for SQL execution ${seen.split(" ").head} while its job ran: the job was parented to the application")
      } finally spark.stop()
    }
  }
}

object SqlParentRaceAgentTest {
  // Static, so the task's closure refers to the same latch as the listener: a captured local would
  // be serialized into the task, even in local mode, and counting down the copy would release
  // nothing.
  val probed = new CountDownLatch(1)
}
