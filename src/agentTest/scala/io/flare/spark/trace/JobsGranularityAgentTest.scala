package io.flare.spark.trace

import io.flare.spark.instrumentation.SubmitMissingTasksAdviceHelper
import io.opentelemetry.api.trace.Span
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import munit.FunSuite

import scala.collection.JavaConverters._

/**
 * At `jobs` granularity the scheduler advice creates no stage spans, and tasks run in the job's
 * context (#174).
 *
 * The advice used to create a stage span for every stage whatever the granularity. At `jobs` the
 * listener never adopted it, so it was never ended or exported, stayed in the pending map for the
 * life of the driver, and its id was what every task carried: executor logs pointed at a span no
 * backend ever received.
 *
 * `local[1]` runs the task in this JVM, so the task can read the driver-side maps while its job is
 * still running.
 */
class JobsGranularityAgentTest extends FunSuite {

  test("at jobs granularity tasks run in the job's context and no stage span is left pending") {
    sys.props("FLARE_TRACE_GRANULARITY") = "jobs"
    try StubCollector.during {
      val sc = new SparkContext(
        new SparkConf()
          .setMaster("local[1]")
          .setAppName("flare-jobs-granularity-probe")
          .set("spark.ui.enabled", "false")
          .set("spark.plugins", "io.flare.spark.plugin.FlareSparkPlugin"))
      try {
        val (traceparent, current, jobSpanIds, pending) = sc.parallelize(Seq(1), 1).map { _ =>
          (
            TaskContext.get().getLocalProperty("traceparent"),
            Span.current().getSpanContext.getSpanId,
            SubmitMissingTasksAdviceHelper.jobSpans.values().asScala.map(_.getSpanContext.getSpanId).toSet,
            SubmitMissingTasksAdviceHelper.pendingStageSpans.size(),
          )
        }.collect().head

        assertEquals(pending, 0, "a stage span was created and left pending at jobs granularity")
        assert(traceparent != null, "the task has no traceparent property")
        val parent = traceparent.split("-")(2)
        assert(jobSpanIds.contains(parent), s"the task's traceparent $traceparent does not name its job span $jobSpanIds")
        assertEquals(current, parent, "the task does not run in its job's context")
      } finally sc.stop()
    } finally sys.props.remove("FLARE_TRACE_GRANULARITY")
  }
}
