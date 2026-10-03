package io.flare.spark.trace

import io.opentelemetry.api.trace.Span
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import munit.FunSuite

/**
 * The TaskRunner advice makes the stage's context current inside a real Spark task, under the real
 * agent, at the default `stages` granularity.
 *
 * At `stages` Flare opens no task span, so the advice is the only thing restoring context on the
 * executor: if it fails, spans and logs made inside the task lose their trace. At `tasks` or `all`
 * the executor plugin opens its own scope, which would hide the failure, so this test must run at
 * the default.
 *
 * Failed on 1.3.0 (#173): the advice's `Scope` signature did not survive the agent's relocation.
 */
class TaskContextAgentTest extends FunSuite {

  test("a task at the default granularity runs inside its stage's trace context") {
    // The plugins flush on shutdown and when executors go idle. Without a collector those exports
    // fail and the agent's exporter backs off, which delays AgentFlushTest's flush in this JVM.
    StubCollector.during {
      val sc = new SparkContext(
        new SparkConf()
          .setMaster("local[1]")
          .setAppName("flare-task-context-probe")
          .set("spark.ui.enabled", "false")
          .set("spark.plugins", "io.flare.spark.plugin.FlareSparkPlugin"))
      try {
        val seen = sc.parallelize(Seq(1), 1).map { _ =>
          val traceparent = TaskContext.get().getLocalProperty("traceparent")
          val current = Span.current().getSpanContext
          (traceparent, current.isValid, current.getTraceId, current.getSpanId)
        }.collect().head
        val (traceparent, valid, traceId, spanId) = seen
        assert(traceparent != null, "the task has no traceparent property: injection did not run")
        val parts = traceparent.split("-")
        val (tpTrace, tpSpan) = (parts(1), parts(2))
        assert(valid, s"no current span inside the task, although its traceparent is $traceparent")
        assertEquals(traceId, tpTrace, "the task's current trace is not the one in its traceparent")
        assertEquals(spanId, tpSpan, "the task's current span is not the stage span in its traceparent")
      } finally sc.stop()
    }
  }
}
