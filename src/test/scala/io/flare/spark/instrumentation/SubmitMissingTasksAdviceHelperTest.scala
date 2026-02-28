package io.flare.spark.instrumentation

import io.opentelemetry.api.trace.{Span, SpanKind, Tracer}
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import munit.FunSuite

/**
 * Unit tests for [[SubmitMissingTasksAdviceHelper]].
 *
 * Tests the pending span adoption mechanics (store, adopt, cleanup) without
 * requiring a real DAGScheduler. Full integration tests with SparkSession
 * verify the end-to-end span hierarchy.
 */
class SubmitMissingTasksAdviceHelperTest extends FunSuite {

  private val exporter = InMemorySpanExporter.create()

  private val tracerProvider: SdkTracerProvider = SdkTracerProvider.builder()
    .addSpanProcessor(SimpleSpanProcessor.create(exporter))
    .build()

  private val tracer: Tracer = tracerProvider.get("test")

  override def afterAll(): Unit = {
    tracerProvider.shutdown()
    super.afterAll()
  }

  override def beforeEach(context: BeforeEach): Unit = {
    exporter.reset()
    SubmitMissingTasksAdviceHelper.jobSpans.clear()
    SubmitMissingTasksAdviceHelper.pendingStageSpans.clear()
    SubmitMissingTasksAdviceHelper.activeSQLSpans.clear()
    super.beforeEach(context)
  }

  // ── Helper ────────────────────────────────────────────────────────────────

  private def createTestSpan(name: String = "test.span"): Span =
    tracer.spanBuilder(name)
      .setSpanKind(SpanKind.INTERNAL)
      .startSpan()

  // ── Tests: getJobSpan / removeJobSpan ───────────────────────────────────

  test("getJobSpan returns the stored span without removing it") {
    val span = createTestSpan("spark.job.0")
    SubmitMissingTasksAdviceHelper.jobSpans.put(0, span)

    val first = SubmitMissingTasksAdviceHelper.getJobSpan(0)
    assert(first.isDefined, "First call should return Some(span)")
    assertEquals(first.get.getSpanContext.getSpanId, span.getSpanContext.getSpanId)

    // Second call should ALSO return the span (not removed)
    val second = SubmitMissingTasksAdviceHelper.getJobSpan(0)
    assert(second.isDefined, "Second call should still return Some(span)")
  }

  test("getJobSpan returns None for unknown jobId") {
    val result = SubmitMissingTasksAdviceHelper.getJobSpan(999)
    assert(result.isEmpty)
  }

  test("removeJobSpan removes and returns the span") {
    val span = createTestSpan("spark.job.1")
    SubmitMissingTasksAdviceHelper.jobSpans.put(1, span)

    val removed = SubmitMissingTasksAdviceHelper.removeJobSpan(1)
    assert(removed.isDefined)
    assertEquals(removed.get.getSpanContext.getSpanId, span.getSpanContext.getSpanId)

    // After removal, getJobSpan should return None
    val after = SubmitMissingTasksAdviceHelper.getJobSpan(1)
    assert(after.isEmpty, "Should be None after removal")
  }

  // ── Tests: adoptPendingStageSpan ────────────────────────────────────────

  test("adoptPendingStageSpan returns and removes the stored span") {
    val span = createTestSpan("spark.stage.10")
    SubmitMissingTasksAdviceHelper.pendingStageSpans.put(10, span)

    val result = SubmitMissingTasksAdviceHelper.adoptPendingStageSpan(10)
    assert(result.isDefined)
    assertEquals(result.get.getSpanContext.getSpanId, span.getSpanContext.getSpanId)

    // Second call should return None (already adopted)
    val second = SubmitMissingTasksAdviceHelper.adoptPendingStageSpan(10)
    assert(second.isEmpty, "Second call should return None")
  }

  test("adoptPendingStageSpan returns None for unknown stageId") {
    val result = SubmitMissingTasksAdviceHelper.adoptPendingStageSpan(999)
    assert(result.isEmpty)
  }

  // ── Tests: multiple jobs and stages ─────────────────────────────────────

  test("multiple jobs are independent") {
    val span0 = createTestSpan("spark.job.0")
    val span1 = createTestSpan("spark.job.1")
    SubmitMissingTasksAdviceHelper.jobSpans.put(0, span0)
    SubmitMissingTasksAdviceHelper.jobSpans.put(1, span1)

    val result0 = SubmitMissingTasksAdviceHelper.getJobSpan(0)
    val result1 = SubmitMissingTasksAdviceHelper.getJobSpan(1)
    assert(result0.isDefined)
    assert(result1.isDefined)
    assertEquals(result0.get.getSpanContext.getSpanId, span0.getSpanContext.getSpanId)
    assertEquals(result1.get.getSpanContext.getSpanId, span1.getSpanContext.getSpanId)
  }

  test("multiple stages for same job are independent") {
    val stage10 = createTestSpan("spark.stage.10")
    val stage11 = createTestSpan("spark.stage.11")
    SubmitMissingTasksAdviceHelper.pendingStageSpans.put(10, stage10)
    SubmitMissingTasksAdviceHelper.pendingStageSpans.put(11, stage11)

    val result10 = SubmitMissingTasksAdviceHelper.adoptPendingStageSpan(10)
    assert(result10.isDefined)
    assertEquals(result10.get.getSpanContext.getSpanId, stage10.getSpanContext.getSpanId)

    // Stage 11 should still be there
    val result11 = SubmitMissingTasksAdviceHelper.adoptPendingStageSpan(11)
    assert(result11.isDefined)
    assertEquals(result11.get.getSpanContext.getSpanId, stage11.getSpanContext.getSpanId)
  }

  test("onEnter returns safely with null dagScheduler") {
    // Should not throw — just silently skip
    SubmitMissingTasksAdviceHelper.onEnter(null, null, 0)
    assert(SubmitMissingTasksAdviceHelper.jobSpans.isEmpty)
  }
}
