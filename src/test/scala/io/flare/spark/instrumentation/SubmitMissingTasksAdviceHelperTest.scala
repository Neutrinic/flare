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

  // ── #104: a lost race must not publish an empty duplicate job span ─────────

  /**
   * Two writers create job spans for the same jobId: this advice on
   * `dag-scheduler-event-loop`, and `TracingSparkListener.onJobStart` on the listener bus.
   *
   * The old shape started a span, called `putIfAbsent`, then `end()`-ed the loser. Ending a
   * span is what exports it, so every lost race published a 0ms `spark.job.N` span with no
   * attributes beside the real one. `getOrCreateJobSpan` builds at most once per jobId, so a
   * loser never starts a span at all.
   */
  test("concurrent callers create exactly one job span, and losers export nothing") {
    SubmitMissingTasksAdviceHelper.jobSpans.clear()
    exporter.reset()

    val threads  = 16
    val started  = new java.util.concurrent.atomic.AtomicInteger(0)
    val barrier  = new java.util.concurrent.CyclicBarrier(threads)
    val results  = new java.util.concurrent.ConcurrentLinkedQueue[Span]()
    val pool     = java.util.concurrent.Executors.newFixedThreadPool(threads)

    try {
      val tasks = (1 to threads).map { _ =>
        new Runnable {
          override def run(): Unit = {
            barrier.await()
            val span = SubmitMissingTasksAdviceHelper.getOrCreateJobSpan(99) {
              started.incrementAndGet()
              tracer.spanBuilder("spark.job.99").setSpanKind(SpanKind.INTERNAL).startSpan()
            }
            results.add(span)
          }
        }
      }
      tasks.foreach(pool.submit(_: Runnable))
      pool.shutdown()
      assert(
        pool.awaitTermination(20, java.util.concurrent.TimeUnit.SECONDS),
        "threads did not finish",
      )
    } finally pool.shutdownNow()

    // The builder must have run once, not once per loser.
    assertEquals(started.get(), 1, "more than one span was started for the same jobId")

    // Every caller must see the same span.
    val distinct = scala.collection.JavaConverters
      .collectionAsScalaIterableConverter(results).asScala
      .map(_.getSpanContext.getSpanId).toSet
    assertEquals(distinct.size, 1, s"callers saw different spans: $distinct")

    // Nothing may have been exported yet: no span has been ended. Under the old shape the
    // losing threads would have ended theirs here, publishing empty duplicates.
    assertEquals(
      exporter.getFinishedSpanItems.size(), 0,
      "a span was exported before anything was ended, which is the #104 duplicate",
    )

    // Ending the single survivor yields exactly one exported span.
    SubmitMissingTasksAdviceHelper.jobSpans.get(99).end()
    assertEquals(exporter.getFinishedSpanItems.size(), 1)

    SubmitMissingTasksAdviceHelper.jobSpans.clear()
  }
}
