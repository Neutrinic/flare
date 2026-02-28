package io.flare.spark.instrumentation

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, SpanKind, Tracer}
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import munit.FunSuite

import java.{util => ju}

/**
 * Unit tests for [[TaskRunnerAdviceHelper]].
 *
 * Tests the extraction logic and context scoping without requiring a real
 * Spark TaskRunner — uses a mock object with a `properties()` method to
 * simulate `TaskDescription`.
 */
class TaskRunnerAdviceHelperTest extends FunSuite {

  private val exporter = InMemorySpanExporter.create()

  private val tracerProvider: SdkTracerProvider = SdkTracerProvider.builder()
    .addSpanProcessor(SimpleSpanProcessor.create(exporter))
    .build()

  private val sdk: OpenTelemetrySdk = OpenTelemetrySdk.builder()
    .setTracerProvider(tracerProvider)
    .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
    .buildAndRegisterGlobal()

  private val tracer: Tracer = sdk.getTracer("test")

  override def afterAll(): Unit = {
    GlobalOpenTelemetry.resetForTest()
    tracerProvider.shutdown()
    super.afterAll()
  }

  override def beforeEach(context: BeforeEach): Unit = {
    exporter.reset()
    super.beforeEach(context)
  }

  // ── Helper: mock TaskDescription with properties() method ─────────────

  /** Mimics TaskDescription's `properties` accessor via reflection. */
  class MockTaskDescription(val props: ju.Properties) {
    def properties(): ju.Properties = props
  }

  private def makeTraceparent(span: Span): String = {
    val sc = span.getSpanContext
    val flags = if (sc.isSampled) "01" else "00"
    s"00-${sc.getTraceId}-${sc.getSpanId}-$flags"
  }

  // ── Tests ─────────────────────────────────────────────────────────────

  test("onEnter returns null for null taskDescription") {
    val scope = TaskRunnerAdviceHelper.onEnter(null)
    assert(scope == null)
  }

  test("onEnter returns null when properties has no traceparent") {
    val props = new ju.Properties()
    val mock = new MockTaskDescription(props)
    val scope = TaskRunnerAdviceHelper.onEnter(mock)
    assert(scope == null)
  }

  test("onEnter returns Scope and makes parent context current") {
    val parentSpan = tracer.spanBuilder("parent")
      .setSpanKind(SpanKind.SERVER)
      .startSpan()

    try {
      val traceparent = makeTraceparent(parentSpan)
      val props = new ju.Properties()
      props.setProperty("traceparent", traceparent)

      val mock = new MockTaskDescription(props)
      val scope = TaskRunnerAdviceHelper.onEnter(mock)

      assert(scope != null, "Scope should be non-null when traceparent is present")

      // The current context should contain the parent span's trace context
      val currentSpan = Span.fromContext(Context.current())
      assertEquals(
        currentSpan.getSpanContext.getTraceId,
        parentSpan.getSpanContext.getTraceId
      )
      assertEquals(
        currentSpan.getSpanContext.getSpanId,
        parentSpan.getSpanContext.getSpanId
      )

      scope.close()
    } finally {
      parentSpan.end()
    }
  }

  test("onExit closes scope and restores previous context") {
    val parentSpan = tracer.spanBuilder("parent").startSpan()

    try {
      val traceparent = makeTraceparent(parentSpan)
      val props = new ju.Properties()
      props.setProperty("traceparent", traceparent)

      // Record the context before entering
      val contextBefore = Context.current()

      val mock = new MockTaskDescription(props)
      val scope = TaskRunnerAdviceHelper.onEnter(mock)
      assert(scope != null)

      // Context changed
      assert(Context.current() != contextBefore)

      // Exit restores it
      TaskRunnerAdviceHelper.onExit(scope)
      assertEquals(Context.current(), contextBefore)
    } finally {
      parentSpan.end()
    }
  }

  test("onExit is safe with null scope") {
    // Should not throw
    TaskRunnerAdviceHelper.onExit(null)
  }

  test("onEnter with tracestate propagates both headers") {
    val parentSpan = tracer.spanBuilder("parent").startSpan()

    try {
      val traceparent = makeTraceparent(parentSpan)
      val props = new ju.Properties()
      props.setProperty("traceparent", traceparent)
      props.setProperty("tracestate", "vendor=value")

      val mock = new MockTaskDescription(props)
      val scope = TaskRunnerAdviceHelper.onEnter(mock)
      assert(scope != null)
      scope.close()
    } finally {
      parentSpan.end()
    }
  }

  test("onEnter with invalid traceparent returns null") {
    val props = new ju.Properties()
    props.setProperty("traceparent", "not-a-valid-traceparent")

    val mock = new MockTaskDescription(props)
    val scope = TaskRunnerAdviceHelper.onEnter(mock)
    // Invalid traceparent → extraction returns root context → null scope
    assert(scope == null)
  }

  test("onEnter with object lacking properties method returns null") {
    // Pass an object that doesn't have a properties() method
    val scope = TaskRunnerAdviceHelper.onEnter("not-a-task-description")
    assert(scope == null, "Should gracefully handle missing properties method")
  }

  test("child span created inside scope has correct parent") {
    val parentSpan = tracer.spanBuilder("parent")
      .setSpanKind(SpanKind.SERVER)
      .startSpan()

    try {
      val traceparent = makeTraceparent(parentSpan)
      val props = new ju.Properties()
      props.setProperty("traceparent", traceparent)

      val mock = new MockTaskDescription(props)
      val scope = TaskRunnerAdviceHelper.onEnter(mock)
      assert(scope != null)

      // Create a child span — simulates what ExecutorPlugin or JDBC instrumentation does
      val childSpan = tracer.spanBuilder("spark.task.executor")
        .setSpanKind(SpanKind.INTERNAL)
        .startSpan()
      childSpan.end()

      scope.close()

      // Verify the child span's parent is the remote parent
      val spans = exporter.getFinishedSpanItems
      val childExport = spans.stream()
        .filter(s => s.getName == "spark.task.executor")
        .findFirst()
        .orElseThrow()

      assertEquals(
        childExport.getParentSpanId,
        parentSpan.getSpanContext.getSpanId,
        "Child span should be parented under the extracted remote context"
      )
      assertEquals(
        childExport.getTraceId,
        parentSpan.getSpanContext.getTraceId,
        "Child span should share the same traceId"
      )
    } finally {
      parentSpan.end()
    }
  }
}
