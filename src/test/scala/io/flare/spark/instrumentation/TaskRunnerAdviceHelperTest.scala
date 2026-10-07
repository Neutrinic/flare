package io.flare.spark.instrumentation

import io.flare.spark.propagation.LocalPropertyPropagator
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

      TaskRunnerAdviceHelper.onExit(scope)
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
      TaskRunnerAdviceHelper.onExit(scope)
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

      TaskRunnerAdviceHelper.onExit(scope)

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

  // ── Per-stage context (#204) ──────────────────────────────────────────

  /** Mimics TaskDescription's `name` and `properties` accessors. */
  class NamedTaskDescription(val name: String, val props: ju.Properties) {
    def properties(): ju.Properties = props
  }

  /** One job's properties after two of its stages were submitted together: 1, then 2. */
  private def twoStagesInjected(first: Span, second: Span): ju.Properties = {
    val props = new ju.Properties()
    LocalPropertyPropagator.injectForStage(Context.root().`with`(first), props, 1)
    LocalPropertyPropagator.injectForStage(Context.root().`with`(second), props, 2)
    props
  }

  private def currentSpanIdOnEnter(td: AnyRef): String = {
    val scope = TaskRunnerAdviceHelper.onEnter(td)
    assert(scope != null, "no context was made current")
    try Span.current().getSpanContext.getSpanId
    finally TaskRunnerAdviceHelper.onExit(scope)
  }

  test("a task runs under its own stage when the job's later stage overwrote the generic keys") {
    val (s1, s2) = (tracer.spanBuilder("spark.stage.1").startSpan(), tracer.spanBuilder("spark.stage.2").startSpan())
    try {
      val props = twoStagesInjected(s1, s2)
      assertEquals(currentSpanIdOnEnter(new NamedTaskDescription("task 3.0 in stage 1.0 (TID 7)", props)),
        s1.getSpanContext.getSpanId)
      assertEquals(currentSpanIdOnEnter(new NamedTaskDescription("task 0.0 in stage 2.0 (TID 8)", props)),
        s2.getSpanContext.getSpanId)
    } finally { s1.end(); s2.end() }
  }

  test("a task falls back to the generic keys when its stage has none, or its name has no stage") {
    val (s1, s2) = (tracer.spanBuilder("spark.stage.1").startSpan(), tracer.spanBuilder("spark.stage.2").startSpan())
    try {
      val props = twoStagesInjected(s1, s2)
      assertEquals(currentSpanIdOnEnter(new NamedTaskDescription("task 0.0 in stage 9.0 (TID 1)", props)),
        s2.getSpanContext.getSpanId)
      assertEquals(currentSpanIdOnEnter(new NamedTaskDescription("unnamed", props)), s2.getSpanContext.getSpanId)
      assertEquals(currentSpanIdOnEnter(new MockTaskDescription(props)), s2.getSpanContext.getSpanId)
    } finally { s1.end(); s2.end() }
  }

  test("stageIdOf reads the stage from Spark's task name") {
    assertEquals(TaskRunnerAdviceHelper.stageIdOf(new NamedTaskDescription("task 12.1 in stage 345.2 (TID 9)", null)), 345)
    assertEquals(TaskRunnerAdviceHelper.stageIdOf(new NamedTaskDescription("task 1.0", null)), -1)
    assertEquals(TaskRunnerAdviceHelper.stageIdOf(new MockTaskDescription(null)), -1)
  }

  test("removeStage drops only that stage's keys") {
    val (s1, s2) = (tracer.spanBuilder("spark.stage.1").startSpan(), tracer.spanBuilder("spark.stage.2").startSpan())
    try {
      val props = twoStagesInjected(s1, s2)
      LocalPropertyPropagator.removeStage(props, 1)
      assertEquals(props.getProperty("flare.stage.1.traceparent"), null)
      assert(props.getProperty("flare.stage.2.traceparent") != null, "stage 2's key was removed")
      assert(props.getProperty("traceparent") != null, "the generic key was removed")
    } finally { s1.end(); s2.end() }
  }
}
