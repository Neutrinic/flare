package io.flare.spark.plugin

import io.flare.spark.config.{FlareConfig, TraceGranularity}
import io.flare.spark.listener.TracingSparkListener
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import munit.FunSuite

class FlareDriverStateTest extends FunSuite {

  private val exporter = InMemorySpanExporter.create()

  private val tracerProvider: SdkTracerProvider = SdkTracerProvider.builder()
    .addSpanProcessor(SimpleSpanProcessor.create(exporter))
    .build()

  private val sdk: OpenTelemetrySdk = OpenTelemetrySdk.builder()
    .setTracerProvider(tracerProvider)
    .build()

  private val tracer: Tracer = sdk.getTracer("test")

  private val testConfig: FlareConfig = FlareConfig(
    enabled          = true,
    granularity      = TraceGranularity.Stages,
    samplingRatio    = 1.0,
    maxSpansPerTrace = 10000,
    slowTaskMs       = 0L,
    retryTasksOnly   = false,
    taskStageIds     = Set.empty,
    taskStagePattern = None,
    metricsEnabled   = true,
  )

  override def beforeEach(context: BeforeEach): Unit = {
    FlareDriverState.reset()
    exporter.reset()
    super.beforeEach(context)
  }

  // ── Basic lifecycle ─────────────────────────────────────────────────────────

  test("starts uninitialized") {
    assert(!FlareDriverState.initialized)
  }

  test("initialize sets initialized to true and returns true") {
    val span = tracer.spanBuilder("test-app").startSpan()
    val listener = new TracingSparkListener(tracer, testConfig)
    assert(FlareDriverState.initialize(span, listener))
    assert(FlareDriverState.initialized)
    span.end()
  }

  test("second initialize returns false (dedup)") {
    val span1 = tracer.spanBuilder("app1").startSpan()
    val listener1 = new TracingSparkListener(tracer, testConfig)
    assert(FlareDriverState.initialize(span1, listener1))

    val span2 = tracer.spanBuilder("app2").startSpan()
    val listener2 = new TracingSparkListener(tracer, testConfig)
    assert(!FlareDriverState.initialize(span2, listener2))

    // First span is still the one in state
    assert(FlareDriverState.applicationSpan.contains(span1))
    span1.end()
    span2.end()
  }

  test("shutdown resets state") {
    val span = tracer.spanBuilder("test-app").startSpan()
    val listener = new TracingSparkListener(tracer, testConfig)
    FlareDriverState.initialize(span, listener)
    assert(FlareDriverState.initialized)

    FlareDriverState.shutdown()
    assert(!FlareDriverState.initialized)
  }

  test("shutdown is idempotent") {
    val span = tracer.spanBuilder("test-app").startSpan()
    val listener = new TracingSparkListener(tracer, testConfig)
    FlareDriverState.initialize(span, listener)

    FlareDriverState.shutdown()
    assert(!FlareDriverState.initialized)
    // Second shutdown is a no-op — should not throw
    FlareDriverState.shutdown()
    assert(!FlareDriverState.initialized)
  }

  test("can re-initialize after shutdown") {
    val span1 = tracer.spanBuilder("app1").startSpan()
    val listener1 = new TracingSparkListener(tracer, testConfig)
    FlareDriverState.initialize(span1, listener1)
    FlareDriverState.shutdown()

    val span2 = tracer.spanBuilder("app2").startSpan()
    val listener2 = new TracingSparkListener(tracer, testConfig)
    assert(FlareDriverState.initialize(span2, listener2))
    assert(FlareDriverState.initialized)
    assert(FlareDriverState.applicationSpan.contains(span2))
    span2.end()
  }

  test("reset clears all state without calling listener shutdown") {
    val span = tracer.spanBuilder("test-app").startSpan()
    val listener = new TracingSparkListener(tracer, testConfig)
    FlareDriverState.initialize(span, listener)

    FlareDriverState.reset()
    assert(!FlareDriverState.initialized)
    assert(FlareDriverState.applicationSpan.isEmpty)
    span.end()
  }

  // ── Thread safety ─────────────────────────────────────────────────────────

  test("concurrent initialize calls — exactly one wins") {
    import java.util.concurrent.{CountDownLatch, CyclicBarrier}
    import java.util.concurrent.atomic.AtomicInteger

    val barrier = new CyclicBarrier(10)
    val wins = new AtomicInteger(0)
    val latch = new CountDownLatch(10)

    (1 to 10).foreach { i =>
      new Thread(() => {
        val span = tracer.spanBuilder(s"app-$i").startSpan()
        val listener = new TracingSparkListener(tracer, testConfig)
        barrier.await() // all threads start together
        if (FlareDriverState.initialize(span, listener)) {
          wins.incrementAndGet()
        }
        span.end()
        latch.countDown()
      }).start()
    }

    latch.await()
    assertEquals(wins.get(), 1, "Exactly one thread should win the init race")
    assert(FlareDriverState.initialized)
  }
}
