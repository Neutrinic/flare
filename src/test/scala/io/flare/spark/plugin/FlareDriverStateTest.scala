package io.flare.spark.plugin

import io.flare.spark.config.{FlareConfig, TraceGranularity}
import io.flare.spark.listener.TracingSparkListener
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import munit.FunSuite
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerJobEnd, SparkListenerJobStart}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.{util => ju}
import scala.collection.JavaConverters._

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

  // ── Idle flush (#199) ───────────────────────────────────────────────────────

  test("the driver flushes once no job has ended for a second, not at the next export interval") {
    val flushes = new AtomicInteger()
    FlareDriverState.idleFlushAction = () => flushes.incrementAndGet()
    try {
      val listener = new TracingSparkListener(tracer, testConfig, throwOnError = true)
      FlareDriverState.initialize(tracer.spanBuilder("spark.application").startSpan(), listener)
      // Three jobs in quick succession: one flush, once they stop.
      (0 to 2).foreach { id =>
        listener.onJobStart(SparkListenerJobStart(id, System.currentTimeMillis(), Seq.empty, new ju.Properties()))
        listener.onJobEnd(SparkListenerJobEnd(id, System.currentTimeMillis(), JobSucceeded))
      }
      assertEquals(flushes.get(), 0, "flushed before the driver went quiet")
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
      while (flushes.get() == 0 && System.nanoTime() < deadline) Thread.sleep(50)
      assertEquals(flushes.get(), 1, "the driver did not flush once its jobs stopped")
    } finally FlareDriverState.idleFlushAction = () => TelemetryFlush.flush("driver idle")
  }

  test("shutdown cancels a pending idle flush, and a job end after it starts none") {
    val flushes = new AtomicInteger()
    FlareDriverState.idleFlushAction = () => flushes.incrementAndGet()
    def flushThreadAlive = Thread.getAllStackTraces.keySet.asScala
      .exists(t => t.getName == "flare-driver-idle-flush" && t.isAlive)
    try {
      val listener = new TracingSparkListener(tracer, testConfig, throwOnError = true)
      FlareDriverState.initialize(tracer.spanBuilder("spark.application").startSpan(), listener)
      listener.onJobStart(SparkListenerJobStart(0, System.currentTimeMillis(), Seq.empty, new ju.Properties()))
      listener.onJobEnd(SparkListenerJobEnd(0, System.currentTimeMillis(), JobSucceeded))
      assert(flushThreadAlive, "the job end did not schedule a flush")

      FlareDriverState.shutdown() // well inside the 1s quiet period
      FlareDriverState.jobEnded()
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
      while (flushThreadAlive && System.nanoTime() < deadline) Thread.sleep(50)
      assert(!flushThreadAlive, "the idle-flush thread outlived shutdown")
      Thread.sleep(1500) // past the quiet period the cancelled flush would have waited
      assertEquals(flushes.get(), 0, "a flush ran after shutdown")
    } finally FlareDriverState.idleFlushAction = () => TelemetryFlush.flush("driver idle")
  }
}
