package io.flare.spark.config

import io.flare.spark.listener.TracingSparkListener
import io.flare.spark.plugin.FlareDriverState
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.`export`.{BatchSpanProcessor, SpanExporter}
import munit.FunSuite

import java.time.Duration
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import java.{util => ju}
import scala.collection.JavaConverters._

/**
 * The shutdown race behind #122, reproduced with a real SDK.
 *
 * A cluster manager's SIGTERM starts the agent's shutdown hook and Flare's together. If the batch
 * processor shuts down before Flare ends the root span, the root is dropped. The batch interval
 * here is an hour, so only the shutdown's final export can deliver anything.
 */
class EndDriverSpansOnShutdownTest extends FunSuite {

  private val config = FlareConfig(
    enabled = true, granularity = TraceGranularity.Stages, samplingRatio = 1.0,
    maxSpansPerTrace = 10000, slowTaskMs = 0L, retryTasksOnly = false,
    taskStageIds = Set.empty, taskStagePattern = None, metricsEnabled = false,
  )

  /** Keeps what it received after shutdown, unlike InMemorySpanExporter, which clears itself. */
  private final class Collecting extends SpanExporter {
    val names = new ConcurrentLinkedQueue[String]()
    override def `export`(spans: ju.Collection[SpanData]): CompletableResultCode = {
      spans.forEach(s => names.add(s.getName))
      CompletableResultCode.ofSuccess()
    }
    override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()
    override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
  }

  override def beforeEach(context: BeforeEach): Unit = {
    FlareDriverState.endOpenSpans() // clear anything a previous test left initialised
    super.beforeEach(context)
  }

  private def provider(exporter: Collecting, wrap: Boolean): SdkTracerProvider = {
    val batch = BatchSpanProcessor.builder(exporter).setScheduleDelay(Duration.ofHours(1)).build()
    SdkTracerProvider.builder()
      .addSpanProcessor(if (wrap) new EndDriverSpansOnShutdown(batch) else batch)
      .build()
  }

  private def startDriver(tp: SdkTracerProvider): Unit = {
    val tracer = tp.get("io.flare.spark")
    val root = tracer.spanBuilder("spark.application").startSpan()
    val listener = new TracingSparkListener(tracer, config)
    listener.setApplicationSpan(root)
    assert(FlareDriverState.initialize(root, listener))
  }

  private def exportedNames(exporter: Collecting): Seq[String] =
    exporter.names.asScala.toSeq

  test("control: when the processor shuts down first, the root span is lost") {
    val exporter = new Collecting
    val tp = provider(exporter, wrap = false)
    startDriver(tp)

    tp.shutdown().join(5, TimeUnit.SECONDS) // the agent's hook wins the race
    FlareDriverState.endOpenSpans()          // Flare's hook, too late

    assertEquals(exportedNames(exporter), Nil)
  }

  test("the wrapper ends Flare's spans before the processor shuts down, so the root is exported") {
    val exporter = new Collecting
    val tp = provider(exporter, wrap = true)
    startDriver(tp)

    tp.shutdown().join(5, TimeUnit.SECONDS)

    assertEquals(exportedNames(exporter), Seq("spark.application"))
    assert(!FlareDriverState.initialized, "Flare's state should have been shut down by the wrapper")
  }

  // Guards, not evidence: the next two pass with or without the wrapper. They pin that the wrapper
  // does no harm when Flare got there first or never registered. The control and the test above
  // are the regression evidence.
  test("if Flare already ended its spans, shutdown is unaffected") {
    val exporter = new Collecting
    val tp = provider(exporter, wrap = true)
    startDriver(tp)

    FlareDriverState.endOpenSpans() // Flare's hook got there first
    tp.shutdown().join(5, TimeUnit.SECONDS)

    assertEquals(exportedNames(exporter), Seq("spark.application"))
  }

  test("without Flare registered, the wrapper just shuts the processor down") {
    val exporter = new Collecting
    val tp = provider(exporter, wrap = true)
    tp.get("other").spanBuilder("unrelated").startSpan().end()

    tp.shutdown().join(5, TimeUnit.SECONDS)

    assertEquals(exportedNames(exporter), Seq("unrelated"))
  }
}
