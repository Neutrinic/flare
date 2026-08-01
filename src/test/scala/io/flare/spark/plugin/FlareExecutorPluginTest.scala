package io.flare.spark.plugin

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.{InMemoryMetricReader, InMemorySpanExporter}
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import munit.FunSuite
import org.apache.spark.FlareTestHelpers
import org.apache.spark.api.plugin.PluginContext

import scala.collection.JavaConverters._

/**
 * Covers the interaction between task span suppression and task metric recording.
 *
 * Two rules are asserted here, and they pull in opposite directions:
 *   1. A task whose span is suppressed must still be measured. Metrics are pre-aggregated,
 *      so the cardinality pressure that justifies dropping spans does not apply to them.
 *   2. A metric recorded for a suppressed task must NOT carry an exemplar, because an
 *      exemplar names a span that will never reach the backend.
 */
class FlareExecutorPluginTest extends FunSuite {

  private val flareProps = List(
    "FLARE_TRACE_GRANULARITY", "FLARE_SLOW_TASK_MS", "FLARE_MAX_SPANS_PER_TRACE",
    "FLARE_SAMPLING_RATIO", "FLARE_METRICS_ENABLED",
  )

  override def afterEach(context: AfterEach): Unit = {
    flareProps.foreach(sys.props.remove)
    FlareTestHelpers.unbindTaskContext()
    GlobalOpenTelemetry.resetForTest()
    super.afterEach(context)
  }

  private object StubPluginContext extends PluginContext {
    override def metricRegistry(): com.codahale.metrics.MetricRegistry = null
    override def conf(): org.apache.spark.SparkConf                    = new org.apache.spark.SparkConf()
    override def executorID(): String                                  = "7"
    override def hostname(): String                                    = "localhost"
    override def resources(): java.util.Map[String, org.apache.spark.resource.ResourceInformation] =
      java.util.Collections.emptyMap()
    override def send(message: Any): Unit  = ()
    override def ask(message: Any): AnyRef = null
  }

  /** Result of driving `taskCount` tasks through a freshly initialised plugin. */
  private case class Run(
    exportedTraceIds: Set[String],
    durationCount:    Long,
    exemplarTraceIds: Seq[String],
  )

  /**
   * Registers a test SDK as the global OpenTelemetry, runs `taskCount` complete tasks through
   * a real FlareExecutorPlugin, and reports what was exported.
   *
   * The plugin resolves its tracer and meter from GlobalOpenTelemetry, so the SDK has to be
   * registered before init().
   */
  private def runTasks(taskCount: Int = 1, succeed: Boolean = true): Run = {
    val spanExporter = InMemorySpanExporter.create()
    val metricReader = InMemoryMetricReader.create()

    val sdk = OpenTelemetrySdk.builder()
      .setTracerProvider(
        SdkTracerProvider.builder()
          .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
          .build()
      )
      .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader).build())
      .buildAndRegisterGlobal()

    try {
      val plugin = new FlareExecutorPlugin()
      plugin.init(StubPluginContext, java.util.Collections.emptyMap[String, String]())

      (1 to taskCount).foreach { _ =>
        FlareTestHelpers.bindEmptyTaskContext()
        plugin.onTaskStart()
        if (succeed) plugin.onTaskSucceeded()
        else plugin.onTaskFailed(org.apache.spark.TaskResultLost)
        FlareTestHelpers.unbindTaskContext()
      }

      val exported = spanExporter.getFinishedSpanItems.asScala
        .map(_.getSpanContext.getTraceId)
        .toSet

      val durationPoints = metricReader.collectAllMetrics().asScala
        .filter(_.getName == "flare.task.duration")
        .flatMap(_.getHistogramData.getPoints.asScala)

      Run(
        exportedTraceIds = exported,
        durationCount    = durationPoints.map(_.getCount).sum,
        exemplarTraceIds = durationPoints
          .flatMap(_.getExemplars.asScala)
          .map(_.getSpanContext.getTraceId)
          .toSeq,
      )
    } finally sdk.close()
  }

  // ── Baseline ───────────────────────────────────────────────────────────────

  test("a traced task exports one span and one metric point linked by an exemplar") {
    sys.props("FLARE_TRACE_GRANULARITY") = "all"
    val run = runTasks()

    assertEquals(run.exportedTraceIds.size, 1)
    assertEquals(run.durationCount, 1L)
    assertEquals(run.exemplarTraceIds.size, 1)
    // The exemplar must name the span that was actually exported.
    assertEquals(run.exemplarTraceIds.toSet, run.exportedTraceIds)
  }

  // ── #52: slow-task suppression must not leave a dangling exemplar ───────────

  test("a task suppressed by FLARE_SLOW_TASK_MS is measured but exports no span") {
    sys.props("FLARE_TRACE_GRANULARITY") = "all"
    // Far above any plausible duration for an empty task, so every task is suppressed.
    sys.props("FLARE_SLOW_TASK_MS") = "600000"
    val run = runTasks()

    assertEquals(run.exportedTraceIds, Set.empty[String])
    // The metric survives — this is the stated rationale for the suppression path.
    assertEquals(run.durationCount, 1L)
  }

  test("a metric for a slow-task-suppressed span carries no exemplar") {
    sys.props("FLARE_TRACE_GRANULARITY") = "all"
    sys.props("FLARE_SLOW_TASK_MS") = "600000"
    val run = runTasks()

    // The span is abandoned unended and never exported. An exemplar naming it would send
    // anyone who clicked it in Grafana to a trace that does not exist.
    assertEquals(run.exemplarTraceIds, Seq.empty[String])
  }

  // ── #53: guards in onTaskStart must not suppress metrics ───────────────────

  test("tasks suppressed by the maxSpansPerTrace circuit breaker are still measured") {
    sys.props("FLARE_TRACE_GRANULARITY")   = "all"
    sys.props("FLARE_MAX_SPANS_PER_TRACE") = "1"
    val run = runTasks(taskCount = 4)

    // The breaker caps spans at 1 ...
    assertEquals(run.exportedTraceIds.size, 1)
    // ... but the task histogram must still see all 4, or the throughput drop caused by the
    // limiter reads as a drop in cluster throughput.
    assertEquals(run.durationCount, 4L)
  }

  test("tasks suppressed by granularity are still measured") {
    // Stages granularity means no task spans at all.
    sys.props("FLARE_TRACE_GRANULARITY") = "stages"
    val run = runTasks(taskCount = 3)

    assertEquals(run.exportedTraceIds, Set.empty[String])
    assertEquals(run.durationCount, 3L)
  }

  test("a failed task is measured even when its span is suppressed") {
    sys.props("FLARE_TRACE_GRANULARITY") = "stages"
    val run = runTasks(taskCount = 2, succeed = false)

    assertEquals(run.exportedTraceIds, Set.empty[String])
    assertEquals(run.durationCount, 2L)
  }

  // ── The invariant, stated once ─────────────────────────────────────────────

  test("every task is measured and every exemplar names an exported span, in every mode") {
    val modes = List(
      Map("FLARE_TRACE_GRANULARITY" -> "all"),
      Map("FLARE_TRACE_GRANULARITY" -> "all", "FLARE_SLOW_TASK_MS" -> "600000"),
      Map("FLARE_TRACE_GRANULARITY" -> "all", "FLARE_MAX_SPANS_PER_TRACE" -> "1"),
      Map("FLARE_TRACE_GRANULARITY" -> "stages"),
    )

    modes.foreach { mode =>
      flareProps.foreach(sys.props.remove)
      mode.foreach { case (k, v) => sys.props(k) = v }
      val run = runTasks(taskCount = 3)
      GlobalOpenTelemetry.resetForTest()

      // Suppressing a span never suppresses its measurement.
      assertEquals(run.durationCount, 3L, s"tasks went unmeasured under $mode")

      val dangling = run.exemplarTraceIds.toSet -- run.exportedTraceIds
      assertEquals(dangling, Set.empty[String], s"dangling exemplars under $mode")
    }
  }
}
