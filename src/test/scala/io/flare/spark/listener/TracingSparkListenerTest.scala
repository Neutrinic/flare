package io.flare.spark.listener

import io.flare.spark.attributes.SparkAttributes._
import io.flare.spark.config.{FlareConfig, TraceGranularity}
import io.flare.spark.instrumentation.SubmitMissingTasksAdviceHelper
import io.opentelemetry.api.trace.{SpanKind, StatusCode}
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import munit.FunSuite
import org.apache.spark.FlareTestHelpers
import org.apache.spark.scheduler._

import scala.collection.JavaConverters._

class TracingSparkListenerTest extends FunSuite {

  // Test config — all features enabled, no filtering
  val config: FlareConfig = FlareConfig(
    enabled          = true,
    granularity      = TraceGranularity.All,
    samplingRatio    = 1.0,
    maxSpansPerTrace = 10000,
    slowTaskMs       = 0L,
    retryTasksOnly   = false,
    taskStageIds     = Set.empty,
    taskStagePattern = None,
    metricsEnabled   = true,
  )

  /**
   * Test fixture: creates a fresh InMemorySpanExporter, SdkTracerProvider,
   * TracingSparkListener, and a root application span. Calls the body with
   * the listener and exporter. Closes the tracer provider in finally.
   *
   * Each test must call listener.shutdown() when done to flush all open spans
   * into the exporter before asserting.
   */
  def withListener(body: (TracingSparkListener, InMemorySpanExporter) => Unit): Unit = {
    // Clean up shared state from previous tests
    SubmitMissingTasksAdviceHelper.jobSpans.clear()
    SubmitMissingTasksAdviceHelper.pendingStageSpans.clear()
    SubmitMissingTasksAdviceHelper.activeSQLSpans.clear()

    val exporter = InMemorySpanExporter.create()
    val tracerProvider = SdkTracerProvider.builder()
      .addSpanProcessor(SimpleSpanProcessor.create(exporter))
      .build()
    val tracer = tracerProvider.get("io.flare.spark.test")
    val listener = new TracingSparkListener(tracer, config, throwOnError = true)

    val appSpan = tracer.spanBuilder("spark.application")
      .setSpanKind(SpanKind.SERVER)
      .startSpan()
    listener.setApplicationSpan(appSpan)

    try body(listener, exporter)
    finally {
      tracerProvider.close()
      SubmitMissingTasksAdviceHelper.jobSpans.clear()
      SubmitMissingTasksAdviceHelper.pendingStageSpans.clear()
      SubmitMissingTasksAdviceHelper.activeSQLSpans.clear()
    }
  }

  // ── Event helpers using FlareTestHelpers for private[spark] access ──────────

  def makeJobStart(jobId: Int, stageIds: Seq[Int]): SparkListenerJobStart =
    SparkListenerJobStart(
      jobId      = jobId,
      time       = System.currentTimeMillis(),
      stageInfos = stageIds.map(id => FlareTestHelpers.makeStageInfo(id, s"stage-$id")),
      properties = new java.util.Properties(),
    )

  def makeJobEnd(jobId: Int, succeeded: Boolean): SparkListenerJobEnd =
    SparkListenerJobEnd(
      jobId     = jobId,
      time      = System.currentTimeMillis(),
      jobResult = if (succeeded) JobSucceeded else FlareTestHelpers.jobFailed(new RuntimeException("boom")),
    )

  def makeStageSubmitted(stageId: Int): SparkListenerStageSubmitted =
    SparkListenerStageSubmitted(FlareTestHelpers.makeStageInfo(stageId, s"stage-$stageId"))

  def makeStageCompleted(stageId: Int, failed: Boolean = false): SparkListenerStageCompleted = {
    val info = FlareTestHelpers.makeStageInfo(stageId, s"stage-$stageId")
    if (failed) info.failureReason = Some("OOM")
    SparkListenerStageCompleted(info)
  }

  def makeStageCompletedWithMetrics(stageId: Int): SparkListenerStageCompleted = {
    val info = FlareTestHelpers.makeStageInfo(
      stageId, s"stage-$stageId", taskMetrics = FlareTestHelpers.emptyTaskMetrics(),
    )
    SparkListenerStageCompleted(info)
  }

  // ── Tests ───────────────────────────────────────────────────────────────────

  test("job span is created as child of application span") {
    withListener { (listener, exporter) =>
      listener.onJobStart(makeJobStart(0, Seq(0)))
      listener.onJobEnd(makeJobEnd(0, succeeded = true))
      listener.shutdown()

      val spans = exporter.getFinishedSpanItems.asScala
      val appSpan = spans.find(_.getName == "spark.application").get
      val jobSpan = spans.find(_.getName == "spark.job.0").get

      assertEquals(jobSpan.getParentSpanId, appSpan.getSpanId)
      assertEquals(jobSpan.getTraceId, appSpan.getTraceId)
    }
  }

  test("stage span uses correct job parent via reverse index") {
    withListener { (listener, exporter) =>
      // Two concurrent jobs, each with its own stage
      listener.onJobStart(makeJobStart(0, Seq(10)))
      listener.onJobStart(makeJobStart(1, Seq(20)))

      listener.onStageSubmitted(makeStageSubmitted(10))
      listener.onStageSubmitted(makeStageSubmitted(20))

      listener.onStageCompleted(makeStageCompleted(10))
      listener.onStageCompleted(makeStageCompleted(20))
      listener.onJobEnd(makeJobEnd(0, succeeded = true))
      listener.onJobEnd(makeJobEnd(1, succeeded = true))
      listener.shutdown()

      val spans = exporter.getFinishedSpanItems.asScala
      val job0  = spans.find(_.getName == "spark.job.0").get
      val job1  = spans.find(_.getName == "spark.job.1").get
      val stg10 = spans.find(_.getName == "spark.stage.10").get
      val stg20 = spans.find(_.getName == "spark.stage.20").get

      // Stage 10 must be child of job 0
      assertEquals(stg10.getParentSpanId, job0.getSpanId)
      // Stage 20 must be child of job 1 — NOT job 0
      assertEquals(stg20.getParentSpanId, job1.getSpanId)
    }
  }

  test("stage span parent falls back to application span when job has ended") {
    withListener { (listener, exporter) =>
      // Start and end a job, then submit a stage whose job is gone
      listener.onJobStart(makeJobStart(0, Seq(5)))
      listener.onJobEnd(makeJobEnd(0, succeeded = true))

      // Stage 5's job (0) has ended — stageToJob entry was cleaned up.
      // The stage should fall back to the application span as parent.
      listener.onStageSubmitted(makeStageSubmitted(5))
      listener.onStageCompleted(makeStageCompleted(5))
      listener.shutdown()

      val spans   = exporter.getFinishedSpanItems.asScala
      val appSpan = spans.find(_.getName == "spark.application").get
      val stg5    = spans.find(_.getName == "spark.stage.5").get

      assertEquals(stg5.getParentSpanId, appSpan.getSpanId)
    }
  }

  test("job span records FAILED status on JobFailed result") {
    withListener { (listener, exporter) =>
      listener.onJobStart(makeJobStart(0, Seq(0)))
      listener.onJobEnd(makeJobEnd(0, succeeded = false))
      listener.shutdown()

      val spans   = exporter.getFinishedSpanItems.asScala
      val jobSpan = spans.find(_.getName == "spark.job.0").get

      assertEquals(jobSpan.getStatus.getStatusCode, StatusCode.ERROR)
      assertEquals(
        jobSpan.getAttributes.get(Job.Result),
        "FAILED",
      )
    }
  }

  test("stage span records task metrics as attributes on completion") {
    withListener { (listener, exporter) =>
      listener.onJobStart(makeJobStart(0, Seq(0)))
      listener.onStageSubmitted(makeStageSubmitted(0))

      // Complete with a TaskMetrics-bearing StageInfo (metrics are zero but attributes must exist)
      listener.onStageCompleted(makeStageCompletedWithMetrics(0))
      listener.onJobEnd(makeJobEnd(0, succeeded = true))
      listener.shutdown()

      val spans    = exporter.getFinishedSpanItems.asScala
      val stgSpan  = spans.find(_.getName == "spark.stage.0").get
      val attrs    = stgSpan.getAttributes

      // Verify metrics attributes are present (values are 0 from a fresh TaskMetrics)
      assert(attrs.get(Stage.ExecutorRunTime) != null, "executorRunTime attribute missing")
      assert(attrs.get(Stage.ExecutorCpuTime) != null, "executorCpuTime attribute missing")
      assert(attrs.get(Stage.InputBytes) != null, "inputBytes attribute missing")
      assert(attrs.get(Stage.OutputBytes) != null, "outputBytes attribute missing")
      assert(attrs.get(Stage.ShuffleReadBytes) != null, "shuffleReadBytes attribute missing")
      assert(attrs.get(Stage.ShuffleWriteBytes) != null, "shuffleWriteBytes attribute missing")
    }
  }

  test("stage span records failure reason on failed stage") {
    withListener { (listener, exporter) =>
      listener.onJobStart(makeJobStart(0, Seq(0)))
      listener.onStageSubmitted(makeStageSubmitted(0))
      listener.onStageCompleted(makeStageCompleted(0, failed = true))
      listener.onJobEnd(makeJobEnd(0, succeeded = true))
      listener.shutdown()

      val spans   = exporter.getFinishedSpanItems.asScala
      val stgSpan = spans.find(_.getName == "spark.stage.0").get

      assertEquals(stgSpan.getStatus.getStatusCode, StatusCode.ERROR)
      assertEquals(stgSpan.getAttributes.get(Stage.FailureReason), "OOM")
    }
  }

  test("shutdown ends all open spans without throwing") {
    withListener { (listener, exporter) =>
      // Start a job and stage, do NOT end them
      listener.onJobStart(makeJobStart(0, Seq(0)))
      listener.onStageSubmitted(makeStageSubmitted(0))

      // Shutdown should end all open spans without throwing
      listener.shutdown()

      val spans = exporter.getFinishedSpanItems.asScala

      // Application, job, and stage spans should all be present (ended by shutdown)
      assert(spans.exists(_.getName == "spark.application"), "application span missing")
      assert(spans.exists(_.getName == "spark.job.0"), "job span missing")
      assert(spans.exists(_.getName == "spark.stage.0"), "stage span missing")
    }
  }

  test("onJobStart adopts pre-created span from SubmitMissingTasksAdviceHelper") {
    withListener { (listener, exporter) =>
      // Simulate what SubmitMissingTasksAdvice does: pre-create a job span
      // and register it by jobId.
      val tracer = io.opentelemetry.sdk.trace.SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
        .build().get("io.flare.spark.test")

      val preCreatedSpan = tracer.spanBuilder("spark.job.42")
        .setSpanKind(SpanKind.INTERNAL)
        .startSpan()

      SubmitMissingTasksAdviceHelper.jobSpans.put(42, preCreatedSpan)

      listener.onJobStart(makeJobStart(42, Seq(0)))
      listener.onJobEnd(makeJobEnd(42, succeeded = true))
      listener.shutdown()

      val spans = exporter.getFinishedSpanItems.asScala

      // The adopted span should appear as "spark.job.42"
      assert(spans.exists(_.getName == "spark.job.42"), "Adopted span should be present")

      // The helper's map should be cleaned up after onJobEnd
      assert(!SubmitMissingTasksAdviceHelper.jobSpans.containsKey(42))

      // Clean up
      SubmitMissingTasksAdviceHelper.jobSpans.clear()
    }
  }

  test("onStageSubmitted adopts pre-created span from SubmitMissingTasksAdviceHelper") {
    withListener { (listener, exporter) =>
      val tracer = io.opentelemetry.sdk.trace.SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
        .build().get("io.flare.spark.test")

      // Pre-create a stage span (as the advice would do)
      val preCreatedStage = tracer.spanBuilder("spark.stage.10")
        .setSpanKind(SpanKind.INTERNAL)
        .startSpan()

      SubmitMissingTasksAdviceHelper.pendingStageSpans.put(10, preCreatedStage)

      listener.onJobStart(makeJobStart(0, Seq(10)))
      listener.onStageSubmitted(makeStageSubmitted(10))
      listener.onStageCompleted(makeStageCompleted(10))
      listener.onJobEnd(makeJobEnd(0, succeeded = true))
      listener.shutdown()

      val spans = exporter.getFinishedSpanItems.asScala

      // The adopted stage span should appear
      assert(spans.exists(_.getName == "spark.stage.10"), "Adopted stage span should be present")

      // The pending map should be empty
      assert(!SubmitMissingTasksAdviceHelper.pendingStageSpans.containsKey(10))

      // Clean up
      SubmitMissingTasksAdviceHelper.pendingStageSpans.clear()
    }
  }

  test("onJobStart falls back to creating span when no pre-created span exists") {
    withListener { (listener, exporter) =>
      // No pre-created span — should create span normally (backward compat)
      // The listener now stores the created span in the helper's jobSpans map
      // via putIfAbsent so the advice can find it if it races.
      listener.onJobStart(makeJobStart(0, Seq(0)))

      // Verify the span was stored in the helper's map
      assert(SubmitMissingTasksAdviceHelper.jobSpans.containsKey(0),
        "Fallback span should be stored in helper's jobSpans map")

      listener.onJobEnd(makeJobEnd(0, succeeded = true))
      listener.shutdown()

      val spans = exporter.getFinishedSpanItems.asScala
      val appSpan = spans.find(_.getName == "spark.application").get
      val jobSpan = spans.find(_.getName == "spark.job.0").get

      assertEquals(jobSpan.getParentSpanId, appSpan.getSpanId)

      // onJobEnd should have cleaned up the helper's map
      assert(!SubmitMissingTasksAdviceHelper.jobSpans.containsKey(0))

      // Clean up
      SubmitMissingTasksAdviceHelper.jobSpans.clear()
    }
  }

  test("SQL-triggered job is parented under SQL span") {
    withListener { (listener, exporter) =>
      // Simulate what the listener's onOtherEvent does for SQL start:
      // create a SQL span and put it in the shared helper map.
      val tracer = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
        .build().get("io.flare.spark.test")
      val sqlSpan = tracer.spanBuilder("spark.sql.0")
        .setSpanKind(SpanKind.INTERNAL)
        .startSpan()
      SubmitMissingTasksAdviceHelper.activeSQLSpans.put(0L, sqlSpan)

      // Start a job with spark.sql.execution.id in its properties
      val jobEvent = makeJobStart(0, Seq(0))
      jobEvent.properties.setProperty("spark.sql.execution.id", "0")
      listener.onJobStart(jobEvent)

      listener.onStageSubmitted(makeStageSubmitted(0))
      listener.onStageCompleted(makeStageCompleted(0))
      listener.onJobEnd(makeJobEnd(0, succeeded = true))

      // End the SQL span
      sqlSpan.end()
      SubmitMissingTasksAdviceHelper.activeSQLSpans.remove(0L)

      listener.shutdown()

      val spans = exporter.getFinishedSpanItems.asScala
      val sqlSpanData = spans.find(_.getName == "spark.sql.0").get
      val jobSpanData = spans.find(_.getName == "spark.job.0").get
      val stgSpan = spans.find(_.getName == "spark.stage.0").get

      // Job is child of SQL (not app)
      assertEquals(jobSpanData.getParentSpanId, sqlSpanData.getSpanId)
      // Stage is child of job
      assertEquals(stgSpan.getParentSpanId, jobSpanData.getSpanId)
    }
  }

  test("non-SQL job is parented under app span") {
    withListener { (listener, exporter) =>
      // Job without spark.sql.execution.id — should be child of app
      listener.onJobStart(makeJobStart(0, Seq(0)))
      listener.onJobEnd(makeJobEnd(0, succeeded = true))
      listener.shutdown()

      val spans = exporter.getFinishedSpanItems.asScala
      val appSpan = spans.find(_.getName == "spark.application").get
      val jobSpan = spans.find(_.getName == "spark.job.0").get

      assertEquals(jobSpan.getParentSpanId, appSpan.getSpanId)
    }
  }

  test("concurrent jobs do not interfere with stage attribution") {
    withListener { (listener, exporter) =>
      // Three concurrent jobs with interleaved stage submissions
      listener.onJobStart(makeJobStart(0, Seq(100, 101)))
      listener.onJobStart(makeJobStart(1, Seq(200)))
      listener.onJobStart(makeJobStart(2, Seq(300, 301)))

      // Stages submitted in non-sequential order
      listener.onStageSubmitted(makeStageSubmitted(200))
      listener.onStageSubmitted(makeStageSubmitted(100))
      listener.onStageSubmitted(makeStageSubmitted(301))
      listener.onStageSubmitted(makeStageSubmitted(101))
      listener.onStageSubmitted(makeStageSubmitted(300))

      // Complete all stages and jobs
      Seq(100, 101, 200, 300, 301).foreach(id => listener.onStageCompleted(makeStageCompleted(id)))
      Seq(0, 1, 2).foreach(id => listener.onJobEnd(makeJobEnd(id, succeeded = true)))
      listener.shutdown()

      val spans  = exporter.getFinishedSpanItems.asScala
      val job0   = spans.find(_.getName == "spark.job.0").get
      val job1   = spans.find(_.getName == "spark.job.1").get
      val job2   = spans.find(_.getName == "spark.job.2").get

      val stg100 = spans.find(_.getName == "spark.stage.100").get
      val stg101 = spans.find(_.getName == "spark.stage.101").get
      val stg200 = spans.find(_.getName == "spark.stage.200").get
      val stg300 = spans.find(_.getName == "spark.stage.300").get
      val stg301 = spans.find(_.getName == "spark.stage.301").get

      // Job 0 stages
      assertEquals(stg100.getParentSpanId, job0.getSpanId)
      assertEquals(stg101.getParentSpanId, job0.getSpanId)
      // Job 1 stage
      assertEquals(stg200.getParentSpanId, job1.getSpanId)
      // Job 2 stages
      assertEquals(stg300.getParentSpanId, job2.getSpanId)
      assertEquals(stg301.getParentSpanId, job2.getSpanId)

      // Clean up
      SubmitMissingTasksAdviceHelper.jobSpans.clear()
    }
  }
}
