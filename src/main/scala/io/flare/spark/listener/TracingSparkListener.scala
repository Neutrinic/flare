package io.flare.spark.listener

import io.flare.spark.SpanCompat._
import io.flare.spark.attributes.SparkAttributes._
import io.flare.spark.config.FlareConfig
import io.opentelemetry.api.trace.{Span, SpanKind, StatusCode, Tracer}
import io.opentelemetry.context.Context
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{
  SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart,
}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
 * Driver-side SparkListener that creates OTEL spans for application, job, and stage lifecycle events.
 *
 * IMPORTANT: Task spans are NOT created here. Driver-side task events represent when the driver
 * heard about task start/end, not actual executor execution time. Real executor-side task spans
 * are created by FlareExecutorPlugin via ExecutorPlugin.onTaskStart/onTaskEnd.
 *
 * Stage→job attribution uses a reverse index built at onJobStart to correctly handle
 * concurrent jobs (see stageToJob map). Do not use activeJobSpans.headOption.
 */
class TracingSparkListener(
  tracer:  Tracer,
  config:  FlareConfig,
  // When true, safeHandle rethrows exceptions instead of swallowing them.
  // Set to true in tests so assertion failures surface the actual cause.
  private val throwOnError: Boolean = false,
) extends SparkListener {

  private val logger = LoggerFactory.getLogger(classOf[TracingSparkListener])

  // All maps use TrieMap for thread safety (LiveListenerBus may change threading model)
  private val activeJobSpans:   TrieMap[Int, Span]  = TrieMap.empty
  private val activeStageSpans: TrieMap[Int, Span]  = TrieMap.empty
  private val activeSQLSpans:   TrieMap[Long, Span] = TrieMap.empty

  // Reverse index: stageId → jobId, built at onJobStart from jobStart.stageIds
  // Critical for correct parent attribution when multiple jobs run concurrently
  private val stageToJob: TrieMap[Int, Int] = TrieMap.empty

  // Set by whoever creates this listener (TracingSparkPlugin or ByteBuddy hook)
  @volatile private var applicationSpan: Option[Span] = None

  def setApplicationSpan(span: Span): Unit = {
    applicationSpan = Some(span)
    logger.info(s"[Flare] Application span set: ${span.getSpanContext.getTraceId}")
  }

  // ── Application ──────────────────────────────────────────────────────────────

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit =
    logger.info(s"[Flare] Application started: ${event.appName}")

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    applicationSpan.foreach { span =>
      span.setStatus(StatusCode.OK)
      span.end()
      logger.info("[Flare] Application span ended")
    }
  }

  // ── Jobs ─────────────────────────────────────────────────────────────────────

  override def onJobStart(event: SparkListenerJobStart): Unit =
    if (config.tracesJobs) safeHandle("onJobStart") {
      val parentContext = applicationSpan
        .map(Context.current().`with`)
        .getOrElse(Context.current())

      val span = tracer
        .spanBuilder(s"spark.job.${event.jobId}")
        .setSpanKind(SpanKind.INTERNAL)
        .setParent(parentContext)
        .startSpan()

      span.setLong(Job.Id, event.jobId.toLong)
      span.setLong(Job.StageCount, event.stageIds.size.toLong)
      Option(event.properties.getProperty("spark.job.description"))
        .foreach(span.setAttribute(Job.Description, _))

      activeJobSpans.put(event.jobId, span)

      // Build reverse index so stages can find their parent job
      event.stageIds.foreach { stageId =>
        stageToJob.put(stageId, event.jobId)
      }

      logger.debug(s"[Flare] Job ${event.jobId} started, stages: ${event.stageIds.mkString(",")}")
    }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    // Always clean up stageToJob, even if the job span was missed.
    // This prevents unbounded growth if onJobStart was lost due to listener registration timing.
    stageToJob.filterInPlace { case (_, jobId) => jobId != event.jobId }

    activeJobSpans.remove(event.jobId).foreach { span =>
      safeHandle("onJobEnd") {
        import org.apache.spark.scheduler.JobSucceeded
        event.jobResult match {
          case JobSucceeded =>
            span.setStatus(StatusCode.OK)
            span.setAttribute(Job.Result, "SUCCESS")
          case failed =>
            span.setStatus(StatusCode.ERROR, failed.toString)
            span.setAttribute(Job.Result, "FAILED")
            span.setAttribute(Error.Message, failed.toString)
        }
        span.end()
        logger.debug(s"[Flare] Job ${event.jobId} ended")
      }
    }
  }

  // ── Stages ───────────────────────────────────────────────────────────────────

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit =
    if (config.tracesStages) safeHandle("onStageSubmitted") {
      val stageId = event.stageInfo.stageId

      // Correct attribution: look up this stage's job, then that job's span
      val parentSpan: Option[Span] =
        stageToJob.get(stageId).flatMap(activeJobSpans.get).orElse(applicationSpan)

      parentSpan match {
        case None =>
          logger.warn(s"[Flare] No parent span found for stage $stageId, skipping")

        case Some(parent) =>
          val span = tracer
            .spanBuilder(s"spark.stage.${stageId}")
            .setSpanKind(SpanKind.INTERNAL)
            .setParent(Context.current().`with`(parent))
            .startSpan()

          span.setLong(Stage.Id, stageId.toLong)
          span.setLong(Stage.AttemptId, event.stageInfo.attemptNumber().toLong)
          span.setAttribute(Stage.Name, event.stageInfo.name)
          span.setLong(Stage.TaskCount, event.stageInfo.numTasks.toLong)

          activeStageSpans.put(stageId, span)
          logger.debug(s"[Flare] Stage $stageId submitted")
      }
    }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val stageId = event.stageInfo.stageId
    activeStageSpans.remove(stageId).foreach { span =>
      safeHandle("onStageCompleted") {
        // Record task metrics as stage attributes
        Option(event.stageInfo.taskMetrics).foreach { m =>
          span.setLong(Stage.ExecutorRunTime, m.executorRunTime)
          span.setLong(Stage.ExecutorCpuTime, m.executorCpuTime / 1000000L) // ns → ms
          span.setLong(Stage.InputBytes, m.inputMetrics.bytesRead)
          span.setLong(Stage.InputRecords, m.inputMetrics.recordsRead)
          span.setLong(Stage.OutputBytes, m.outputMetrics.bytesWritten)
          span.setLong(Stage.OutputRecords, m.outputMetrics.recordsWritten)
          span.setLong(Stage.ShuffleReadBytes, m.shuffleReadMetrics.totalBytesRead)
          span.setLong(Stage.ShuffleWriteBytes, m.shuffleWriteMetrics.bytesWritten)
          span.setLong(Stage.MemorySpilled, m.memoryBytesSpilled)
        }

        event.stageInfo.failureReason match {
          case Some(reason) =>
            span.setStatus(StatusCode.ERROR, reason)
            span.setAttribute(Stage.FailureReason, reason.take(500)) // cap length
          case None =>
            span.setStatus(StatusCode.OK)
        }

        span.end()
        logger.debug(s"[Flare] Stage $stageId completed")
      }
    }
  }

  // ── SQL ──────────────────────────────────────────────────────────────────────

  override def onOtherEvent(event: SparkListenerEvent): Unit =
    if (config.tracesStages) {
      event match {
        case e: SparkListenerSQLExecutionStart => safeHandle("onSQLStart") {
          applicationSpan.foreach { parent =>
            val span = tracer
              .spanBuilder(s"spark.sql.${e.executionId}")
              .setSpanKind(SpanKind.INTERNAL)
              .setParent(Context.current().`with`(parent))
              .startSpan()
            activeSQLSpans.put(e.executionId, span)
          }
        }
        case e: SparkListenerSQLExecutionEnd => safeHandle("onSQLEnd") {
          activeSQLSpans.remove(e.executionId).foreach { span =>
            span.setStatus(StatusCode.OK)
            span.end()
          }
        }
        case _ =>
      }
    }

  // ── Cleanup ───────────────────────────────────────────────────────────────────

  def shutdown(): Unit = {
    logger.info("[Flare] Shutting down listener, ending any open spans")
    Seq[TrieMap[_, Span]](activeStageSpans, activeJobSpans, activeSQLSpans)
      .foreach { m => m.values.foreach(_.end()); m.clear() }
    applicationSpan.foreach(_.end())
    stageToJob.clear()
  }

  // ── Utilities ─────────────────────────────────────────────────────────────────

  private def safeHandle(eventName: String)(f: => Unit): Unit =
    try f
    catch {
      case ex: Exception =>
        logger.error(s"[Flare] Error handling $eventName: ${ex.getMessage}", ex)
        if (throwOnError) throw ex
    }
}
