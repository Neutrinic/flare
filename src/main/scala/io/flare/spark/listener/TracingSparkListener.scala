package io.flare.spark.listener

import io.flare.spark.SpanCompat._
import io.flare.spark.attributes.SparkAttributes._
import io.flare.spark.config.FlareConfig
import io.flare.spark.instrumentation.SubmitMissingTasksAdviceHelper
import io.flare.spark.metrics.{FlareMetrics, MetricAttributes}
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
  metrics: Option[FlareMetrics] = None,
  // When true, safeHandle rethrows exceptions instead of swallowing them.
  // Set to true in tests so assertion failures surface the actual cause.
  private val throwOnError: Boolean = false,
) extends SparkListener {

  private val logger = LoggerFactory.getLogger(classOf[TracingSparkListener])

  // All maps use TrieMap for thread safety (LiveListenerBus may change threading model)
  private val activeJobSpans:   TrieMap[Int, Span]  = TrieMap.empty
  private val activeStageSpans: TrieMap[Int, Span]  = TrieMap.empty

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
      // Check if SubmitMissingTasksAdvice pre-created a job span for this jobId.
      // The advice hooks DAGScheduler.submitMissingTasks and creates the job span
      // on first call for a given jobId. We adopt (get, don't remove) it here.
      //
      // Race condition: onJobStart fires async on the listener bus. The advice's
      // submitMissingTasks may or may not have run yet. Both sides use putIfAbsent
      // on the shared jobSpans map so only ONE span wins per jobId.
      val span = SubmitMissingTasksAdviceHelper.getJobSpan(event.jobId) match {
        case Some(preCreated) =>
          logger.debug(s"[Flare] Job ${event.jobId} adopted pre-created span")
          preCreated

        case None =>
          // No pre-created span yet. Create one and store it in the helper's map
          // via putIfAbsent. If the advice races us and stores first, we discard ours.
          // Parent under SQL span if this job was triggered by a SQL execution.
          val sqlParent = Option(event.properties)
            .flatMap(p => Option(p.getProperty("spark.sql.execution.id")))
            .flatMap(id => try Some(id.toLong) catch { case _: NumberFormatException => None })
            .flatMap(id => Option(SubmitMissingTasksAdviceHelper.activeSQLSpans.get(id)))

          val parentContext = sqlParent.orElse(applicationSpan)
            .map(Context.current().`with`)
            .getOrElse(Context.current())

          val newSpan = tracer
            .spanBuilder(s"spark.job.${event.jobId}")
            .setSpanKind(SpanKind.INTERNAL)
            .setParent(parentContext)
            .startSpan()

          val existing = SubmitMissingTasksAdviceHelper.jobSpans.putIfAbsent(event.jobId, newSpan)
          if (existing != null) {
            // Advice created a span between our get and putIfAbsent — use theirs
            newSpan.end()
            existing
          } else {
            newSpan
          }
      }

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
    stageToJob.foreach { case (stageId, jId) => if (jId == event.jobId) stageToJob.remove(stageId) }

    // Clean up pre-created job span from the helper's map
    SubmitMissingTasksAdviceHelper.removeJobSpan(event.jobId)

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

      // Check if SubmitMissingTasksAdvice pre-created a stage span
      val span = SubmitMissingTasksAdviceHelper.adoptPendingStageSpan(stageId) match {
        case Some(preCreated) =>
          logger.debug(s"[Flare] Stage $stageId adopted pre-created span")
          preCreated

        case None =>
          // No pre-created span — create as before (backward-compat / SparkPlugin-only)
          val parentSpan: Option[Span] =
            stageToJob.get(stageId).flatMap(activeJobSpans.get).orElse(applicationSpan)

          parentSpan match {
            case None =>
              logger.warn(s"[Flare] No parent span found for stage $stageId, skipping")
              return

            case Some(parent) =>
              tracer
                .spanBuilder(s"spark.stage.$stageId")
                .setSpanKind(SpanKind.INTERNAL)
                .setParent(Context.current().`with`(parent))
                .startSpan()
          }
      }

      span.setLong(Stage.Id, stageId.toLong)
      span.setLong(Stage.AttemptId, event.stageInfo.attemptNumber().toLong)
      span.setAttribute(Stage.Name, event.stageInfo.name)
      span.setLong(Stage.TaskCount, event.stageInfo.numTasks.toLong)

      activeStageSpans.put(stageId, span)
      logger.debug(s"[Flare] Stage $stageId submitted")
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

          // Record OTEL stage-level metrics
          metrics.foreach { fm =>
            val attrs = MetricAttributes.forStage(stageId, event.stageInfo.name)
            fm.stageExecutorRunTime.record(m.executorRunTime.toDouble, attrs)
            val inputBytes = m.inputMetrics.bytesRead
            if (inputBytes > 0) fm.stageInputBytes.add(inputBytes, attrs)
            val outputBytes = m.outputMetrics.bytesWritten
            if (outputBytes > 0) fm.stageOutputBytes.add(outputBytes, attrs)
            val shuffleRead = m.shuffleReadMetrics.totalBytesRead
            if (shuffleRead > 0) fm.stageShuffleReadBytes.add(shuffleRead, attrs)
            val shuffleWrite = m.shuffleWriteMetrics.bytesWritten
            if (shuffleWrite > 0) fm.stageShuffleWriteBytes.add(shuffleWrite, attrs)
          }
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

            // Fields are read by name, never positionally — see describeSqlExecution.
            describeSqlExecution(
              span,
              e.executionId,
              e.description,
              e.details,
              e.physicalPlanDescription,
            )

            // Store in shared map so advice and onJobStart can parent jobs under SQL
            SubmitMissingTasksAdviceHelper.activeSQLSpans.put(e.executionId, span)
          }
        }
        case e: SparkListenerSQLExecutionEnd => safeHandle("onSQLEnd") {
          Option(SubmitMissingTasksAdviceHelper.activeSQLSpans.remove(e.executionId))
            .foreach { span =>
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
    Seq[TrieMap[_, Span]](activeStageSpans, activeJobSpans)
      .foreach { m => m.values.foreach(_.end()); m.clear() }
    // End any open SQL spans from the shared map
    val sqlSpans = SubmitMissingTasksAdviceHelper.activeSQLSpans
    sqlSpans.values().forEach(_.end())
    sqlSpans.clear()
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

  /**
   * Applies SparkListenerSQLExecutionStart metadata to the `spark.sql.N` span.
   *
   * Takes loose values rather than the event itself, deliberately. The event's constructor is not
   * source-compatible across the supported Spark matrix — 3.4 inserted `rootExecutionId` as the
   * second parameter, and 4.0 appended `jobTags` and `jobGroupId`:
   *
   * {{{
   * 3.3: (executionId, description, details, physicalPlanDescription, sparkPlanInfo, time, modifiedConfigs)
   * 3.4: (executionId, rootExecutionId, description, ..., modifiedConfigs)
   * 4.0: (executionId, rootExecutionId, description, ..., modifiedConfigs, jobTags, jobGroupId)
   * }}}
   *
   * Reading fields by name at the single call site compiles everywhere; constructing one does
   * not, so no shared test source set can build a fixture. Keeping the logic here means it stays
   * directly testable without a Spark event at all.
   */
  private[listener] def describeSqlExecution(
    span:                    Span,
    executionId:             Long,
    description:             String,
    details:                 String,
    physicalPlanDescription: String,
  ): Unit = {
    span.setLong(Sql.ExecutionId, executionId)
    // Spark leaves these empty on some execution paths; an empty attribute is pure noise.
    setIfNonEmpty(span, Sql.Description, description, config.sqlDescriptionMaxChars)
    setIfNonEmpty(span, Sql.Details, details, config.sqlDetailsMaxChars)

    val plan = Option(physicalPlanDescription).getOrElse("")
    if (plan.nonEmpty && config.sqlPlanMaxChars > 0) {
      span.setAttribute(Sql.Plan, plan.take(config.sqlPlanMaxChars))
      // Flag truncation explicitly. A silently clipped plan reads exactly like a complete one,
      // which is worse than no plan at all when someone is diagnosing a slow join.
      if (plan.length > config.sqlPlanMaxChars) span.setBool(Sql.PlanTruncated, true)
    }
  }

  /** Sets `key` only when Spark supplied a value and the cap allows it. `maxChars = 0` drops it. */
  private def setIfNonEmpty(
    span:     Span,
    key:      io.opentelemetry.api.common.AttributeKey[String],
    value:    String,
    maxChars: Int,
  ): Unit = {
    val v = Option(value).getOrElse("")
    if (v.nonEmpty && maxChars > 0) span.setAttribute(key, v.take(maxChars))
  }
}
