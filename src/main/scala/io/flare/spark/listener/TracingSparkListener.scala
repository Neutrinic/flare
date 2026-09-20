package io.flare.spark.listener

import io.flare.spark.SpanCompat._
import io.flare.spark.attributes.{FailureDetail, PlanFingerprint}
import io.flare.spark.attributes.SparkAttributes._
import io.flare.spark.config.FlareConfig
import io.flare.spark.instrumentation.SubmitMissingTasksAdviceHelper
import io.flare.spark.metrics.{FlareMetrics, MetricAttributes}
import io.opentelemetry.api.trace.{Span, SpanKind, StatusCode, Tracer}
import io.opentelemetry.context.Context
import org.apache.spark.FlareJobResultAccess
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{
  SparkListenerSQLAdaptiveExecutionUpdate,
  SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart,
}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicLong
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

  // stageId → SQL execution id, and that execution's description. Both built at onJobStart,
  // which is the only event carrying the spark.sql.execution.id property alongside the stage
  // list. Lets a stage span name the user code that a Spark-generated stage name cannot —
  // see #48 and Stage.SqlDescription.
  private val stageToSql:      TrieMap[Int, Long]    = TrieMap.empty
  private val sqlDescriptions: TrieMap[Long, String] = TrieMap.empty

  // stageId → summed scheduler delay across the stage's tasks, accumulated at onTaskEnd.
  // Unlike every other stage attribute this one is not on StageInfo.taskMetrics: deriving it
  // needs each task's wall clock, which only SparkListenerTaskEnd carries. One Long per
  // in-flight stage.
  private val stageSchedulerDelayMs: TrieMap[Int, AtomicLong] = TrieMap.empty

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
      // Adopts the advice's span when submitMissingTasks got here first, and creates one
      // otherwise. The builder runs at most once per jobId, so losing this race costs
      // nothing rather than publishing an empty duplicate span — see #104.
      val span = SubmitMissingTasksAdviceHelper.getOrCreateJobSpan(event.jobId) {
        // Parent under SQL span if this job was triggered by a SQL execution.
        val sqlParent = sqlExecutionIdOf(event.properties)
          .flatMap(id => Option(SubmitMissingTasksAdviceHelper.activeSQLSpans.get(id)))

        val parentContext = sqlParent.orElse(applicationSpan)
          .map(Context.current().`with`)
          .getOrElse(Context.current())

        tracer
          .spanBuilder(s"spark.job.${event.jobId}")
          .setSpanKind(SpanKind.INTERNAL)
          .setParent(parentContext)
          .startSpan()
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

      // Same for the SQL execution, when this job belongs to one. Read from the job's
      // properties rather than the span, because the description is what stages need and
      // the span does not carry it back.
      sqlExecutionIdOf(event.properties).foreach { execId =>
        event.stageIds.foreach(stageId => stageToSql.put(stageId, execId))
      }

      logger.debug(s"[Flare] Job ${event.jobId} started, stages: ${event.stageIds.mkString(",")}")
    }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    // Always clean up stageToJob, even if the job span was missed.
    // This prevents unbounded growth if onJobStart was lost due to listener registration timing.
    // stageToSql is keyed the same way and is dropped with it, so the two cannot diverge.
    stageToJob.foreach { case (stageId, jId) =>
      if (jId == event.jobId) {
        stageToJob.remove(stageId)
        stageToSql.remove(stageId)
      }
    }

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
            span.setAttribute(Job.Result, "FAILED")
            // The job is the one place Spark hands us a live Throwable, so it is the one place
            // the span can carry a real stack trace. Falling back to toString keeps a job whose
            // result is not a JobFailed from losing its failure message entirely.
            val detail = FlareJobResultAccess
              .failureException(failed)
              .map(FailureDetail.fromThrowable)
              .getOrElse(FailureDetail.fromReasonString(failed.toString))
            FailureDetail.record(span, detail)
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

      // Spark's own stage name is kept above exactly as-is, so anything correlating with the
      // Spark UI still matches. These are additive: for an async subquery or broadcast stage,
      // stageInfo.name resolves inside a Spark thread pool and names nothing useful, and
      // stageInfo.details cannot rescue it because that stack was captured on the pool thread
      // and holds no user frame at all. The SQL execution does name the user code.
      stageToSql.get(stageId).foreach { execId =>
        span.setLong(Stage.SqlExecutionId, execId)
        sqlDescriptionOf(stageId).foreach { desc =>
          setIfNonEmpty(span, Stage.SqlDescription, desc, config.sqlDescriptionMaxChars)
        }
      }

      activeStageSpans.put(stageId, span)
      logger.debug(s"[Flare] Stage $stageId submitted")
    }

  /**
   * Accumulates scheduler delay. No task span is created here — see the class comment.
   *
   * Scheduler delay is the one part of the stage breakdown Spark does not report. It has to be
   * derived per task, because it needs the task's wall clock, which lives on TaskInfo rather
   * than TaskMetrics.
   */
  override def onTaskEnd(event: SparkListenerTaskEnd): Unit =
    if (config.tracesStages) safeHandle("onTaskEnd") {
      val info = event.taskInfo
      // taskMetrics is null when a task failed before it could report any, and duration throws
      // outright on a task that never finished. Neither yields a delay worth deriving.
      if (info != null && info.finished && event.taskMetrics != null) {
        val m = event.taskMetrics
        // TaskInfo.gettingResultTime is the instant the driver started fetching an indirect
        // result, not an elapsed time, and stays 0 when the result came back inline.
        val gettingResultMs =
          if (info.gettingResultTime > 0) math.max(0L, info.finishTime - info.gettingResultTime)
          else 0L
        val delay = schedulerDelayMs(
          durationMs                = info.duration,
          executorRunTimeMs         = m.executorRunTime,
          deserializeTimeMs         = m.executorDeserializeTime,
          resultSerializationTimeMs = m.resultSerializationTime,
          gettingResultTimeMs       = gettingResultMs,
        )
        stageSchedulerDelayMs.getOrElseUpdate(event.stageId, new AtomicLong()).addAndGet(delay)
      }
    }

  /**
   * The part of a task's wall clock that was not spent doing the task.
   *
   * Everything Spark measures about a task — deserializing it, running it, serializing the
   * result, shipping that result back — is subtracted from the wall clock the driver observed.
   * What remains is queueing: waiting for an executor slot, and the scheduler round trip.
   *
   * Clamped at zero per task, deliberately, and never on the sum. The driver's clock and the
   * executor's are different clocks, so a fast task can report a run time a few ms longer than
   * its own duration. Summing first would let those negatives cancel real delay elsewhere in
   * the stage and quietly understate it.
   */
  private[listener] def schedulerDelayMs(
    durationMs:                Long,
    executorRunTimeMs:         Long,
    deserializeTimeMs:         Long,
    resultSerializationTimeMs: Long,
    gettingResultTimeMs:       Long,
  ): Long =
    math.max(
      0L,
      durationMs - executorRunTimeMs - deserializeTimeMs - resultSerializationTimeMs - gettingResultTimeMs,
    )

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val stageId = event.stageInfo.stageId
    // Removed unconditionally: a stage whose span was never created still accumulated delay,
    // and this is the only event that tells us the stage is over.
    val schedulerDelay = stageSchedulerDelayMs.remove(stageId)
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
          span.setLong(Stage.DiskSpilled, m.diskBytesSpilled)

          // The breakdown of a slow stage. Total duration says a stage was slow; these say why.
          span.setLong(Stage.JvmGcTime, m.jvmGCTime)
          span.setLong(Stage.ExecutorDeserializeTime, m.executorDeserializeTime)
          span.setLong(Stage.ExecutorDeserializeCpuTime, m.executorDeserializeCpuTime / 1000000L) // ns → ms
          span.setLong(Stage.ResultSerializationTime, m.resultSerializationTime)

          // Record OTEL stage-level metrics
          metrics.foreach { fm =>
            // Same lookup the span does at onStageSubmitted. Safe here because a stage always
            // completes before its job ends, and onJobEnd is what drops stageToSql.
            val attrs =
              MetricAttributes.forStage(stageId, event.stageInfo.name, sqlDescriptionOf(stageId))
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

        // Only set when task ends were actually observed. A stage that reported none would
        // otherwise export a zero, which reads as "no queueing" rather than "not measured".
        schedulerDelay.foreach(d => span.setLong(Stage.SchedulerDelay, d.get()))

        event.stageInfo.failureReason match {
          case Some(reason) =>
            // Spark only ever gives the stage a formatted string here — no Throwable, no
            // structured fields — so error.type is recovered by pattern and left unset when
            // nothing matches. Task spans carry the structured version of the same failure.
            FailureDetail.record(span, FailureDetail.fromReasonString(reason))
            span.setAttribute(Stage.FailureReason, reason.take(500)) // cap length
          case None =>
            span.setStatus(StatusCode.OK)
        }

        span.end()
        logger.debug(s"[Flare] Stage $stageId completed")
      }
    }
  }

  // ── Cluster lifecycle (#49) ──────────────────────────────────────────────────
  //
  // Metrics only, never spans. An executor's lifetime is a level over time, not an operation
  // a trace should describe — an executor alive for the whole application would be a span
  // longer than every trace it overlaps.
  //
  // All of these are cheap and low-cardinality except onBlockUpdated, which is gated.

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit =
    safeHandle("onExecutorAdded") {
      metrics.foreach(_.executorCount.add(1L, MetricAttributes.forExecutor(event.executorId)))
    }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit =
    safeHandle("onExecutorRemoved") {
      metrics.foreach { fm =>
        fm.executorCount.add(-1L, MetricAttributes.forExecutor(event.executorId))
        // The reason is the point: it separates a routine dynamic-allocation scale-down from
        // a crash, which is otherwise indistinguishable in the executor count alone.
        fm.executorRemoved.add(1L, MetricAttributes.forExecutorRemoval(event.executorId, event.reason))
      }
    }

  // Spark posts SparkListenerExecutorExcluded since 3.1; the older Blacklisted event and its
  // callback still exist on every version in the matrix but are not what the health tracker
  // emits, so overriding only this one avoids double counting.
  override def onExecutorExcluded(event: SparkListenerExecutorExcluded): Unit =
    safeHandle("onExecutorExcluded") {
      metrics.foreach(_.executorExcluded.add(1L, MetricAttributes.forExecutor(event.executorId)))
    }

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit =
    safeHandle("onBlockManagerAdded") {
      metrics.foreach(_.blockManagerCount.add(1L,
        MetricAttributes.forExecutor(event.blockManagerId.executorId)))
    }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit =
    safeHandle("onBlockManagerRemoved") {
      metrics.foreach(_.blockManagerCount.add(-1L,
        MetricAttributes.forExecutor(event.blockManagerId.executorId)))
    }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit =
    safeHandle("onUnpersistRDD") {
      // No rdd.id tag — an application can create unboundedly many RDDs, and this counter
      // is alive for the whole application rather than per query.
      metrics.foreach(_.rddUnpersisted.add(1L))
    }

  /**
   * Running storage totals, off unless FLARE_TRACK_BLOCK_UPDATES=true.
   *
   * This fires once per block. On a large cached dataset that is a firehose on the listener
   * bus thread, which is why it is opt-in rather than on by default.
   *
   * Spark signals a block being dropped by sending an invalid StorageLevel with the sizes it
   * had, so removal is a negative delta of those sizes rather than a separate event.
   */
  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit =
    if (config.trackBlockUpdates) safeHandle("onBlockUpdated") {
      metrics.foreach { fm =>
        val info  = event.blockUpdatedInfo
        val attrs = MetricAttributes.forExecutor(info.blockManagerId.executorId)
        val live  = info.storageLevel.isValid
        val sign  = if (live) 1L else -1L

        if (info.memSize  != 0L) fm.storageMemoryBytes.add(sign * info.memSize, attrs)
        if (info.diskSize != 0L) fm.storageDiskBytes.add(sign * info.diskSize, attrs)
        fm.storageBlocks.add(sign, attrs)
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
        // AQE re-plans after execution starts, so the tree captured at start is provisional.
        // Every update carries the current plan; the last one to arrive is what ran.
        case e: SparkListenerSQLAdaptiveExecutionUpdate => safeHandle("onSQLAdaptiveUpdate") {
          Option(SubmitMissingTasksAdviceHelper.activeSQLSpans.get(e.executionId))
            .foreach(span => updateSqlPlan(span, e.physicalPlanDescription))
        }
        case e: SparkListenerSQLExecutionEnd => safeHandle("onSQLEnd") {
          Option(SubmitMissingTasksAdviceHelper.activeSQLSpans.remove(e.executionId))
            .foreach { span =>
              span.setStatus(StatusCode.OK)
              span.end()
            }
          // Removed unconditionally: an execution whose span was never created still recorded
          // a description, and this is the only event that says the execution is over.
          sqlDescriptions.remove(e.executionId)
        }
        case _ =>
      }
    }

  /**
   * The SQL execution id a job belongs to, if any.
   *
   * `spark.sql.execution.id` is a local property Spark sets on the job's properties. It is
   * absent for a pure-RDD job, and a non-numeric value would be a Spark bug rather than
   * something to propagate, so both fall back to None.
   */
  /**
   * The SQL execution description for a stage, if it belongs to one.
   *
   * Shared by the stage span (#48) and the stage metrics (#75) so the two surfaces can never
   * disagree about what a stage is called. None for a pure-RDD stage, which has no description
   * to report rather than an empty one.
   */
  private def sqlDescriptionOf(stageId: Int): Option[String] =
    stageToSql.get(stageId).flatMap(sqlDescriptions.get)

  private def sqlExecutionIdOf(properties: java.util.Properties): Option[Long] =
    Option(properties)
      .flatMap(p => Option(p.getProperty("spark.sql.execution.id")))
      .flatMap(id => try Some(id.toLong) catch { case _: NumberFormatException => None })

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
    stageToSql.clear()
    sqlDescriptions.clear()
    stageSchedulerDelayMs.clear()
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

    // Also retained off-span so stage spans can carry it (#48). It has to be kept separately
    // because an OTEL Span is write-only — an attribute cannot be read back off one. Stored
    // here rather than at the event, so the single place that knows both id and description
    // owns it, and so tests can reach it without constructing a SQL event whose constructor
    // is not source-compatible across the matrix.
    Option(description).filter(_.nonEmpty).foreach(sqlDescriptions.put(executionId, _))
    setIfNonEmpty(span, Sql.Details, details, config.sqlDetailsMaxChars)

    // The plan is provisional at this point — AQE has not run. If it re-plans,
    // SparkListenerSQLAdaptiveExecutionUpdate overwrites Sql.Plan with the tree that ran.
    // If it never fires, this plan is final and the value stands.
    setPlan(span, Sql.Plan, Sql.PlanTruncated, physicalPlanDescription, config.sqlPlanMaxChars)

    // Fingerprints hash the FULL plan and ignore the character caps entirely, so the same query
    // groups identically across deployments configured differently — and still groups at all
    // when the caps are 0 and no plan text is exported.
    //
    // Both keys get the same value here because at execution start the plan IS the initial plan.
    // If AQE re-plans, updateSqlPlan moves Sql.PlanFingerprint on and this one stays put, so the
    // pair ends up as "shape as planned" vs "shape as executed".
    //
    // Do NOT read the two differing as "AQE made an optimisation": under AQE the final tree
    // always gains == Final Plan == and QueryStage wrappers, so they differ for essentially
    // every AQE-enabled query. Measured in the dev stack — all three PipelineJob executions
    // differed. The initial fingerprint is the more stable grouping key of the two, since it
    // predates both the query stages and their runtime statistics.
    //
    // Deliberately not gated on sqlPlanInitialMaxChars: that flag controls the initial plan TEXT,
    // and the whole point here is getting the grouping key without the payload.
    PlanFingerprint.of(physicalPlanDescription).foreach { fp =>
      span.setAttribute(Sql.PlanFingerprint, fp)
      span.setAttribute(Sql.PlanInitialFingerprint, fp)
    }

    // Retained separately so the AQE decision survives the overwrite. Off unless configured.
    setPlan(
      span,
      Sql.PlanInitial,
      Sql.PlanInitialTruncated,
      physicalPlanDescription,
      config.sqlPlanInitialMaxChars,
    )
  }

  /**
   * Replaces `spark.sql.plan` with a plan produced by AQE.
   *
   * Called once per SparkListenerSQLAdaptiveExecutionUpdate. Several may arrive for one
   * execution; each overwrites the last, so only the final state is retained and the cost is
   * bounded regardless of how many times AQE re-plans.
   */
  private[listener] def updateSqlPlan(span: Span, physicalPlanDescription: String): Unit = {
    setPlan(
      span,
      Sql.Plan,
      Sql.PlanTruncated,
      physicalPlanDescription,
      config.sqlPlanMaxChars,
      // The previous plan may have set the truncation flag. Attributes cannot be removed, so an
      // overwrite that no longer truncates has to say so explicitly or the stale `true` stands.
      clearTruncationFlag = true,
    )

    // Overwrites the running fingerprint, leaving Sql.PlanInitialFingerprint on the pre-AQE tree.
    // The two differing is then a cheap boolean for "AQE re-planned this query", without holding
    // or diffing two large plan strings.
    PlanFingerprint.of(physicalPlanDescription)
      .foreach(span.setAttribute(Sql.PlanFingerprint, _))
  }

  /** Sets a plan attribute and its truncation flag, honouring `maxChars` (0 drops both). */
  private def setPlan(
    span:                Span,
    key:                 io.opentelemetry.api.common.AttributeKey[String],
    truncatedKey:        io.opentelemetry.api.common.AttributeKey[java.lang.Boolean],
    value:               String,
    maxChars:            Int,
    clearTruncationFlag: Boolean = false,
  ): Unit = {
    val plan = Option(value).getOrElse("")
    if (plan.nonEmpty && maxChars > 0) {
      span.setAttribute(key, plan.take(maxChars))
      // Flag truncation explicitly. A silently clipped plan reads exactly like a complete one,
      // which is worse than no plan at all when someone is diagnosing a slow join.
      val truncated = plan.length > maxChars
      if (truncated) span.setBool(truncatedKey, true)
      else if (clearTruncationFlag) span.setBool(truncatedKey, false)
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
