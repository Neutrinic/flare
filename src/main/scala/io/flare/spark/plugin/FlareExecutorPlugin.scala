package io.flare.spark.plugin

import io.flare.spark.BuildInfo
import io.flare.spark.SpanCompat._
import io.flare.spark.attributes.FailureDetail
import io.flare.spark.attributes.SparkAttributes._
import io.flare.spark.config.{FlareConfig, TraceGranularity}
import io.flare.spark.metrics.{FlareMetrics, MetricAttributes}
import io.flare.spark.propagation.LocalPropertyPropagator
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, SpanKind, StatusCode}
import io.opentelemetry.context.Scope
import org.apache.spark.TaskContext
import org.apache.spark.{TaskFailedReason, TaskKilled}
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.slf4j.LoggerFactory

import java.{util => ju}
import java.util.concurrent.atomic.AtomicLong

/**
 * Executor-side plugin that restores OTEL context from Spark local properties
 * and creates real executor-side task spans with the correct parent.
 *
 * v0.2 additions:
 * - Stage-level task filtering via FLARE_TASK_STAGES / FLARE_TASK_STAGE_PATTERN
 * - Slow task filter via FLARE_SLOW_TASK_MS (drop spans for fast tasks)
 * - Retry-only filter via FLARE_RETRY_TASKS_ONLY
 * - Task span metrics: shuffle bytes, peak memory, SQL execution ID
 * - Log MDC enrichment: trace_id/span_id injected into Log4j 2 ThreadContext
 * - Shutdown flush: force-flush TracerProvider on executor shutdown (K8s SIGTERM)
 */
class FlareExecutorPlugin extends ExecutorPlugin {

  private val logger = LoggerFactory.getLogger(classOf[FlareExecutorPlugin])

  // Loaded once in init(), then only read — no volatile needed since Spark guarantees
  // init() completes before any task callbacks fire on this executor.
  private var config: FlareConfig = _
  private var metrics: FlareMetrics = _
  private var executorId: String = "unknown"

  // Counts spans created on this executor JVM across all tasks.
  private val spanCount = new AtomicLong(0L)

  // Set to true once the maxSpansPerTrace warning has been logged, to avoid log spam
  @volatile private var maxSpansWarned = false

  // Span state for the current task on this thread. None means a filter suppressed the span.
  private val taskState = new ThreadLocal[Option[(Span, Option[Scope])]]()

  // Metric state for the current task on this thread: the captured TaskContext and the start
  // timestamp. Deliberately independent of taskState — metrics are pre-aggregated, so the
  // cardinality pressure that justifies dropping spans does not apply to them, and a task whose
  // span is suppressed must still be measured.
  //
  // TaskContext is captured at onTaskStart because Spark 4.0 clears TaskContext.get() before
  // onTaskSucceeded/onTaskFailed fires, so we can't rely on it in endTask.
  private val taskMetricState = new ThreadLocal[Option[(TaskContext, Long)]]()

  // Flushes once no task has ended for a second (#122). Some clusters kill executors without a
  // shutdown call, Databricks job clusters among them, and the metric reader only exports every
  // 60s, so task metrics from the last minute of a run were lost. A second after the last task is
  // the last point Flare can act.
  //
  // Executors also go quiet between the stages of every query, and flushed every few seconds, each
  // flush re-sending every metric series (#139). Within IdleFlushIntervalMs of a flush, the next
  // needs IdleFlushThrottledQuietMs of quiet, longer than most gaps between stages.
  private val idleFlush = new QuietPeriodAction(
    1000L, () => TelemetryFlush.flush("executor idle"), "flare-executor-idle-flush",
    minIntervalMs = FlareExecutorPlugin.IdleFlushIntervalMs,
    throttledDelayMs = FlareExecutorPlugin.IdleFlushThrottledQuietMs,
  )

  override def init(ctx: PluginContext, extraConf: ju.Map[String, String]): Unit = {
    FlareConfig.warnIfSamplingRatioSet(w => logger.warn(w))

    config = try FlareConfig.load() catch {
      case e: IllegalArgumentException =>
        logger.error(s"[Flare] Configuration error — executor task spans disabled: ${e.getMessage}")
        FlareConfig(enabled = false, granularity = TraceGranularity.Stages,
          maxSpansPerTrace = 0, slowTaskMs = 0L,
          retryTasksOnly = false, taskStageIds = Set.empty, taskStagePattern = None,
          metricsEnabled = false)
    }
    // FLARE_ENABLED=false is the kill switch for everything Flare emits, metrics included (#176).
    metrics = FlareMetrics.create(config.enabled && config.metricsEnabled)
    executorId = ctx.executorID()
    logger.info(s"[Flare] Executor plugin initialized (executorId=$executorId, granularity=${config.granularity}, " +
      s"maxSpans=${config.maxSpansPerTrace}, " +
      s"taskTracing=${config.tracesTasks}, metrics=${config.enabled && config.metricsEnabled})")

    // Stage name is not available on the executor in Phase 1 (ExecutorPlugin has no access to
    // stage metadata — only stageId from TaskContext). Warn if someone configures the pattern
    // filter so they don't silently get no matches.
    if (config.taskStagePattern.isDefined) {
      logger.warn("[Flare] FLARE_TASK_STAGE_PATTERN is configured but stage names are not " +
        "available on executor in Phase 1. The pattern filter will have no effect — use " +
        "FLARE_TASK_STAGES (stage IDs) instead, or wait for Phase 2 ByteBuddy instrumentation.")
    }
  }

  override def onTaskStart(): Unit = {
    // Clear both up front so a task can never inherit state from the previous task that ran
    // on this thread, whatever path the rest of this method takes.
    taskState.set(None)
    taskMetricState.set(None)

    // Disabled: no span and no measurement, so nothing is armed for endTask to record.
    if (!config.enabled) return

    val taskContext = TaskContext.get()
    if (taskContext == null) {
      // Nothing to attribute a span or a measurement to. The only guard that suppresses both.
      logger.warn("[Flare] onTaskStart called with null TaskContext")
      return
    }

    // Armed before the span guards, so every task Spark hands us is measured even when its
    // span is filtered out below.
    taskMetricState.set(Some((taskContext, System.nanoTime())))

    // Guard 1: granularity + stage/retry filters (replaces simple tracesTasks check).
    // slowTaskMs is deferred to endTask since duration is not known at start.
    if (!config.shouldTraceTask(
      attemptNumber = taskContext.attemptNumber(),
      speculative   = false, // TaskContext does not expose speculative flag directly.
                             // Speculative tasks have attemptNumber > 0, so the retry filter
                             // handles them. True speculative detection needs Phase 2 ByteBuddy.
      stageId       = taskContext.stageId(),
      stageName     = "",    // stage name not available on executor until Phase 2
    )) return

    val parentContext = LocalPropertyPropagator.extract(taskContext)

    // Guard 2: sampling — honour the sampling decision already made by the driver.
    // The driver's TracerProvider applies head-based sampling when creating the application span.
    // That decision propagates via traceparent flags. If the parent is valid but not sampled,
    // we must not create child spans — otherwise the executor generates orphan spans that the
    // backend can't stitch into a trace.
    val parentSpanContext = io.opentelemetry.api.trace.Span.fromContext(parentContext).getSpanContext
    if (parentSpanContext.isValid && !parentSpanContext.isSampled) {
      logger.debug(s"[Flare] Task not sampled (inherited from driver), partition=${taskContext.partitionId()}")
      return
    }

    // Guard 3: maxSpansPerTrace circuit breaker — check BEFORE incrementing.
    if (spanCount.get() >= config.maxSpansPerTrace) {
      if (!maxSpansWarned) {
        logger.warn(s"[Flare] Executor span limit reached (${config.maxSpansPerTrace}). " +
          s"Suppressing further task spans. Task metrics continue to be recorded. " +
          s"Increase FLARE_MAX_SPANS_PER_TRACE to raise the limit.")
        maxSpansWarned = true
      }
      return
    }
    val currentCount = spanCount.incrementAndGet()

    val tracer = GlobalOpenTelemetry.getTracer("io.flare.spark", BuildInfo.version)

    val spanBuilder = tracer
      .spanBuilder("spark.task.executor")
      .setSpanKind(SpanKind.INTERNAL)
      .setParent(parentContext)
      .setLong(Task.PartitionId, taskContext.partitionId().toLong)
      .setLong(Task.AttemptId, taskContext.attemptNumber().toLong)

    // tracesTaskDetails = granularity ALL — add stageId
    if (config.tracesTaskDetails) {
      spanBuilder.setLong(Stage.Id, taskContext.stageId().toLong)
    }

    val span = spanBuilder.startSpan()

    // Under FLARE_SLOW_TASK_MS the span may be dropped at the end, once the duration is known, so
    // it is not made current (#100). Anything made inside the task, a JDBC call or the task's
    // log lines, would otherwise point at a span that is never exported. They sit under the stage
    // instead, whose context the TaskRunner advice makes current around every task (#173), and
    // the stage span is always exported. Without the filter every task span is exported, so the
    // task span is current and in-task spans nest under it.
    val scope =
      if (config.slowTaskMs > 0) None
      else {
        // MDC enrichment: trace_id/span_id in Log4j 2's ThreadContext, for log correlation.
        MdcEnricher.put(span.getSpanContext.getTraceId, span.getSpanContext.getSpanId)
        Some(span.makeCurrent())
      }
    taskState.set(Some((span, scope)))

    logger.debug(s"[Flare] Task span started, partition=${taskContext.partitionId()}, " +
      s"traceId=${span.getSpanContext.getTraceId}, spanCount=$currentCount")
  }

  override def onTaskSucceeded(): Unit = endTask(success = true, failure = None)

  // The TaskFailedReason is carried through rather than stringified here: ExceptionFailure holds
  // the class name, description and full stack trace as separate fields, and toString throws that
  // structure away. This is the only failure path in Flare with that detail available.
  override def onTaskFailed(failureReason: TaskFailedReason): Unit =
    endTask(success = false, failure = Option(failureReason))

  private def endTask(success: Boolean, failure: Option[TaskFailedReason]): Unit = {
    val result = FlareExecutorPlugin.taskResult(success, failure)
    val metricState = Option(taskMetricState.get()).flatten
    val durationMs  = metricState
      .map { case (_, startNanos) => (System.nanoTime() - startNanos) / 1000000L }
      .getOrElse(-1L)

    try {
      (Option(taskState.get()).flatten, metricState) match {
        // A span was created and survives the slow-task filter: describe it, then record
        // metrics while the scope is still open so the SDK attaches an exemplar pointing at
        // a span that really will be exported.
        case (Some((span, scope)), Some((tc, _))) if !isSuppressedAsFast(durationMs) =>
          try {
            describeTaskSpan(span, tc, durationMs, result, failure)
            // A span kept by the slow-task filter was never current; make it current for the
            // recording only, so the exemplar still names this task.
            val recordScope = if (scope.isEmpty) Some(span.makeCurrent()) else None
            try recordTaskMetrics(tc, durationMs, result)
            finally recordScope.foreach(_.close())
          } finally {
            MdcEnricher.remove()
            scope.foreach(_.close())
            span.end()
          }

        // A span was created but the task finished under FLARE_SLOW_TASK_MS, so the span is
        // abandoned rather than ended. We cannot avoid starting it — duration is unknown at
        // task start — and calling span.end() would export exactly what we mean to suppress.
        // BatchSpanProcessor only exports ended spans, so leaving it unended drops it.
        //
        // Ordering is load-bearing: the scope must be closed BEFORE the metric is recorded.
        // The SDK's default exemplar filter is trace_based, which reads Span.current() at
        // record time. Recording inside the scope would stamp the data point with the trace
        // and span id of a span that is never exported, leaving a dashboard exemplar that
        // links to nothing.
        //
        // Tradeoff: the SDK's internal SdkSpan object remains allocated until GC reclaims it.
        // On jobs with many fast tasks this is transient memory pressure proportional to
        // concurrent executor threads, not total tasks. Acceptable for the filtering benefit.
        case (Some((span, scope)), Some((tc, _))) =>
          MdcEnricher.remove()
          scope.foreach(_.close())
          spanCount.decrementAndGet() // reclaim slot — this span won't be exported
          recordTaskMetrics(tc, durationMs, result)

        // No span: a guard in onTaskStart suppressed it. The task still ran and is still
        // measured — this is the case that keeps the task histogram honest when the
        // maxSpansPerTrace circuit breaker trips, or when driver sampling excluded the trace.
        // No scope is open here, so no exemplar is attached, which is correct.
        case (None, Some((tc, _))) =>
          recordTaskMetrics(tc, durationMs, result)

        // No TaskContext was available at task start. Nothing to record.
        case _ => ()
      }
    } finally {
      taskState.remove()
      taskMetricState.remove()
      idleFlush.request()
    }
  }

  /** True when FLARE_SLOW_TASK_MS is active and this task finished under the threshold. */
  private def isSuppressedAsFast(durationMs: Long): Boolean =
    config.slowTaskMs > 0 && durationMs >= 0 && durationMs < config.slowTaskMs

  private def describeTaskSpan(
    span:       Span,
    tc:         TaskContext,
    durationMs: Long,
    result:     String,
    failure:    Option[TaskFailedReason],
  ): Unit = {
    if (result == "SUCCESS") {
      span.setStatus(StatusCode.OK)
      span.setAttribute(Task.Result, "SUCCESS")
    } else if (result == "KILLED") {
      // Not an error (#198): Spark stopped the task itself, mostly because another attempt
      // succeeded first, or because its job or stage was cancelled. The status stays unset.
      span.setAttribute(Task.Result, "KILLED")
      failure.collect { case k: TaskKilled => FlareExecutorPlugin.killReason(k.reason) }
        .foreach(span.setAttribute(Task.KillReason, _))
    } else {
      span.setAttribute(Task.Result, "FAILED")
      FailureDetail.record(
        span,
        failure.map(FailureDetail.fromTaskFailure).getOrElse(
          // onTaskFailed is contractually given a reason, so this is defensive only.
          FailureDetail(errorType = None, message = "Task failed", stackTrace = None)
        ),
      )
    }

    if (durationMs >= 0) {
      span.setLong(Task.DurationMs, durationMs)
    }

    // Task metrics from TaskMetrics (only fully populated at task end).
    try {
      val m = tc.taskMetrics()
      span.setLong(Task.ShuffleReadBytes, m.shuffleReadMetrics.totalBytesRead)
      span.setLong(Task.ShuffleWriteBytes, m.shuffleWriteMetrics.bytesWritten)
      span.setLong(Task.PeakMemory, m.peakExecutionMemory)
      span.setLong(Task.InputBytes, m.inputMetrics.bytesRead)
      span.setLong(Task.OutputBytes, m.outputMetrics.bytesWritten)
    } catch {
      // TaskMetrics is private[spark] — may throw IllegalAccessError from outside
      // org.apache.spark, or fail in barrier mode. Log once for diagnosis.
      case e: Throwable =>
        logger.debug(s"[Flare] Could not read TaskMetrics: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }

    // SQL execution ID from local properties (captured context still has properties)
    Option(tc.getLocalProperty("spark.sql.execution.id"))
      .flatMap(s => scala.util.Try(s.toLong).toOption)
      .foreach(id => span.setLong(Task.SqlExecutionId, id))
  }

  private def recordTaskMetrics(tc: TaskContext, durationMs: Long, result: String): Unit = {
    try {
      val eid = this.executorId
      val attrs = MetricAttributes.forTask(eid, result)

      if (durationMs >= 0) {
        metrics.taskDuration.record(durationMs.toDouble, attrs)
      }

      val m = tc.taskMetrics()
      val totalRecords = m.inputMetrics.recordsRead + m.outputMetrics.recordsWritten
      if (durationMs > 0 && totalRecords > 0) {
        val throughput = totalRecords.toDouble / (durationMs.toDouble / 1000.0)
        metrics.taskRecordsThroughput.record(throughput, attrs)
      }

      val shuffleRead = m.shuffleReadMetrics.totalBytesRead
      if (shuffleRead > 0) metrics.taskShuffleReadBytes.add(shuffleRead, attrs)

      val shuffleWrite = m.shuffleWriteMetrics.bytesWritten
      if (shuffleWrite > 0) metrics.taskShuffleWriteBytes.add(shuffleWrite, attrs)
    } catch {
      case e: Throwable =>
        logger.debug(s"[Flare] Could not record task metrics: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }
  }

  override def shutdown(): Unit = {
    logger.info(s"[Flare] Executor plugin shutdown (total spans created: ${spanCount.get()})")

    // End any in-flight task span on this thread (e.g. K8s SIGTERM during task execution)
    Option(taskState.get()).flatten.foreach { case (span, scope) =>
      MdcEnricher.remove()
      span.setStatus(StatusCode.ERROR, "Executor shutdown before task completed")
      span.setAttribute(Task.Result, "SHUTDOWN")
      scope.foreach(_.close())
      span.end()
      taskState.remove()
    }
    taskMetricState.remove()

    // Push buffered spans and metrics out before the JVM exits (#122).
    idleFlush.close()
    TelemetryFlush.flush("executor shutdown", verbose = true)
  }
}

/**
 * MDC enrichment for Log4j 2 ThreadContext.
 * Injects trace_id and span_id so executor log lines can be correlated with traces in Grafana.
 * Falls back to no-op if Log4j 2 is not on the classpath.
 */
private[plugin] object MdcEnricher {

  private val log4j2Available: Boolean = try {
    Class.forName("org.apache.logging.log4j.ThreadContext")
    true
  } catch {
    case _: ClassNotFoundException => false
  }

  def put(traceId: String, spanId: String): Unit =
    if (log4j2Available) {
      org.apache.logging.log4j.ThreadContext.put("trace_id", traceId)
      org.apache.logging.log4j.ThreadContext.put("span_id", spanId)
    }

  def remove(): Unit =
    if (log4j2Available) {
      org.apache.logging.log4j.ThreadContext.remove("trace_id")
      org.apache.logging.log4j.ThreadContext.remove("span_id")
    }
}

private[plugin] object FlareExecutorPlugin {

  /**
   * A task's `task.result`: SUCCESS, KILLED or FAILED. KILLED is a task Spark stopped itself
   * (#198): under speculation, the attempt that lost the race once another succeeded, and the tasks
   * of a cancelled job or stage. Counting those as FAILED reported errors on healthy runs.
   */
  /**
   * Why Spark killed a task, bucketed: `another_attempt_succeeded` for the losing attempt under
   * speculation, `stage_finished` for a task still running when its stage finished, `cancelled`
   * for a cancelled job, stage or job group, `other` otherwise. Spark's reason is free text, and a
   * cancellation repeats the stage failure behind it, hostnames and executor ids included, which the
   * stage span already carries.
   *
   * `stage_finished` is checked before `cancelled` (#227): Spark words it "Stage cancelled: Stage
   * finished", though nothing was cancelled. It is how the original attempt is killed when a
   * speculative copy of its stage's last task wins (Spark 4.0.4 on the lab), and how a stage that
   * finishes early, as for a `take()`, stops the tasks it no longer needs.
   */
  def killReason(reason: String): String = {
    val r = Option(reason).getOrElse("").toLowerCase(java.util.Locale.ROOT)
    if (r.contains("another attempt succeeded")) "another_attempt_succeeded"
    else if (r.contains("stage finished")) "stage_finished"
    else if (r.contains("cancel")) "cancelled"
    else "other"
  }

  def taskResult(success: Boolean, failure: Option[TaskFailedReason]): String =
    if (success) "SUCCESS"
    else failure match {
      case Some(_: TaskKilled) => "KILLED"
      case _                   => "FAILED"
    }

  /**
   * After an idle flush, how long the next one needs the longer quiet (#139). Shared by the
   * executor's flush after tasks and the driver's after jobs (#199).
   */
  val IdleFlushIntervalMs: Long = 30000L

  /**
   * The quiet an idle flush needs within [[IdleFlushIntervalMs]] of the previous one. It has to
   * beat the kill: a Databricks job cluster starts tearing down about 8.6s after its last job ends
   * (measured on DBR 15.4), so the last flush of a run, 3s after its last task or job, leaves
   * about 5s.
   */
  val IdleFlushThrottledQuietMs: Long = 3000L
}
