package io.flare.spark.plugin

import io.flare.spark.BuildInfo
import io.flare.spark.SpanCompat._
import io.flare.spark.attributes.SparkAttributes._
import io.flare.spark.config.{FlareConfig, TraceGranularity}
import io.flare.spark.metrics.{FlareMetrics, MetricAttributes}
import io.flare.spark.propagation.LocalPropertyPropagator
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, SpanKind, StatusCode}
import io.opentelemetry.context.Scope
import org.apache.spark.TaskContext
import org.apache.spark.TaskFailedReason
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

  // ThreadLocal holds the active (span, scope, taskContext) for the current task on this thread.
  // None means this task was not sampled or granularity suppressed it.
  // TaskContext is captured at onTaskStart because Spark 4.0 clears TaskContext.get() before
  // onTaskSucceeded/onTaskFailed fires, so we can't rely on it in endTask.
  private val taskState = new ThreadLocal[Option[(Span, Scope, TaskContext)]]()

  // Nanosecond timestamp at task start — used for duration_ms attribute and slow task filter.
  private val taskStartNanos = new ThreadLocal[Long]()

  override def init(ctx: PluginContext, extraConf: ju.Map[String, String]): Unit = {
    config = try FlareConfig.load() catch {
      case e: IllegalArgumentException =>
        logger.error(s"[Flare] Configuration error — executor task spans disabled: ${e.getMessage}")
        FlareConfig(enabled = false, granularity = TraceGranularity.Stages,
          samplingRatio = 0.0, maxSpansPerTrace = 0, slowTaskMs = 0L,
          retryTasksOnly = false, taskStageIds = Set.empty, taskStagePattern = None,
          metricsEnabled = false)
    }
    metrics = FlareMetrics.create(config.metricsEnabled)
    executorId = ctx.executorID()
    logger.info(s"[Flare] Executor plugin initialized (executorId=$executorId, granularity=${config.granularity}, " +
      s"sampling=${config.samplingRatio}, maxSpans=${config.maxSpansPerTrace}, " +
      s"taskTracing=${config.tracesTasks}, metrics=${config.metricsEnabled})")

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
    val taskContext = TaskContext.get()
    if (taskContext == null) {
      logger.warn("[Flare] onTaskStart called with null TaskContext")
      taskState.set(None)
      return
    }

    // Guard 1: granularity + stage/retry filters (replaces simple tracesTasks check).
    // slowTaskMs is deferred to endTask since duration is not known at start.
    if (!config.shouldTraceTask(
      attemptNumber = taskContext.attemptNumber(),
      speculative   = false, // TaskContext does not expose speculative flag directly.
                             // Speculative tasks have attemptNumber > 0, so the retry filter
                             // handles them. True speculative detection needs Phase 2 ByteBuddy.
      stageId       = taskContext.stageId(),
      stageName     = "",    // stage name not available on executor until Phase 2
    )) {
      taskState.set(None)
      return
    }

    val parentContext = LocalPropertyPropagator.extract(taskContext)

    // Guard 2: sampling — honour the sampling decision already made by the driver.
    // The driver's TracerProvider applies head-based sampling when creating the application span.
    // That decision propagates via traceparent flags. If the parent is valid but not sampled,
    // we must not create child spans — otherwise the executor generates orphan spans that the
    // backend can't stitch into a trace.
    val parentSpanContext = io.opentelemetry.api.trace.Span.fromContext(parentContext).getSpanContext
    if (parentSpanContext.isValid && !parentSpanContext.isSampled) {
      logger.debug(s"[Flare] Task not sampled (inherited from driver), partition=${taskContext.partitionId()}")
      taskState.set(None)
      return
    }

    // Guard 3: maxSpansPerTrace circuit breaker — check BEFORE incrementing.
    if (spanCount.get() >= config.maxSpansPerTrace) {
      if (!maxSpansWarned) {
        logger.warn(s"[Flare] Executor span limit reached (${config.maxSpansPerTrace}). " +
          s"Suppressing further task spans. Increase FLARE_MAX_SPANS_PER_TRACE to raise the limit.")
        maxSpansWarned = true
      }
      taskState.set(None)
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

    val span  = spanBuilder.startSpan()
    val scope = span.makeCurrent()
    taskState.set(Some((span, scope, taskContext)))
    taskStartNanos.set(System.nanoTime())

    // MDC enrichment — inject trace_id/span_id into Log4j 2 ThreadContext for log correlation
    MdcEnricher.put(span.getSpanContext.getTraceId, span.getSpanContext.getSpanId)

    logger.debug(s"[Flare] Task span started, partition=${taskContext.partitionId()}, " +
      s"traceId=${span.getSpanContext.getTraceId}, spanCount=$currentCount")
  }

  override def onTaskSucceeded(): Unit = endTask(success = true, reason = None)

  override def onTaskFailed(failureReason: TaskFailedReason): Unit =
    endTask(success = false, reason = Some(failureReason.toString))

  private def endTask(success: Boolean, reason: Option[String]): Unit = {
    Option(taskState.get()).flatten.foreach { case (span, scope, capturedTaskContext) =>
      try {
        // Compute duration for slow task filter and duration attribute
        val startNanos = taskStartNanos.get()
        val durationMs = if (startNanos > 0) (System.nanoTime() - startNanos) / 1000000L else -1L

        // Slow task filter — drop span if task was faster than threshold.
        // We cannot avoid starting the span (duration unknown at start), so we abandon it here.
        // Calling span.end() would export a span we want to suppress. Instead we close the scope
        // and leave the span unended — BatchSpanProcessor only exports ended spans.
        // Tradeoff: the SDK's internal SdkSpan object remains allocated until GC reclaims it.
        // On jobs with many fast tasks (e.g. thousands below threshold) this is transient memory
        // pressure proportional to concurrent executor threads, not total tasks — each ThreadLocal
        // is overwritten on the next task. Acceptable for the filtering benefit.
        if (config.slowTaskMs > 0 && durationMs >= 0 && durationMs < config.slowTaskMs) {
          // Record metrics even for fast tasks whose spans are dropped — metrics have
          // their own aggregation and don't suffer from span cardinality pressure.
          recordTaskMetrics(capturedTaskContext, durationMs, success)
          MdcEnricher.remove()
          scope.close()
          spanCount.decrementAndGet() // reclaim slot — this span won't be exported
          taskState.remove()
          taskStartNanos.remove()
          return
        }

        // Set status
        if (success) {
          span.setStatus(StatusCode.OK)
          span.setAttribute(Task.Result, "SUCCESS")
        } else {
          span.setStatus(StatusCode.ERROR, reason.getOrElse("Task failed"))
          span.setAttribute(Task.Result, "FAILED")
          reason.foreach(r => span.setAttribute(Error.Message, r))
        }

        // Record task duration
        if (durationMs >= 0) {
          span.setLong(Task.DurationMs, durationMs)
        }

        // Record task metrics from TaskMetrics (only fully populated at task end).
        // TaskContext is captured at onTaskStart because Spark 4.0 clears TaskContext.get()
        // before onTaskSucceeded/onTaskFailed fires (unlike Spark 3.x).
        try {
          val m = capturedTaskContext.taskMetrics()
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
        Option(capturedTaskContext.getLocalProperty("spark.sql.execution.id"))
          .flatMap(_.toLongOption)
          .foreach(id => span.setLong(Task.SqlExecutionId, id))

        // Record OTEL metrics while span scope is still active → automatic exemplar linking.
        // The SDK captures the current span's trace ID + span ID on each data point.
        recordTaskMetrics(capturedTaskContext, durationMs, success)
      } finally {
        MdcEnricher.remove()
        scope.close()
        span.end()
        taskState.remove()
        taskStartNanos.remove()
      }
    }
  }

  private def recordTaskMetrics(tc: TaskContext, durationMs: Long, success: Boolean): Unit = {
    try {
      val eid = this.executorId
      val attrs = MetricAttributes.forTask(eid, tc.stageId(), if (success) "SUCCESS" else "FAILED")

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
    Option(taskState.get()).flatten.foreach { case (span, scope, _) =>
      MdcEnricher.remove()
      span.setStatus(StatusCode.ERROR, "Executor shutdown before task completed")
      span.setAttribute(Task.Result, "SHUTDOWN")
      scope.close()
      span.end()
      taskState.remove()
    }

    // Force-flush TracerProvider and MeterProvider to push buffered spans/metrics
    // before JVM exits. The SDK classes live in the agent classloader — catch
    // NoClassDefFoundError if they're not visible from the app classloader.
    try {
      GlobalOpenTelemetry.get() match {
        case sdk: io.opentelemetry.sdk.OpenTelemetrySdk =>
          sdk.getSdkTracerProvider.forceFlush().join(5, ju.concurrent.TimeUnit.SECONDS)
          sdk.getSdkMeterProvider.forceFlush().join(5, ju.concurrent.TimeUnit.SECONDS)
          logger.info("[Flare] Forced flush of TracerProvider and MeterProvider completed")
        case _ => ()
      }
    } catch {
      case _: NoClassDefFoundError =>
        logger.debug("[Flare] SDK classes not accessible, relying on agent shutdown hook")
      case e: Exception =>
        logger.warn(s"[Flare] Error during shutdown flush: ${e.getMessage}")
    }
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
