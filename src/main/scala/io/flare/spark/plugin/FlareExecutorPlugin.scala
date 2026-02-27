package io.flare.spark.plugin

import io.flare.spark.BuildInfo
import io.flare.spark.SpanCompat._
import io.flare.spark.attributes.SparkAttributes._
import io.flare.spark.config.{FlareConfig, TraceGranularity}
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
 * Respects FLARE_TRACE_GRANULARITY — task spans are only created when granularity
 * is `tasks` or `all`. With `jobs` or `stages`, onTaskStart is a no-op.
 *
 * Respects FLARE_SAMPLING_RATIO — samples consistently per trace by checking
 * the sampling bit already set in the incoming W3C traceparent. If the driver
 * sampled the trace, the executor follows. No independent sampling decision here.
 *
 * Respects FLARE_MAX_SPANS_PER_TRACE — once the per-executor span counter
 * exceeds the limit, new task spans are suppressed and a warning is logged once.
 *
 * Uses ThreadLocal because executor threads are pooled and run tasks serially
 * per thread, but multiple threads may run tasks concurrently on one executor.
 */
class FlareExecutorPlugin extends ExecutorPlugin {

  private val logger = LoggerFactory.getLogger(classOf[FlareExecutorPlugin])

  // Loaded once in init(), then only read — no volatile needed since Spark guarantees
  // init() completes before any task callbacks fire on this executor.
  private var config: FlareConfig = _

  // Counts spans created on this executor JVM across all tasks.
  // Reset is not possible mid-job, so this is a conservative circuit breaker
  // that prevents unbounded span growth on large jobs.
  private val spanCount = new AtomicLong(0L)

  // Set to true once the maxSpansPerTrace warning has been logged, to avoid log spam
  @volatile private var maxSpansWarned = false

  // ThreadLocal holds the active (span, scope) for the current task on this thread.
  // None means this task was not sampled or granularity suppressed it.
  private val taskState = new ThreadLocal[Option[(Span, Scope)]]()

  override def init(ctx: PluginContext, extraConf: ju.Map[String, String]): Unit = {
    config = try FlareConfig.load() catch {
      case e: IllegalArgumentException =>
        logger.error(s"[Flare] Configuration error — executor task spans disabled: ${e.getMessage}")
        // Fall back to a disabled config so onTaskStart is a no-op
        FlareConfig(enabled = false, granularity = TraceGranularity.Stages,
          samplingRatio = 0.0, maxSpansPerTrace = 0, slowTaskMs = 0L,
          retryTasksOnly = false, taskStageIds = Set.empty, taskStagePattern = None)
    }
    logger.info(s"[Flare] Executor plugin initialized (granularity=${config.granularity}, " +
      s"sampling=${config.samplingRatio}, maxSpans=${config.maxSpansPerTrace}, " +
      s"taskTracing=${config.tracesTasks})")
  }

  override def onTaskStart(): Unit = {
    // Guard 1: granularity — tasks must be enabled
    if (!config.tracesTasks) {
      taskState.set(None)
      return
    }

    val taskContext = TaskContext.get()
    if (taskContext == null) {
      logger.warn("[Flare] onTaskStart called with null TaskContext")
      taskState.set(None)
      return
    }

    val parentContext = LocalPropertyPropagator.extract(taskContext)

    // Guard 2: sampling — honour the sampling decision already made by the driver.
    // The W3C traceparent flags byte encodes the sampling bit. If the driver did not
    // sample this trace (flags = 00), don't create an executor span either.
    // This keeps sampling consistent across the driver→executor boundary.
    val parentSpanContext = io.opentelemetry.api.trace.Span.fromContext(parentContext).getSpanContext
    if (parentSpanContext.isValid && !parentSpanContext.isSampled) {
      logger.debug(s"[Flare] Task not sampled (inherited from driver), partition=${taskContext.partitionId()}")
      taskState.set(None)
      return
    }

    // Guard 3: maxSpansPerTrace circuit breaker — check BEFORE incrementing so
    // suppressed tasks don't consume counter slots.
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

    // Task.Speculative: attemptNumber > 0 means retry, not necessarily speculative.
    // Spark's public TaskContext API does not expose a speculative flag directly.
    // We record the attempt number and leave the speculative attribute unset
    // rather than conflating retries with speculation.

    // tracesTaskDetails = granularity ALL — add stageId
    if (config.tracesTaskDetails) {
      spanBuilder.setLong(Stage.Id, taskContext.stageId().toLong)
    }

    val span  = spanBuilder.startSpan()
    val scope = span.makeCurrent()
    taskState.set(Some((span, scope)))

    logger.debug(s"[Flare] Task span started, partition=${taskContext.partitionId()}, " +
      s"traceId=${span.getSpanContext.getTraceId}, spanCount=$currentCount")
  }

  override def onTaskSucceeded(): Unit = endTask(success = true, reason = None)

  override def onTaskFailed(failureReason: TaskFailedReason): Unit =
    endTask(success = false, reason = Some(failureReason.toString))

  private def endTask(success: Boolean, reason: Option[String]): Unit = {
    Option(taskState.get()).flatten.foreach { case (span, scope) =>
      try {
        if (success) {
          span.setStatus(StatusCode.OK)
          span.setAttribute(Task.Result, "SUCCESS")
        } else {
          span.setStatus(StatusCode.ERROR, reason.getOrElse("Task failed"))
          span.setAttribute(Task.Result, "FAILED")
          reason.foreach(r => span.setAttribute(Error.Message, r))
        }
      } finally {
        scope.close()
        span.end()
        taskState.remove()
      }
    }
  }

  override def shutdown(): Unit = {
    logger.info(s"[Flare] Executor plugin shutdown (total spans created: ${spanCount.get()})")
  }
}
