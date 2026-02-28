package io.flare.spark.instrumentation

import io.flare.spark.plugin.FlareDriverState
import io.flare.spark.propagation.LocalPropertyPropagator
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, SpanKind, StatusCode}
import io.opentelemetry.context.Context

import java.util.concurrent.ConcurrentHashMap
import java.util.logging.{Level, Logger}

/**
 * Scala delegation target for [[SubmitMissingTasksAdvice]].
 *
 * Hooks `DAGScheduler.submitMissingTasks(stage, jobId)` to inject a per-stage
 * traceparent into `ActiveJob.properties` before tasks are created. This makes
 * executor task spans children of their specific stage span:
 *
 * {{{
 *   spark.application
 *   └── spark.job.{id}
 *       └── spark.stage.{id}
 *           └── spark.task.executor   (on executor JVM)
 * }}}
 *
 * Unlike the previous `runJob` hook, this captures ALL stages — including
 * AQE (Adaptive Query Execution) sub-jobs that bypass `SparkContext.runJob`.
 *
 * Job spans are created on first access per jobId (via `computeIfAbsent`)
 * and stage spans are created per `submitMissingTasks` call. Both are stored
 * for adoption by [[io.flare.spark.listener.TracingSparkListener]] when
 * `onJobStart` / `onStageSubmitted` fire on the listener bus.
 *
 * Reflection is used to access `DAGScheduler.jobIdToActiveJob(jobId).properties`
 * because `DAGScheduler` and `ActiveJob` are `private[scheduler]`.
 *
 * IMPORTANT: Uses `java.util.logging` (not SLF4J) because at advice execution
 * time, SLF4J may not be fully initialized in the agent's extension classloader.
 */
object SubmitMissingTasksAdviceHelper {

  private val logger = Logger.getLogger(classOf[SubmitMissingTasksAdviceHelper.type].getName)

  /**
   * Job spans keyed by jobId. Created on first `submitMissingTasks` call for a
   * given jobId. The listener's `onJobStart` reads (but does not remove) the span
   * via [[getJobSpan]]; the listener's `onJobEnd` removes it via [[removeJobSpan]].
   */
  private[spark] val jobSpans = new ConcurrentHashMap[Int, Span]()

  /**
   * Pending stage spans keyed by stageId. Created per `submitMissingTasks` call,
   * adopted (removed) by the listener at `onStageSubmitted` via [[adoptPendingStageSpan]].
   */
  private[spark] val pendingStageSpans = new ConcurrentHashMap[Int, Span]()

  /**
   * Active SQL execution spans keyed by executionId. Managed by the listener's
   * `onOtherEvent` (SQL start/end). Shared here so the advice can parent job spans
   * under their SQL execution when `spark.sql.execution.id` is present in job properties.
   */
  private[spark] val activeSQLSpans = new ConcurrentHashMap[Long, Span]()

  // ── Cached reflection accessors ────────────────────────────────────────────

  @volatile private var reflectionReady = false
  private var jobIdToActiveJobField: java.lang.reflect.Field = _
  private var stageIdMethod: java.lang.reflect.Method = _
  private var propertiesMethod: java.lang.reflect.Method = _

  private def ensureReflection(dagScheduler: Any): Boolean = {
    if (reflectionReady) return true
    synchronized {
      if (reflectionReady) return true
      try {
        // DAGScheduler.jobIdToActiveJob — private val, Scala mutable.HashMap
        val dsClass = dagScheduler.getClass
        val fields = dsClass.getDeclaredFields
        jobIdToActiveJobField = fields.find(_.getName == "jobIdToActiveJob")
          .orElse(fields.find(_.getName.contains("jobIdToActiveJob")))
          .getOrElse(throw new NoSuchFieldException(
            s"jobIdToActiveJob not found in ${dsClass.getName}"))
        jobIdToActiveJobField.setAccessible(true)

        // Stage.id — public getter inherited from abstract Stage
        stageIdMethod = Class.forName("org.apache.spark.scheduler.Stage")
          .getMethod("id")

        // ActiveJob.properties — public getter on package-private class
        propertiesMethod = Class.forName("org.apache.spark.scheduler.ActiveJob")
          .getMethod("properties")

        reflectionReady = true
        logger.info("[Flare] DAGScheduler reflection initialized successfully")
        true
      } catch {
        case e: Exception =>
          logger.log(Level.WARNING,
            "[Flare] Reflection init failed for DAGScheduler instrumentation", e)
          false
      }
    }
  }

  // ── Advice entry point ────────────────────────────────────────────────────

  /**
   * Called from `@Advice.OnMethodEnter` on `DAGScheduler.submitMissingTasks(stage, jobId)`.
   *
   * 1. Looks up `ActiveJob.properties` via reflection
   * 2. Gets or creates a job span for the jobId
   * 3. Creates a stage span under the job span
   * 4. Injects the stage span's W3C traceparent into the properties
   */
  def onEnter(dagScheduler: Any, stage: Any, jobId: Int): Unit = {
    try {
      if (!ensureReflection(dagScheduler)) return

      val appSpan = FlareDriverState.applicationSpan match {
        case Some(span) => span
        case None => return
      }

      // Get stageId from the Stage argument
      val stageId = stageIdMethod.invoke(stage).asInstanceOf[java.lang.Integer].intValue()

      // Get ActiveJob.properties via reflection on DAGScheduler internals
      val jobMap = jobIdToActiveJobField.get(dagScheduler)
        .asInstanceOf[scala.collection.mutable.Map[Int, AnyRef]]
      val activeJob = jobMap.get(jobId).orNull
      if (activeJob == null) {
        logger.fine(s"[Flare] No ActiveJob for jobId=$jobId, skipping")
        return
      }
      val props = propertiesMethod.invoke(activeJob).asInstanceOf[java.util.Properties]
      if (props == null) return

      val tracer = GlobalOpenTelemetry.getTracer("io.flare.spark")

      // Get or create job span for this jobId (first stage creates it).
      // Use putIfAbsent to handle the race where the listener's onJobStart
      // may have already created and stored a span for this jobId.
      var jobSpan = jobSpans.get(jobId)
      if (jobSpan == null) {
        // Determine parent: SQL span if job is SQL-triggered, otherwise app span.
        // Spark sets spark.sql.execution.id in ActiveJob.properties for SQL jobs.
        val parentSpan = Option(props.getProperty("spark.sql.execution.id"))
          .flatMap(id => try Some(id.toLong) catch { case _: NumberFormatException => None })
          .map(id => activeSQLSpans.get(id))
          .flatMap(Option(_))
          .getOrElse(appSpan)

        val parentCtx = Context.root().`with`(parentSpan)
        val newSpan = tracer
          .spanBuilder(s"spark.job.$jobId")
          .setSpanKind(SpanKind.INTERNAL)
          .setParent(parentCtx)
          .startSpan()
        val existing = jobSpans.putIfAbsent(jobId, newSpan)
        if (existing != null) {
          // Listener already created a span for this jobId — discard ours
          newSpan.end()
          jobSpan = existing
        } else {
          jobSpan = newSpan
        }
      }

      // Create stage span as child of job span
      val stageParentCtx = Context.root().`with`(jobSpan)
      val stageSpan = tracer
        .spanBuilder(s"spark.stage.$stageId")
        .setSpanKind(SpanKind.INTERNAL)
        .setParent(stageParentCtx)
        .startSpan()

      // Handle superseded stage spans (stage retries with same stageId)
      val superseded = pendingStageSpans.put(stageId, stageSpan)
      if (superseded != null) {
        superseded.setStatus(StatusCode.ERROR, "Superseded by stage retry")
        superseded.end()
      }

      // Inject stage span's traceparent into ActiveJob.properties.
      // The method body reads these properties and passes them to the TaskSet,
      // so tasks inherit the stage-level context on the executor.
      val stageCtx = Context.root().`with`(stageSpan)
      LocalPropertyPropagator.injectIntoProperties(stageCtx, props)

      logger.fine(s"[Flare] submitMissingTasks: jobId=$jobId stageId=$stageId traceparent injected")
    } catch {
      case e: Exception =>
        logger.log(Level.FINE, "[Flare] submitMissingTasks advice failed", e)
    }
  }

  // ── Listener adoption methods ─────────────────────────────────────────────

  /**
   * Called by [[io.flare.spark.listener.TracingSparkListener]] at `onJobStart`
   * to get the pre-created job span. Does NOT remove the span — it stays in the
   * map for subsequent `submitMissingTasks` calls that need it as stage parent.
   */
  def getJobSpan(jobId: Int): Option[Span] =
    Option(jobSpans.get(jobId))

  /**
   * Called by [[io.flare.spark.listener.TracingSparkListener]] at `onJobEnd`
   * to remove the job span from the map after it has been ended.
   */
  def removeJobSpan(jobId: Int): Option[Span] =
    Option(jobSpans.remove(jobId))

  /**
   * Called by [[io.flare.spark.listener.TracingSparkListener]] at `onStageSubmitted`
   * to adopt the pre-created stage span. Removes the span from the pending map.
   */
  def adoptPendingStageSpan(stageId: Int): Option[Span] =
    Option(pendingStageSpans.remove(stageId))
}
