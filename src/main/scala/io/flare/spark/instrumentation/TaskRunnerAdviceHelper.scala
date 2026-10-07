package io.flare.spark.instrumentation

import io.flare.spark.propagation.LocalPropertyPropagator
import io.opentelemetry.context.{Context, Scope}

import java.util.logging.{Level, Logger}

/**
 * Scala delegation target for [[TaskRunnerAdvice]].
 *
 * Extracts the W3C `traceparent` from the task's serialized properties and makes
 * the parent OTEL context current for the entire duration of `TaskRunner.run()`.
 *
 * This does NOT create spans — it only restores context. Span lifecycle is managed
 * by `FlareExecutorPlugin`, which fires inside `run()` after `TaskContext` is set.
 *
 * At `@Advice.OnMethodEnter` time, `TaskContext.get()` is null because Spark sets
 * it later inside `run()`. Instead, the advice reads the `taskDescription` field
 * directly (via `@Advice.FieldValue`) and accesses `TaskDescription.properties`
 * via reflection since `TaskDescription` is `private[spark]`.
 *
 * IMPORTANT: Uses `java.util.logging` (not SLF4J) because at advice execution time,
 * SLF4J may not be fully initialized in the agent's extension classloader context.
 */
object TaskRunnerAdviceHelper {

  private val logger = Logger.getLogger(TaskRunnerAdviceHelper.getClass.getName)

  // Cached reflection — Method objects are thread-safe for invoke()
  @volatile private var propertiesMethod: (Class[_], java.lang.reflect.Method) = _
  @volatile private var nameMethod: Option[(Class[_], Option[java.lang.reflect.Method])] = None

  /** `task 3.0 in stage 1.0 (TID 7)`: the name `TaskSetManager` gives every task, 3.3 to 4.2. */
  private val StageInName = """ in stage (\d+)\.""".r.unanchored

  /**
   * Called from `@Advice.OnMethodEnter`.
   *
   * @param taskDescription the `TaskDescription` instance from `TaskRunner.taskDescription`
   *                        field, injected by ByteBuddy's `@Advice.FieldValue`
   * @return an OTEL `Scope` that must be closed in `onExit`, or null if no context
   *         was extracted (no traceparent, or root context only). Typed `AnyRef`: an
   *         OpenTelemetry type here breaks under the agent's relocation, see [[TaskRunnerAdvice]]
   */
  def onEnter(taskDescription: Any): AnyRef = {
    if (taskDescription == null) return null
    try {
      val props = getProperties(taskDescription)
      if (props == null) return null

      val parentContext = LocalPropertyPropagator.extractFromProperties(props, stageIdOf(taskDescription))
      if (parentContext == Context.root()) {
        null
      } else {
        parentContext.makeCurrent()
      }
    } catch {
      case e: Exception =>
        logger.log(Level.FINE, "[Flare] TaskRunner advice enter failed", e)
        null
    }
  }

  /**
   * Called from `@Advice.OnMethodExit`.
   *
   * @param scope the `Scope` returned from `onEnter`, may be null
   */
  def onExit(scope: AnyRef): Unit = {
    if (scope != null) {
      try {
        scope.asInstanceOf[Scope].close()
      } catch {
        case e: Exception =>
          logger.log(Level.FINE, "[Flare] TaskRunner advice exit failed", e)
      }
    }
  }

  /**
   * The task's stage id, or -1 if it cannot be read. `TaskDescription` has no stage id field, so it
   * is read from the task's name (#204); the per-stage context then falls back to the generic one.
   */
  private[instrumentation] def stageIdOf(taskDescription: Any): Int = {
    val cls = taskDescription.getClass
    val method = nameMethod match {
      case Some((c, m)) if c eq cls => m
      case _ =>
        val m = try Some(cls.getMethod("name")) catch { case _: NoSuchMethodException => None }
        nameMethod = Some((cls, m))
        m
    }
    method.map(_.invoke(taskDescription)).collect { case s: String => s } match {
      case Some(StageInName(id)) => try id.toInt catch { case _: NumberFormatException => -1 }
      case _ => -1
    }
  }

  /**
   * Access `TaskDescription.properties` via reflection.
   *
   * `TaskDescription` is `private[spark]` so we cannot reference it by type
   * from extension code. The `properties` method is a public accessor on the
   * case class, returning `java.util.Properties`.
   *
   * The Method reference is cached with its class after the first lookup, and looked up again for
   * another class (only tests pass one). This is a benign race, not proper double-checked locking:
   * two threads can both miss and both do the reflective lookup. This is harmless: both resolve to
   * the same Method object, and the write to `@volatile propertiesMethod` is atomic. Not worth
   * synchronizing for a one-time-per-JVM lookup.
   */
  private def getProperties(taskDescription: Any): java.util.Properties = {
    val cls = taskDescription.getClass
    var cached = propertiesMethod
    if (cached == null || (cached._1 ne cls)) {
      cached = (cls, cls.getMethod("properties"))
      propertiesMethod = cached
    }
    cached._2.invoke(taskDescription).asInstanceOf[java.util.Properties]
  }
}
