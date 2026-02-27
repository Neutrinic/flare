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

  private val logger = Logger.getLogger(classOf[TaskRunnerAdviceHelper.type].getName)

  // Cached reflection — Method objects are thread-safe for invoke()
  @volatile private var propertiesMethod: java.lang.reflect.Method = _

  /**
   * Called from `@Advice.OnMethodEnter`.
   *
   * @param taskDescription the `TaskDescription` instance from `TaskRunner.taskDescription`
   *                        field, injected by ByteBuddy's `@Advice.FieldValue`
   * @return an OTEL `Scope` that must be closed in `onExit`, or null if no context
   *         was extracted (no traceparent, or root context only)
   */
  def onEnter(taskDescription: Any): Scope = {
    if (taskDescription == null) return null
    try {
      val props = getProperties(taskDescription)
      if (props == null) return null

      val parentContext = LocalPropertyPropagator.extractFromProperties(props)
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
  def onExit(scope: Scope): Unit = {
    if (scope != null) {
      try {
        scope.close()
      } catch {
        case e: Exception =>
          logger.log(Level.FINE, "[Flare] TaskRunner advice exit failed", e)
      }
    }
  }

  /**
   * Access `TaskDescription.properties` via reflection.
   *
   * `TaskDescription` is `private[spark]` so we cannot reference it by type
   * from extension code. The `properties` method is a public accessor on the
   * case class, returning `java.util.Properties`.
   *
   * The Method reference is cached after first successful lookup. `@volatile`
   * + double-check is sufficient — worst case is two threads both do the
   * reflective lookup, which is harmless.
   */
  private def getProperties(taskDescription: Any): java.util.Properties = {
    var m = propertiesMethod
    if (m == null) {
      m = taskDescription.getClass.getMethod("properties")
      propertiesMethod = m
    }
    m.invoke(taskDescription).asInstanceOf[java.util.Properties]
  }
}
