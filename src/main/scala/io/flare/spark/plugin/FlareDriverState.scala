package io.flare.spark.plugin

import io.flare.spark.listener.TracingSparkListener
import io.opentelemetry.api.trace.Span

import java.util.logging.Logger

/**
 * Shared driver-side state that prevents double-initialization when both the
 * SparkPlugin path (Phase 1) and ByteBuddy advice path (Phase 2) are active.
 *
 * First path to call [[initialize]] wins; the second is a no-op.
 *
 * Uses `@volatile` + `synchronized` for correctness:
 * - `@volatile` on [[initialized]] allows fast non-locking reads in the common
 *   case (already initialized).
 * - `synchronized` in [[initialize]] and [[shutdown]] ensures only one thread
 *   can perform the transition.
 *
 * IMPORTANT: Uses java.util.logging (not SLF4J) because this class is loaded
 * by the OTEL agent's extension classloader where SLF4J is shaded and not
 * visible to extensions.
 */
object FlareDriverState {

  private val logger = Logger.getLogger(FlareDriverState.getClass.getName)

  @volatile private var _initialized: Boolean = false
  @volatile private[spark] var applicationSpan: Option[Span] = None
  @volatile private var listener: Option[TracingSparkListener] = None

  /** Read-only access to initialization state. */
  def initialized: Boolean = _initialized

  /**
   * Attempt to initialize. Returns true if this call performed the initialization,
   * false if it was already done by another path.
   */
  def initialize(span: Span, tracingListener: TracingSparkListener): Boolean = synchronized {
    if (_initialized) {
      logger.info("[Flare] Already initialized, skipping duplicate init")
      false
    } else {
      applicationSpan = Some(span)
      listener = Some(tracingListener)
      _initialized = true
      registerShutdownHook()
      logger.info("[Flare] Driver state initialized")
      true
    }
  }

  @volatile private var hookRegistered = false

  /**
   * End Flare's open spans as soon as the JVM starts shutting down (#83).
   *
   * The application span is the trace root and the last span to end, so it is the one most
   * exposed to shutdown ordering. Without this hook it is ended from Spark's own shutdown path:
   * `sc.stop()` runs inside Spark's JVM shutdown hook, and only after stopping the DAGScheduler,
   * draining the listener bus and releasing executors does the plugin end the span.
   *
   * The javaagent closes its SDK from a separate JVM shutdown hook. The JVM starts all shutdown
   * hooks at once, in no defined order, so on an abrupt exit those two race — and `sc.stop()` is
   * slow enough that the agent usually wins. Its `BatchSpanProcessor` then drops the root span as
   * it is ended. Measured on a two-host cluster against a remote collector: the root was lost in
   * 4 of 5 runs ending in `System.exit`, leaving every `spark.sql.N` orphaned and the trace
   * rootless.
   *
   * This hook does only one thing, and does it immediately: end the open spans. It does not wait
   * on Spark's teardown, so it lands inside the agent's flush window rather than after it. It is
   * still a race against the agent's hook, which Flare cannot order itself ahead of, so this
   * narrows the loss rather than guaranteeing against it.
   *
   * A normal exit is unaffected. When the job calls `spark.stop()` the plugin has already shut
   * state down before the JVM exits, so this finds nothing initialised and returns.
   *
   * Flushing is left to the agent's own hook. Spark-side code cannot flush under the agent at all,
   * because `GlobalOpenTelemetry` returns an API bridge rather than the SDK.
   */
  private def registerShutdownHook(): Unit =
    if (!hookRegistered) {
      hookRegistered = true
      try {
        Runtime.getRuntime.addShutdownHook(
          new Thread(() => shutdown(), "flare-driver-shutdown")
        )
      } catch {
        // Shutdown already under way; nothing left to protect.
        case _: IllegalStateException => ()
      }
    }

  /**
   * Shutdown: delegate to the listener (which ends all open spans including
   * the application span). Idempotent — safe to call from both plugin shutdown
   * and a JVM shutdown hook.
   */
  def shutdown(): Unit = synchronized {
    if (!_initialized) return
    listener.foreach(_.shutdown())
    applicationSpan = None
    listener = None
    _initialized = false
    logger.info("[Flare] Driver state shut down")
  }

  /** Visible for testing — reset all state. */
  private[plugin] def reset(): Unit = synchronized {
    applicationSpan = None
    listener = None
    _initialized = false
  }
}
