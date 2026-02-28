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

  private val logger = Logger.getLogger(classOf[FlareDriverState.type].getName)

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
      logger.info("[Flare] Driver state initialized")
      true
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
