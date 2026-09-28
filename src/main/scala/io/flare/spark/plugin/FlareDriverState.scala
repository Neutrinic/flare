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
      DriverSpans.register()
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
   * on Spark's teardown, so it lands inside the agent's flush window rather than after it. On its
   * own it is still a race against the agent's hook. What closes the race is the agent side
   * calling [[endOpenSpans]] before its span processors shut down (#122); this hook remains for
   * a JVM without the agent's processor wrapper, such as a plain SDK install.
   *
   * A normal exit is unaffected. When the job calls `spark.stop()` the plugin has already shut
   * state down before the JVM exits, so this finds nothing initialised and returns.
   *
   * [[shutdown]] flushes once the spans are ended, on this path and the plugin's alike (#122).
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
   * Shutdown: end all open spans including the application span, then flush. Idempotent — safe
   * to call from both plugin shutdown and a JVM shutdown hook.
   *
   * The flush is what gets the root span out when the JVM is killed shortly after Spark stops
   * (#122). Ending a span only queues it for the next batch.
   */
  def shutdown(): Unit =
    if (endOpenSpans()) TelemetryFlush.flush("driver shutdown", verbose = true)

  /**
   * Ends Flare's open spans, including the root, and clears state. Returns true if this call did
   * it, false if there was nothing to end.
   *
   * Also called by the agent, through [[DriverSpans]], as its tracer provider starts shutting
   * down, and before the span processors stop accepting spans. That is what makes the root span
   * safe from the shutdown race (#83, #122): when a cluster manager sends SIGTERM, as a Databricks
   * job cluster does, Spark never stops the context, and Flare's own JVM hook runs alongside the
   * agent's. Without this, the agent's hook usually wins and the root span is dropped as it ends.
   * No flush here: the processor shutdown that follows exports what is queued.
   */
  def endOpenSpans(): Boolean = synchronized {
    if (!_initialized) false
    else {
      listener.foreach(_.shutdown())
      applicationSpan = None
      listener = None
      _initialized = false
      logger.info("[Flare] Driver state shut down")
      true
    }
  }

  /** Visible for testing — reset all state. */
  private[plugin] def reset(): Unit = synchronized {
    applicationSpan = None
    listener = None
    _initialized = false
  }
}
