package io.flare.spark.plugin

import io.opentelemetry.api.GlobalOpenTelemetry

import java.lang.reflect.Method
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, ThreadFactory, TimeUnit}
import java.util.logging.{Level, Logger}

/**
 * Pushes buffered spans and metrics out now instead of on the next export interval (#122).
 *
 * This matters when a JVM is killed without warning. A Databricks job cluster is torn down the
 * moment the run ends: the driver is killed about a second after Spark stops, and executors get
 * no shutdown call at all. Anything still sitting in the batch span processor (5s) or the periodic
 * metric reader (60s) is lost, which is how the root `spark.application` span and every executor
 * task metric went missing there.
 *
 * Under the javaagent, Spark-side code cannot reach the SDK: `GlobalOpenTelemetry` returns the
 * agent's API bridge. The agent does publish a flush entry point for this, in its bootstrap
 * classloader so every classloader can see it: `OpenTelemetrySdkAccess.forceFlush`, which its AWS
 * Lambda instrumentation calls before the process is frozen. It is looked up reflectively so Flare
 * still loads without the agent, where the SDK path below applies instead.
 *
 * Uses java.util.logging because [[FlareDriverState]] calls this, and it may be loaded by the
 * agent's extension classloader, where SLF4J is shaded away.
 */
private[spark] object TelemetryFlush {

  private val logger = Logger.getLogger(TelemetryFlush.getClass.getName)

  private val AgentAccessClass = "io.opentelemetry.javaagent.bootstrap.OpenTelemetrySdkAccess"

  // Left holds why the agent's entry point is unavailable, for the shutdown log line.
  private lazy val agentForceFlush: Either[String, Method] =
    try Right(Class.forName(AgentAccessClass).getMethod("forceFlush", classOf[Long], classOf[TimeUnit]))
    catch {
      case e @ (_: ClassNotFoundException | _: NoSuchMethodException | _: LinkageError) =>
        Left(s"${e.getClass.getSimpleName}: ${e.getMessage}")
    }

  /** Flush through the agent if it is attached. Returns false when there is no agent to ask. */
  private[plugin] def flushAgent(timeoutMs: Long): Boolean = agentForceFlush match {
    case Right(method) =>
      try {
        method.invoke(null, Long.box(timeoutMs), TimeUnit.MILLISECONDS)
        true
      } catch {
        case e: Exception =>
          logger.log(Level.WARNING, s"[Flare] Agent flush failed: ${e.getClass.getSimpleName}", e)
          false
      }
    case Left(_) => false
  }

  /**
   * Flush through the agent, or failing that through a plain SDK installed as the global. Blocks
   * for at most roughly `timeoutMs` per provider.
   *
   * `verbose` logs the outcome at INFO. Use it for one-off flushes at shutdown, where it is the only
   * evidence the flush ran, and not for the executor's frequent idle flushes.
   */
  def flush(reason: String, timeoutMs: Long = 5000L, verbose: Boolean = false): Unit = {
    val level = if (verbose) Level.INFO else Level.FINE
    val started = System.nanoTime()
    if (flushAgent(timeoutMs)) {
      logger.log(level, s"[Flare] Flushed telemetry through the agent ($reason) in " +
        s"${(System.nanoTime() - started) / 1000000L} ms")
    } else {
      agentForceFlush match {
        case Left(why) => logger.log(level, s"[Flare] Agent flush unavailable ($why)")
        case Right(_)  => ()
      }
      try {
        // getOrNoop, not get: get() installs a no-op global when none is set yet, and a flush
        // must never decide what the global is.
        GlobalOpenTelemetry.getOrNoop() match {
          case sdk: io.opentelemetry.sdk.OpenTelemetrySdk =>
            sdk.getSdkTracerProvider.forceFlush().join(timeoutMs, TimeUnit.MILLISECONDS)
            sdk.getSdkMeterProvider.forceFlush().join(timeoutMs, TimeUnit.MILLISECONDS)
            logger.log(level, s"[Flare] Flushed the global SDK ($reason)")
          case other =>
            logger.log(level, s"[Flare] Nothing to flush ($reason): no agent, and " +
              s"GlobalOpenTelemetry is ${other.getClass.getName}")
        }
      } catch {
        // The SDK is not on this classloader, or an older API without getOrNoop is. Nothing to
        // flush from here.
        case _: LinkageError => ()
        case e: Exception =>
          logger.log(Level.WARNING, s"[Flare] Flush failed ($reason): ${e.getMessage}", e)
      }
    }
  }
}

/**
 * Runs `action` once things go quiet: each [[request]] restarts a `delayMs` timer, and the action
 * fires when a timer runs out.
 *
 * The executor requests a flush at every task end. While tasks keep finishing the timer keeps
 * moving, so a busy executor does not flush at all and the periodic exporters cover it. Once the
 * last task ends, the flush follows `delayMs` later, which is the last point Flare can act on a
 * cluster that kills executors without a shutdown call.
 *
 * Within `minIntervalMs` of the previous run, the quiet has to last `throttledDelayMs` instead
 * (#139). Executors also go quiet between the stages of every query, every few seconds, and each
 * flush re-sends every cumulative metric series. Those gaps are mostly shorter than
 * `throttledDelayMs`, so they no longer flush; the end of a run is not, so it still does, just
 * `throttledDelayMs` after the last task rather than `delayMs`. Deferring to the end of the
 * interval instead would be too late: a Databricks job cluster starts tearing down about 8.6s
 * after the last job ends.
 */
private[plugin] final class QuietPeriodAction(
  delayMs: Long,
  action: () => Unit,
  name: String,
  minIntervalMs: Long = 0L,
  throttledDelayMs: Long = 0L,
) {

  // Started on the first request, so an executor that never runs a task never starts a thread.
  private lazy val scheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, name)
        t.setDaemon(true)
        t
      }
    })

  private var pending: Option[ScheduledFuture[_]] = None
  private var closed = false
  // When the action last started, from System.nanoTime. None until it first runs.
  private var lastRun: Option[Long] = None

  def request(): Unit = synchronized {
    if (closed) return
    pending.foreach(_.cancel(false))
    val sinceLastMs = lastRun.map(t => (System.nanoTime() - t) / 1000000L)
    // Inside the interval: the longer quiet, or the end of the interval if that comes first.
    val waitMs = sinceLastMs.filter(_ < minIntervalMs)
      .fold(delayMs)(since => math.max(delayMs, math.min(throttledDelayMs, minIntervalMs - since)))
    pending =
      try Some(scheduler.schedule(new Runnable {
        override def run(): Unit = {
          QuietPeriodAction.this.synchronized { lastRun = Some(System.nanoTime()) }
          action()
        }
      }, waitMs, TimeUnit.MILLISECONDS))
      catch {
        // Closed by shutdown, which flushes on its own.
        case _: java.util.concurrent.RejectedExecutionException => None
      }
  }

  /** Drop any pending run and stop the thread. */
  def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      // A pending entry means the scheduler was started. Touching it otherwise would start it.
      pending.foreach { p =>
        p.cancel(false)
        scheduler.shutdownNow()
      }
      pending = None
    }
  }
}
