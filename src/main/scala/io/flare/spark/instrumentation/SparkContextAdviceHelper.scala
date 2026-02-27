package io.flare.spark.instrumentation

import io.flare.spark.BuildInfo
import io.flare.spark.attributes.SparkAttributes._
import io.flare.spark.config.FlareConfig
import io.flare.spark.listener.TracingSparkListener
import io.flare.spark.plugin.FlareDriverState
import io.flare.spark.propagation.LocalPropertyPropagator
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.context.Context
import org.apache.spark.SparkContext

import java.util.logging.{Level, Logger}

/**
 * Scala delegation target for [[SparkContextAdvice]].
 *
 * Performs the same initialization as [[io.flare.spark.plugin.FlareDriverPlugin.init()]]
 * but triggered by ByteBuddy hooking SparkContext construction instead of SparkPlugin.
 *
 * Uses [[FlareDriverState]] for dedup: if the SparkPlugin path already initialized,
 * this is a no-op and vice versa.
 *
 * IMPORTANT: Uses java.util.logging (not SLF4J) because at advice execution time,
 * SLF4J may not be fully initialized in the agent's extension classloader context.
 * The java.util.logging API is always on the bootstrap classpath.
 */
object SparkContextAdviceHelper {

  private val logger = Logger.getLogger(classOf[SparkContextAdviceHelper.type].getName)

  def onSparkContextInit(sc: SparkContext): Unit = {
    // Fast path: already initialized by SparkPlugin or a previous advice call
    if (FlareDriverState.initialized) {
      logger.fine("[Flare] ByteBuddy advice: already initialized, skipping")
      return
    }

    try {
      val config = FlareConfig.load()

      if (!config.enabled) {
        logger.info("[Flare] Disabled via FLARE_ENABLED=false")
        return
      }

      val tracer = GlobalOpenTelemetry.getTracer("io.flare.spark", BuildInfo.version)

      // 1. Create the root application span
      val appSpan = tracer
        .spanBuilder("spark.application")
        .setSpanKind(SpanKind.SERVER)
        .setAttribute(Application.Name, sc.appName)
        .setAttribute(Application.Id, sc.applicationId)
        .setAttribute(Application.Master, sc.master)
        .setAttribute(Flare.Version, BuildInfo.version)
        .setAttribute(Flare.TraceGranularity, config.granularity.toString.toLowerCase)
        .startSpan()

      // 2. Inject traceparent into SparkContext local properties
      val spanContext = Context.current().`with`(appSpan)
      LocalPropertyPropagator.inject(spanContext, sc)

      // 3. Create listener (but do NOT register with SparkContext yet)
      val tracingListener = new TracingSparkListener(tracer, config)
      tracingListener.setApplicationSpan(appSpan)

      // 4. Atomically register in shared state — must happen BEFORE addSparkListener.
      //    If we added the listener first and then lost the race, the duplicate listener
      //    would remain registered on the SparkContext (no removeSparkListener API) and
      //    produce duplicate job/stage spans for the lifetime of the application.
      if (!FlareDriverState.initialize(appSpan, tracingListener)) {
        // Another path won the race — clean up our span; listener was never registered
        appSpan.end()
        logger.info("[Flare] ByteBuddy advice: lost init race to SparkPlugin, cleaned up")
        return
      }

      // 5. Won the race — safe to register the listener now
      sc.addSparkListener(tracingListener)

      logger.info(
        s"[Flare] ByteBuddy advice initialized — " +
        s"traceId=${appSpan.getSpanContext.getTraceId}, " +
        s"granularity=${config.granularity}, " +
        s"sampling=${config.samplingRatio}"
      )

    } catch {
      case e: IllegalArgumentException =>
        logger.log(Level.SEVERE, s"[Flare] Configuration error — advice disabled: ${e.getMessage}", e)
      case e: Exception =>
        logger.log(Level.SEVERE, s"[Flare] ByteBuddy advice init failed: ${e.getMessage}", e)
    }
  }
}
