package io.flare.spark.plugin

import io.flare.spark.BuildInfo
import io.flare.spark.attributes.SparkAttributes._
import io.flare.spark.config.FlareConfig
import io.flare.spark.listener.TracingSparkListener
import io.flare.spark.metrics.FlareMetrics
import io.flare.spark.propagation.LocalPropertyPropagator
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, SpanKind, StatusCode}
import io.opentelemetry.context.Context
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import org.slf4j.LoggerFactory

import java.{util => ju}

/**
 * Driver-side plugin that wires everything together on the driver JVM:
 *
 * 1. Creates the root application span (SERVER kind)
 * 2. Injects W3C traceparent into SparkContext local properties so executors
 *    inherit the trace context via TaskDescription serialization
 * 3. Registers TracingSparkListener for job/stage/SQL spans on the driver
 *
 * This runs inside DriverPlugin.init(), which fires during SparkContext construction
 * after the LiveListenerBus is started but before any jobs are submitted.
 *
 * The traceparent injected here points to the application span. When ByteBuddy is active,
 * [[io.flare.spark.instrumentation.RunJobAdviceHelper]] temporarily overrides it at each
 * `runJob` call with a per-job traceparent, giving executor tasks the correct job span as
 * parent. Without ByteBuddy, all tasks fall back to being children of the application span.
 */
class FlareDriverPlugin extends DriverPlugin {

  private val logger = LoggerFactory.getLogger(classOf[FlareDriverPlugin])

  // Written once in init(), read once in shutdown(). Spark guarantees init() completes
  // before shutdown() is called, so no volatile needed.
  private var applicationSpan: Option[Span] = None
  private var listener: Option[TracingSparkListener] = None

  override def init(sc: SparkContext, pluginContext: PluginContext): ju.Map[String, String] = {
    // Dedup guard: if ByteBuddy advice already initialized (Phase 2), skip plugin init.
    if (FlareDriverState.initialized) {
      logger.info("[Flare] Already initialized by ByteBuddy advice, skipping plugin init")
      return ju.Collections.emptyMap()
    }

    val config = try FlareConfig.load() catch {
      case e: IllegalArgumentException =>
        logger.error(s"[Flare] Configuration error — plugin disabled: ${e.getMessage}")
        return ju.Collections.emptyMap()
    }

    if (!config.enabled) {
      logger.info("[Flare] Disabled via FLARE_ENABLED=false")
      return ju.Collections.emptyMap()
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

    applicationSpan = Some(appSpan)

    // 2. Inject traceparent into SparkContext local properties.
    //    SparkContext.localProperties is an InheritableThreadLocal — properties set here
    //    on the init thread propagate to all child threads and are serialized into
    //    TaskDescription for each submitted task.
    val spanContext = Context.current().`with`(appSpan)
    LocalPropertyPropagator.inject(spanContext, sc)

    // 3. Register TracingSparkListener for driver-side job/stage/SQL spans
    val flareMetrics = FlareMetrics.create(config.metricsEnabled)
    val tracingListener = new TracingSparkListener(tracer, config, Some(flareMetrics))
    tracingListener.setApplicationSpan(appSpan)
    sc.addSparkListener(tracingListener)
    listener = Some(tracingListener)

    // 4. Register in shared state so ByteBuddy advice (if loaded later) skips init
    FlareDriverState.initialize(appSpan, tracingListener)

    logger.info(s"[Flare] Driver plugin initialized — " +
      s"traceId=${appSpan.getSpanContext.getTraceId}, " +
      s"granularity=${config.granularity}, " +
      s"sampling=${config.samplingRatio}")

    ju.Collections.emptyMap()
  }

  override def shutdown(): Unit = {
    // Delegate to FlareDriverState which coordinates with both SparkPlugin and
    // ByteBuddy paths. FlareDriverState.shutdown() calls listener.shutdown()
    // which ends all open spans (stages, jobs, app).
    FlareDriverState.shutdown()

    // Only end the application span directly if the listener was never registered
    // (e.g., init failed partway through before addSparkListener, or FlareDriverState
    // was never initialized because config was invalid/disabled).
    if (listener.isEmpty) {
      applicationSpan.foreach { span =>
        span.setStatus(StatusCode.OK)
        span.end()
      }
    }

    // Force-flush TracerProvider and MeterProvider to push buffered spans/metrics.
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
        logger.warn(s"[Flare] Error during shutdown flush: {}", e.getMessage)
    }

    logger.info("[Flare] Driver plugin shutdown complete")
  }
}
