package io.flare.spark.plugin

import io.flare.spark.BuildInfo
import io.flare.spark.attributes.SparkAttributes._
import io.flare.spark.config.FlareConfig
import io.flare.spark.listener.TracingSparkListener
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
 * Phase 1 limitation: traceparent is injected once (pointing to the application span).
 * All executor task spans will be children of the application span, not of their
 * specific job/stage span. Phase 2 (ByteBuddy hooking runJob) will inject per-job
 * traceparent for full hierarchical nesting.
 */
class FlareDriverPlugin extends DriverPlugin {

  private val logger = LoggerFactory.getLogger(classOf[FlareDriverPlugin])

  // Written once in init(), read once in shutdown(). Spark guarantees init() completes
  // before shutdown() is called, so no volatile needed.
  private var applicationSpan: Option[Span] = None
  private var listener: Option[TracingSparkListener] = None

  override def init(sc: SparkContext, pluginContext: PluginContext): ju.Map[String, String] = {
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
    val tracingListener = new TracingSparkListener(tracer, config)
    tracingListener.setApplicationSpan(appSpan)
    sc.addSparkListener(tracingListener)
    listener = Some(tracingListener)

    logger.info(s"[Flare] Driver plugin initialized — " +
      s"traceId=${appSpan.getSpanContext.getTraceId}, " +
      s"granularity=${config.granularity}, " +
      s"sampling=${config.samplingRatio}")

    ju.Collections.emptyMap()
  }

  override def shutdown(): Unit = {
    // Delegate to the listener which ends all open spans (stages, jobs, app).
    // The listener's shutdown() calls span.end() on the application span.
    // Span.end() is idempotent, but setStatus after end() is a no-op — so we
    // must NOT unconditionally set OK here, as that would silently mask an
    // ERROR status set by the listener's onApplicationEnd or shutdown path.
    listener.foreach(_.shutdown())

    // Only end the application span directly if the listener was never registered
    // (e.g., init failed partway through before addSparkListener).
    if (listener.isEmpty) {
      applicationSpan.foreach { span =>
        span.setStatus(StatusCode.OK)
        span.end()
      }
    }

    logger.info("[Flare] Driver plugin shutdown complete")
  }
}
