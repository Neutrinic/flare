package io.flare.spark.propagation

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.context.propagation.{TextMapGetter, TextMapSetter}
import io.opentelemetry.context.{Context => OtelContext}
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory

import java.{util => ju}

/**
 * Injects and extracts W3C traceparent/tracestate via Spark's LocalProperty mechanism.
 *
 * Injection (driver side): writes to SparkContext.setLocalProperty().
 * The local property is serialized into TaskDescription and deserialized on the executor JVM.
 *
 * Extraction (executor side): reads from TaskContext.getLocalProperty().
 *
 * IMPORTANT: Use W3C format exclusively. Do NOT use custom traceId-spanId-flags serialization.
 * The property key is the literal string "traceparent" (and "tracestate" if present).
 */
object LocalPropertyPropagator {

  private val logger = LoggerFactory.getLogger(getClass)

  // W3C standard header names, used as local property keys
  private val TraceparentKey = "traceparent"
  private val TracestateKey  = "tracestate"

  /**
   * Inject current OTEL context into Spark local properties.
   * Call on the driver before submitting tasks.
   */
  def inject(context: OtelContext, sc: SparkContext): Unit = {
    val propagator = GlobalOpenTelemetry.getPropagators.getTextMapPropagator
    propagator.inject(context, sc, SparkContextSetter)
    logger.debug(s"[Flare] Injected trace context into local properties")
  }

  /**
   * Extract OTEL parent context from Spark task local properties.
   * Call on the executor at task start, before the task lambda runs.
   */
  def extract(taskContext: TaskContext): OtelContext = {
    val propagator = GlobalOpenTelemetry.getPropagators.getTextMapPropagator
    val parentContext = propagator.extract(OtelContext.root(), taskContext, TaskContextGetter)

    if (parentContext == OtelContext.root()) {
      logger.debug("[Flare] No trace context found in task properties")
    } else {
      logger.debug("[Flare] Extracted trace context from task properties")
    }

    parentContext
  }

  /**
   * Extract OTEL parent context from a raw `java.util.Properties` instance.
   *
   * Used by `TaskRunnerAdviceHelper` where `TaskContext` is not yet available —
   * the ByteBuddy advice fires at the top of `TaskRunner.run()` before Spark sets
   * `TaskContext.get()`. The traceparent is read directly from
   * `TaskDescription.properties` via `@Advice.FieldValue`.
   */
  def extractFromProperties(props: ju.Properties): OtelContext = {
    if (props == null) return OtelContext.root()
    val propagator = GlobalOpenTelemetry.getPropagators.getTextMapPropagator
    propagator.extract(OtelContext.root(), props, PropertiesGetter)
  }

  // ── TextMapSetter for SparkContext ─────────────────────────────────────────

  private val SparkContextSetter: TextMapSetter[SparkContext] =
    (carrier: SparkContext, key: String, value: String) => {
      carrier.setLocalProperty(key, value)
    }

  // ── TextMapGetter for TaskContext ──────────────────────────────────────────

  private val TaskContextGetter: TextMapGetter[TaskContext] =
    new TextMapGetter[TaskContext] {
      override def keys(carrier: TaskContext): java.lang.Iterable[String] =
        ju.Arrays.asList(TraceparentKey, TracestateKey)

      override def get(carrier: TaskContext, key: String): String =
        if (carrier == null) null
        else carrier.getLocalProperty(key)
    }

  // ── TextMapGetter for java.util.Properties ────────────────────────────────
  //
  // keys() intentionally returns only W3C header names. The W3C propagator
  // (default OTEL_PROPAGATORS) calls get() with "traceparent"/"tracestate"
  // directly. If a non-W3C propagator is configured (B3, Jaeger), it will
  // call get() with its own key names — Properties.getProperty will return
  // null for missing keys, which is the correct "not present" signal.
  // The keys() iterable is advisory and most propagators don't iterate it.

  private[propagation] val PropertiesGetter: TextMapGetter[ju.Properties] =
    new TextMapGetter[ju.Properties] {
      override def keys(carrier: ju.Properties): java.lang.Iterable[String] =
        ju.Arrays.asList(TraceparentKey, TracestateKey)

      override def get(carrier: ju.Properties, key: String): String =
        if (carrier == null) null
        else carrier.getProperty(key)
    }
}
