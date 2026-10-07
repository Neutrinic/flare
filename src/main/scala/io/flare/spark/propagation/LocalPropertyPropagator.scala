package io.flare.spark.propagation

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.context.propagation.{TextMapGetter, TextMapSetter}
import io.opentelemetry.context.{Context => OtelContext}
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory

import java.{util => ju}
import java.util.Properties

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
 *
 * Each stage's context is also written under its own keys, `flare.stage.<stageId>.<key>` (#204).
 * Spark hands one job's `Properties` instance, uncloned, to every stage of the job, and a task reads
 * it only when it is launched. When a job submits two stages at once (any multi-parent shuffle), the
 * second stage's `traceparent` overwrites the first's before the first stage's tasks have launched,
 * so they would run under the wrong stage. Executors read their own stage's keys first and fall back
 * to the generic ones, which a driver without per-stage keys still writes.
 */
object LocalPropertyPropagator {

  private val logger = LoggerFactory.getLogger(getClass)

  // W3C standard header names, used as local property keys
  private val TraceparentKey = "traceparent"
  private val TracestateKey  = "tracestate"

  private def stagePrefix(stageId: Int): String = s"flare.stage.$stageId."

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
    val parentContext = overlayStage(
      propagator.extract(OtelContext.root(), taskContext, TaskContextGetter),
      taskContext, stageGetter(taskContext.stageId(), TaskContextGetter))

    if (parentContext == OtelContext.root()) {
      logger.debug("[Flare] No trace context found in task properties")
    } else {
      logger.debug("[Flare] Extracted trace context from task properties")
    }

    parentContext
  }

  /**
   * Inject OTEL context directly into a `java.util.Properties` instance.
   *
   * Used by `SubmitMissingTasksAdviceHelper` to inject per-stage traceparent
   * into `ActiveJob.properties` before tasks are created.
   */
  def injectIntoProperties(context: OtelContext, props: Properties): Unit = {
    val propagator = GlobalOpenTelemetry.getPropagators.getTextMapPropagator
    propagator.inject(context, props, PropertiesSetter)
  }

  /**
   * Inject a stage's context under that stage's own keys, and under the generic keys too.
   * See the object comment for why the generic keys alone are not enough (#204).
   */
  def injectForStage(context: OtelContext, props: Properties, stageId: Int): Unit = {
    val propagator = GlobalOpenTelemetry.getPropagators.getTextMapPropagator
    propagator.inject(context, props, PropertiesSetter)
    val prefix = stagePrefix(stageId)
    propagator.inject(context, props, (carrier: Properties, key: String, value: String) => {
      carrier.setProperty(prefix + key, value)
    })
  }

  /**
   * Remove a stage's own keys. Called for a stage's parents when the stage is submitted: Spark
   * submits a stage only once its parents have finished, so every task of theirs has launched and
   * no longer reads the job's properties. Without this, every task of a long job would carry the
   * keys of every stage before it.
   */
  def removeStage(props: Properties, stageId: Int): Unit = {
    val prefix = stagePrefix(stageId)
    props.stringPropertyNames().forEach { key =>
      if (key.startsWith(prefix)) props.remove(key)
    }
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

  /**
   * As [[extractFromProperties]], with the context written for `stageId` laid over the generic one
   * (#204). The stage's keys carry only its span, so anything else in the generic keys, such as
   * baggage the application set as a local property, is kept. Without stage keys, or with an
   * unknown stage (`stageId < 0`), this is the generic context.
   */
  def extractFromProperties(props: ju.Properties, stageId: Int): OtelContext = {
    if (props == null) return OtelContext.root()
    val generic = extractFromProperties(props)
    if (stageId < 0) generic else overlayStage(generic, props, stageGetter(stageId, PropertiesGetter))
  }

  /**
   * Extracts the stage's keys into `generic`. A propagator replaces only what it finds, so the
   * stage's span replaces the generic one, and baggage the stage keys do not carry is kept.
   */
  private def overlayStage[C](generic: OtelContext, carrier: C, stageKeys: TextMapGetter[C]): OtelContext =
    GlobalOpenTelemetry.getPropagators.getTextMapPropagator.extract(generic, carrier, stageKeys)

  /** Reads `key` as the stage's own key, `flare.stage.<stageId>.<key>`, through `getter`. */
  private def stageGetter[C](stageId: Int, getter: TextMapGetter[C]): TextMapGetter[C] = {
    val prefix = stagePrefix(stageId)
    new TextMapGetter[C] {
      override def keys(carrier: C): java.lang.Iterable[String] =
        ju.Arrays.asList(prefix + TraceparentKey, prefix + TracestateKey)

      override def get(carrier: C, key: String): String = getter.get(carrier, prefix + key)
    }
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

  // ── TextMapSetter for java.util.Properties ────────────────────────────────

  private val PropertiesSetter: TextMapSetter[Properties] =
    (carrier: Properties, key: String, value: String) => {
      carrier.setProperty(key, value)
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
