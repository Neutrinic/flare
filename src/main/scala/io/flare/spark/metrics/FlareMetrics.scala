package io.flare.spark.metrics

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.{DoubleHistogram, LongCounter, Meter}

/**
 * Holder for all Flare OTEL metric instruments.
 *
 * Constructed from a `Meter` instance — production code uses `GlobalOpenTelemetry.getMeter()`,
 * tests can pass a meter backed by `InMemoryMetricReader`.
 *
 * When metrics are disabled, the meter is a no-op instance so all recording calls
 * are zero-cost no-ops without per-call `if` checks.
 *
 * Instruments are thread-safe by OTEL contract.
 */
class FlareMetrics(meter: Meter) {

  // ── Executor-side (Issue #11) ──────────────────────────────────────────

  val taskDuration: DoubleHistogram = meter
    .histogramBuilder("flare.task.duration")
    .setDescription("Task execution duration")
    .setUnit("ms")
    .build()

  val taskRecordsThroughput: DoubleHistogram = meter
    .histogramBuilder("flare.task.records_throughput")
    .setDescription("Task records processed per second")
    .setUnit("{records}/s")
    .build()

  val taskShuffleReadBytes: LongCounter = meter
    .counterBuilder("flare.task.shuffle.read_bytes")
    .setDescription("Bytes read during shuffle by individual tasks")
    .setUnit("By")
    .build()

  val taskShuffleWriteBytes: LongCounter = meter
    .counterBuilder("flare.task.shuffle.write_bytes")
    .setDescription("Bytes written during shuffle by individual tasks")
    .setUnit("By")
    .build()

  // ── Driver-side (Issue #10 partial) ────────────────────────────────────

  val stageExecutorRunTime: DoubleHistogram = meter
    .histogramBuilder("flare.stage.executor.run_time")
    .setDescription("Total executor run time per stage")
    .setUnit("ms")
    .build()

  val stageInputBytes: LongCounter = meter
    .counterBuilder("flare.stage.input.bytes")
    .setDescription("Total bytes read across all tasks in a stage")
    .setUnit("By")
    .build()

  val stageOutputBytes: LongCounter = meter
    .counterBuilder("flare.stage.output.bytes")
    .setDescription("Total bytes written across all tasks in a stage")
    .setUnit("By")
    .build()

  val stageShuffleReadBytes: LongCounter = meter
    .counterBuilder("flare.stage.shuffle.read_bytes")
    .setDescription("Total shuffle bytes read in a stage")
    .setUnit("By")
    .build()

  val stageShuffleWriteBytes: LongCounter = meter
    .counterBuilder("flare.stage.shuffle.write_bytes")
    .setDescription("Total shuffle bytes written in a stage")
    .setUnit("By")
    .build()
}

object FlareMetrics {

  /**
   * Create a `FlareMetrics` instance. When `enabled` is false, returns an instance
   * backed by a no-op meter so all recording calls silently discard data.
   */
  def create(enabled: Boolean): FlareMetrics = {
    val meter = if (enabled)
      GlobalOpenTelemetry.getMeter("io.flare.spark")
    else
      OpenTelemetry.noop().getMeter("io.flare.spark")
    new FlareMetrics(meter)
  }
}
