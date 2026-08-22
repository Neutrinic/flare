package io.flare.spark.metrics

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.{DoubleHistogram, LongCounter, LongUpDownCounter, Meter}

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

  // ── Cluster lifecycle (Issue #49) ──────────────────────────────────────
  //
  // Deliberately metrics, not spans. Executor lifetime is a level over time, not an
  // operation with a beginning and an end that a trace should describe. An executor
  // that lives for the whole application would otherwise be a span longer than every
  // trace it overlaps.
  //
  // UpDownCounter rather than a Counter: these go both ways, and what matters is the
  // standing value. Summing an increment-only counter would say how many executors
  // were ever created, never how many exist now.

  val executorCount: LongUpDownCounter = meter
    .upDownCounterBuilder("flare.executor.count")
    .setDescription("Executors currently registered with the driver")
    .setUnit("{executor}")
    .build()

  /** Removals keyed by reason — the whole point is telling a scale-down from a failure. */
  val executorRemoved: LongCounter = meter
    .counterBuilder("flare.executor.removed")
    .setDescription("Executors removed, by reason")
    .setUnit("{executor}")
    .build()

  val executorExcluded: LongCounter = meter
    .counterBuilder("flare.executor.excluded")
    .setDescription("Executors excluded by Spark's health tracker")
    .setUnit("{executor}")
    .build()

  val blockManagerCount: LongUpDownCounter = meter
    .upDownCounterBuilder("flare.block_manager.count")
    .setDescription("Block managers currently registered, including the driver's")
    .setUnit("{block_manager}")
    .build()

  val rddUnpersisted: LongCounter = meter
    .counterBuilder("flare.rdd.unpersisted")
    .setDescription("Explicit RDD unpersist calls")
    .setUnit("{rdd}")
    .build()

  // ── Block-level storage (Issue #49, opt-in) ────────────────────────────
  //
  // Behind FLARE_TRACK_BLOCK_UPDATES because SparkListenerBlockUpdated fires per block:
  // on a large cached dataset that is a firehose on the listener bus thread. Block ids
  // are NEVER used as attributes — that would be unbounded cardinality. Only running
  // totals per executor are kept, which is what "how much is cached, and is it spilling"
  // actually needs.

  val storageMemoryBytes: LongUpDownCounter = meter
    .upDownCounterBuilder("flare.storage.memory.bytes")
    .setDescription("Bytes currently held in memory by the block manager")
    .setUnit("By")
    .build()

  val storageDiskBytes: LongUpDownCounter = meter
    .upDownCounterBuilder("flare.storage.disk.bytes")
    .setDescription("Bytes currently spilled to disk by the block manager")
    .setUnit("By")
    .build()

  val storageBlocks: LongUpDownCounter = meter
    .upDownCounterBuilder("flare.storage.blocks")
    .setDescription("Blocks currently held by the block manager")
    .setUnit("{block}")
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
