package io.flare.spark.listener

import io.flare.spark.config.{FlareConfig, TraceGranularity}
import io.flare.spark.metrics.FlareMetrics
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import io.opentelemetry.sdk.trace.SdkTracerProvider
import munit.FunSuite
import org.apache.spark.FlareTestHelpers

import scala.collection.JavaConverters._

/**
 * #49 — cluster lifecycle instruments.
 *
 * These are metrics, not spans, so the listener's span assertions do not cover them. What is
 * verified here is the part that could silently no-op: that each Spark callback reaches the
 * right instrument, that the up/down counters actually go back down, and that the block-update
 * firehose stays off unless explicitly enabled.
 */
class ClusterLifecycleMetricsTest extends FunSuite {

  private def baseConfig(trackBlocks: Boolean) = FlareConfig(
    enabled = true, granularity = TraceGranularity.All, maxSpansPerTrace = 10000, slowTaskMs = 0L, retryTasksOnly = false,
    taskStageIds = Set.empty, taskStagePattern = None, metricsEnabled = true,
    trackBlockUpdates = trackBlocks,
  )

  /** Runs `body` against a live listener and returns metric name -> summed long value. */
  private def collect(trackBlocks: Boolean = false)(
    body: TracingSparkListener => Unit
  ): Map[String, Long] = {
    val reader = InMemoryMetricReader.create()
    val mp = SdkMeterProvider.builder().registerMetricReader(reader).build()
    val tp = SdkTracerProvider.builder().build()
    try {
      val listener = new TracingSparkListener(
        tp.get("t"), baseConfig(trackBlocks),
        Some(new FlareMetrics(mp.get("io.flare.spark"))), throwOnError = true,
      )
      body(listener)
      reader.collectAllMetrics().asScala.map { m =>
        m.getName -> m.getLongSumData.getPoints.asScala.map(_.getValue).sum
      }.toMap
    } finally { tp.close(); mp.close() }
  }

  test("executor count rises on add and falls on remove") {
    val m = collect() { l =>
      l.onExecutorAdded(FlareTestHelpers.executorAdded("1"))
      l.onExecutorAdded(FlareTestHelpers.executorAdded("2"))
      l.onExecutorRemoved(FlareTestHelpers.executorRemoved("1", "Container killed by YARN"))
    }
    // Two up, one down. A plain Counter would report 3 here and never come back down.
    assertEquals(m.get("flare.executor.count"), Some(1L))
    assertEquals(m.get("flare.executor.removed"), Some(1L))
  }

  test("a decommissioned executor, removed twice by Spark, is counted once (#196)") {
    val reader = InMemoryMetricReader.create()
    val mp = SdkMeterProvider.builder().registerMetricReader(reader).build()
    val tp = SdkTracerProvider.builder().build()
    try {
      val l = new TracingSparkListener(tp.get("t"), baseConfig(false),
        Some(new FlareMetrics(mp.get("io.flare.spark"))), throwOnError = true)
      Seq("1", "2").foreach(id => l.onExecutorAdded(FlareTestHelpers.executorAdded(id)))
      // What Spark 4.0.4 posted for each decommissioned executor on the lab.
      l.onExecutorRemoved(FlareTestHelpers.executorRemoved("1", "Executor decommission: Executor 1 is decommissioned."))
      l.onExecutorRemoved(FlareTestHelpers.executorRemoved("1", "Command exited with code 0"))

      val points = reader.collectAllMetrics().asScala
      def sumOf(name: String) = points.filter(_.getName == name).flatMap(_.getLongSumData.getPoints.asScala)
      assertEquals(sumOf("flare.executor.count").map(_.getValue).sum, 1L) // executor 2 is still up
      val removed = sumOf("flare.executor.removed")
      assertEquals(removed.map(_.getValue).sum, 1L)
      assertEquals(removed.map(_.getAttributes.get(AttributeKey.stringKey("reason"))).toSet, Set("idle_or_decommissioned"))
    } finally { tp.close(); mp.close() }
  }

  test("an executor announced before the listener registered still has its removal counted") {
    // Under automatic initialization the listener registers as the SparkContext constructor
    // returns, after the startup executors were announced: on YARN and Kubernetes, most of them.
    val m = collect() { l =>
      l.onExecutorRemoved(FlareTestHelpers.executorRemoved("7", "Container marked as failed, exit code 137"))
      l.onExecutorRemoved(FlareTestHelpers.executorRemoved("7", "Command exited with code 137"))
    }
    assertEquals(m.get("flare.executor.removed"), Some(1L)) // counted, once
    assertEquals(m.getOrElse("flare.executor.count", 0L), 0L) // never seen added, so not decremented
  }

  test("with metrics off, removed executors are still forgotten") {
    val tp = SdkTracerProvider.builder().build()
    try {
      val l = new TracingSparkListener(tp.get("t"), baseConfig(false), metrics = None, throwOnError = true)
      (1 to 50).foreach { i =>
        l.onExecutorAdded(FlareTestHelpers.executorAdded(i.toString))
        l.onExecutorRemoved(FlareTestHelpers.executorRemoved(i.toString, "Executor idle timeout exceeded"))
      }
      assertEquals(l.liveExecutorCount, 0)
    } finally tp.close()
  }

  test("tasks lost with their executor are counted on the driver (#200)") {
    val reader = InMemoryMetricReader.create()
    val mp = SdkMeterProvider.builder().registerMetricReader(reader).build()
    val tp = SdkTracerProvider.builder().build()
    try {
      val l = new TracingSparkListener(tp.get("t"), baseConfig(false),
        Some(new FlareMetrics(mp.get("io.flare.spark"))), throwOnError = true)
      // Two tasks on an executor that was killed, one on another whose heartbeats stopped.
      l.onTaskEnd(FlareTestHelpers.executorLostTaskEnd("4", "Command exited with code 137"))
      l.onTaskEnd(FlareTestHelpers.executorLostTaskEnd("4", "Command exited with code 137"))
      l.onTaskEnd(FlareTestHelpers.executorLostTaskEnd("5", "Executor heartbeat timed out after 120000 ms"))

      val lost = reader.collectAllMetrics().asScala.filter(_.getName == "flare.task.lost")
        .flatMap(_.getLongSumData.getPoints.asScala)
        .map(p => (p.getAttributes.get(AttributeKey.stringKey("executor.id")), p.getAttributes.get(AttributeKey.stringKey("reason"))) -> p.getValue)
        .toMap
      assertEquals(lost, Map(("4", "exited") -> 2L, ("5", "heartbeat_timeout") -> 1L))
    } finally { tp.close(); mp.close() }
  }

  test("seeded executors are counted once, and an executor already removed is not seeded (#224)") {
    val m = collect() { l =>
      l.onExecutorRemoved(FlareTestHelpers.executorRemoved("3", "Executor idle timeout exceeded"))
      l.seedExecutors(Seq("1", "2", "3")) // as the advice reads them after registering
      l.onExecutorAdded(FlareTestHelpers.executorAdded("2")) // an event that also got through
      l.onExecutorRemoved(FlareTestHelpers.executorRemoved("1", "Command exited with code 137"))
    }
    // 1 and 2 seeded (3 was already gone), 2 not counted twice, 1 removed: one live executor.
    assertEquals(m.get("flare.executor.count"), Some(1L))
    assertEquals(m.get("flare.executor.removed"), Some(2L))
  }

  test("executor removals are tagged with a bucketed reason") {
    val reader = InMemoryMetricReader.create()
    val mp = SdkMeterProvider.builder().registerMetricReader(reader).build()
    val tp = SdkTracerProvider.builder().build()
    try {
      val l = new TracingSparkListener(tp.get("t"), baseConfig(false),
        Some(new FlareMetrics(mp.get("io.flare.spark"))), throwOnError = true)
      Seq("1", "2").foreach(id => l.onExecutorAdded(FlareTestHelpers.executorAdded(id)))
      l.onExecutorRemoved(FlareTestHelpers.executorRemoved("1", "Executor idle timeout exceeded"))
      l.onExecutorRemoved(FlareTestHelpers.executorRemoved("2", "Container marked as failed, exit code 137"))

      val reasons = reader.collectAllMetrics().asScala
        .filter(_.getName == "flare.executor.removed")
        .flatMap(_.getLongSumData.getPoints.asScala)
        .flatMap(_.getAttributes.asMap.asScala.collect { case (k, v) if k.getKey == "reason" => v.toString })
        .toSet

      // The distinction that matters: a routine scale-down must not look like a crash.
      assert(reasons.contains("idle_or_decommissioned"), s"got $reasons")
      assert(reasons.contains("exited"), s"got $reasons")
    } finally { tp.close(); mp.close() }
  }

  test("block manager count and rdd unpersist are recorded") {
    val m = collect() { l =>
      l.onBlockManagerAdded(FlareTestHelpers.blockManagerAdded("1"))
      l.onBlockManagerAdded(FlareTestHelpers.blockManagerAdded("2"))
      l.onBlockManagerRemoved(FlareTestHelpers.blockManagerRemoved("2"))
      l.onUnpersistRDD(FlareTestHelpers.unpersistRDD(7))
    }
    assertEquals(m.get("flare.block_manager.count"), Some(1L))
    assertEquals(m.get("flare.rdd.unpersisted"), Some(1L))
  }

  // The gate is the whole reason this event is safe to implement at all.
  test("block updates are ignored unless FLARE_TRACK_BLOCK_UPDATES is set") {
    val m = collect(trackBlocks = false) { l =>
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 4096L, 0L, cached = true))
    }
    assertEquals(m.get("flare.storage.memory.bytes"), None)
    assertEquals(m.get("flare.storage.blocks"), None)
  }

  test("block updates track running totals when enabled, and unwind on drop") {
    val m = collect(trackBlocks = true) { l =>
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 4096L, 1024L, cached = true, partition = 0))
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 2048L, 0L, cached = true, partition = 1))
      // An invalid StorageLevel is Spark's drop signal, whatever sizes it carries.
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 4096L, 1024L, cached = false, partition = 0))
    }
    assertEquals(m.get("flare.storage.memory.bytes"), Some(2048L))
    assertEquals(m.get("flare.storage.disk.bytes"), Some(0L))
    assertEquals(m.get("flare.storage.blocks"), Some(1L))
  }

  // #179. An update reports a block's state; adding each one counted a re-reported block twice.
  test("a block reported twice and then dropped leaves nothing behind") {
    val m = collect(trackBlocks = true) { l =>
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 4096L, 0L, cached = true))
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 4096L, 0L, cached = true))
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 0L, 0L, cached = false))
    }
    assertEquals(m.get("flare.storage.memory.bytes"), Some(0L))
    assertEquals(m.get("flare.storage.blocks"), Some(0L))
  }

  test("a block moving from memory to disk moves its bytes, and stays one block") {
    val m = collect(trackBlocks = true) { l =>
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 4096L, 0L, cached = true))
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 0L, 4096L, cached = true))
    }
    assertEquals(m.get("flare.storage.memory.bytes"), Some(0L))
    assertEquals(m.get("flare.storage.disk.bytes"), Some(4096L))
    assertEquals(m.get("flare.storage.blocks"), Some(1L))
  }

  // Spark's eviction report for a block moved from memory to disk carries the old memory size with
  // a DISK_ONLY level. Counting that size would show the block in memory and on disk at once.
  test("a block evicted to disk counts on disk only, though the report carries its memory size") {
    import org.apache.spark.storage.StorageLevel
    val m = collect(trackBlocks = true) { l =>
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 4096L, 0L, cached = true, storageLevel = Some(StorageLevel.MEMORY_ONLY)))
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 4096L, 4096L, cached = true, storageLevel = Some(StorageLevel.DISK_ONLY)))
    }
    assertEquals(m.get("flare.storage.memory.bytes"), Some(0L))
    assertEquals(m.get("flare.storage.disk.bytes"), Some(4096L))
    assertEquals(m.get("flare.storage.blocks"), Some(1L))
  }

  // #179. No block update follows a block manager's removal; its blocks went with it.
  test("removing a block manager removes its blocks from the totals") {
    val m = collect(trackBlocks = true) { l =>
      l.onBlockManagerAdded(FlareTestHelpers.blockManagerAdded("1"))
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 4096L, 1024L, cached = true, partition = 0))
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 2048L, 0L, cached = true, partition = 1))
      l.onBlockUpdated(FlareTestHelpers.blockUpdated("1", 0L, 512L, cached = true, partition = 2))
      l.onBlockManagerRemoved(FlareTestHelpers.blockManagerRemoved("1"))
    }
    assertEquals(m.get("flare.storage.memory.bytes"), Some(0L))
    assertEquals(m.get("flare.storage.disk.bytes"), Some(0L))
    assertEquals(m.get("flare.storage.blocks"), Some(0L))
  }
}
