package io.flare.spark.metrics

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import munit.FunSuite

import scala.collection.JavaConverters._

class FlareMetricsTest extends FunSuite {

  private def withMetrics(body: (FlareMetrics, InMemoryMetricReader) => Unit): Unit = {
    val reader = InMemoryMetricReader.create()
    val meterProvider = SdkMeterProvider.builder()
      .registerMetricReader(reader)
      .build()
    val meter = meterProvider.get("io.flare.spark")
    val metrics = new FlareMetrics(meter)
    try body(metrics, reader)
    finally meterProvider.close()
  }

  test("taskDuration histogram records a value") {
    withMetrics { (metrics, reader) =>
      val attrs = MetricAttributes.forTask("exec-0", 3, "SUCCESS")
      metrics.taskDuration.record(150.0, attrs)

      val metricData = reader.collectAllMetrics().asScala
      val histogram = metricData.find(_.getName == "spark.task.duration")
      assert(histogram.isDefined, s"Expected spark.task.duration, got: ${metricData.map(_.getName).mkString(", ")}")
    }
  }

  test("taskRecordsThroughput histogram records a value") {
    withMetrics { (metrics, reader) =>
      val attrs = MetricAttributes.forTask("exec-0", 1, "SUCCESS")
      metrics.taskRecordsThroughput.record(5000.0, attrs)

      val metricData = reader.collectAllMetrics().asScala
      val histogram = metricData.find(_.getName == "spark.task.records_throughput")
      assert(histogram.isDefined)
    }
  }

  test("taskShuffleReadBytes counter increments") {
    withMetrics { (metrics, reader) =>
      val attrs = MetricAttributes.forTask("exec-1", 5, "SUCCESS")
      metrics.taskShuffleReadBytes.add(1024L, attrs)
      metrics.taskShuffleReadBytes.add(2048L, attrs)

      val metricData = reader.collectAllMetrics().asScala
      val counter = metricData.find(_.getName == "spark.task.shuffle.read_bytes")
      assert(counter.isDefined, s"Expected spark.task.shuffle.read_bytes, got: ${metricData.map(_.getName).mkString(", ")}")
    }
  }

  test("stage metrics record correctly") {
    withMetrics { (metrics, reader) =>
      val attrs = MetricAttributes.forStage(7, "shuffle read")
      metrics.stageExecutorRunTime.record(3500.0, attrs)
      metrics.stageInputBytes.add(1048576L, attrs)
      metrics.stageShuffleReadBytes.add(512000L, attrs)

      val metricData = reader.collectAllMetrics().asScala
      val names = metricData.map(_.getName).toSet
      assert(names.contains("spark.stage.executor.run_time"))
      assert(names.contains("spark.stage.input.bytes"))
      assert(names.contains("spark.stage.shuffle.read_bytes"))
    }
  }

  test("noop metrics discard all recordings") {
    val noopMeter = OpenTelemetry.noop().getMeter("io.flare.spark")
    val metrics = new FlareMetrics(noopMeter)

    // These should not throw — no-op instruments silently discard
    val attrs = MetricAttributes.forTask("exec-0", 1, "SUCCESS")
    metrics.taskDuration.record(100.0, attrs)
    metrics.taskShuffleReadBytes.add(1024L, attrs)
    metrics.taskRecordsThroughput.record(5000.0, attrs)
  }

  test("FlareMetrics.create(enabled=false) returns noop instance") {
    // When disabled, instruments are backed by no-op meter.
    // Just verify it doesn't throw.
    val metrics = FlareMetrics.create(enabled = false)
    val attrs = MetricAttributes.forTask("exec-0", 1, "SUCCESS")
    metrics.taskDuration.record(100.0, attrs)
  }

  test("MetricAttributes.forTask includes correct keys") {
    val attrs = MetricAttributes.forTask("exec-2", 10, "FAILED")
    val attrMap = new java.util.HashMap[String, Any]()
    attrs.forEach((k, v) => attrMap.put(k.getKey, v))

    assertEquals(attrMap.get("executor.id"), "exec-2")
    assertEquals(attrMap.get("stage.id"), 10L: java.lang.Long)
    assertEquals(attrMap.get("task.result"), "FAILED")
  }

  test("MetricAttributes.forStage includes correct keys") {
    val attrs = MetricAttributes.forStage(7, "shuffle read")
    val attrMap = new java.util.HashMap[String, Any]()
    attrs.forEach((k, v) => attrMap.put(k.getKey, v))

    assertEquals(attrMap.get("stage.id"), 7L: java.lang.Long)
    assertEquals(attrMap.get("stage.name"), "shuffle read")
  }
}
