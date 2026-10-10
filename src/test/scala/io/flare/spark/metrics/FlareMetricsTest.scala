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
      val attrs = MetricAttributes.forTask("exec-0", "SUCCESS")
      metrics.taskDuration.record(150.0, attrs)

      val metricData = reader.collectAllMetrics().asScala
      val histogram = metricData.find(_.getName == "flare.task.duration")
      assert(histogram.isDefined, s"Expected spark.task.duration, got: ${metricData.map(_.getName).mkString(", ")}")
    }
  }

  // #180. The SDK's default boundaries stop at 10,000, so anything longer than ten seconds landed in
  // the overflow bucket and every percentile above it read as 10 s.
  test("duration histograms resolve minutes, not just the first ten seconds") {
    withMetrics { (fm, reader) =>
      fm.taskDuration.record(90000.0, MetricAttributes.forTask("1", "SUCCESS"))
      fm.stageExecutorRunTime.record(7200000.0, MetricAttributes.forStage("collect at A.scala:1", None))
      fm.taskRecordsThroughput.record(2500000.0, MetricAttributes.forTask("1", "SUCCESS"))
      val points = reader.collectAllMetrics().asScala
        .map(m => m.getName -> m.getHistogramData.getPoints.asScala.head).toMap

      def bucketOf(name: String): (Double, Double) = {
        val p      = points(name)
        val bounds = p.getBoundaries.asScala.map(_.doubleValue).toVector
        val i      = p.getCounts.asScala.indexWhere(_ > 0)
        assert(i < bounds.size, s"$name landed in the overflow bucket above ${bounds.last}")
        (if (i == 0) 0.0 else bounds(i - 1), bounds(i))
      }
      assertEquals(bucketOf("flare.task.duration"), (60000.0, 120000.0))
      assertEquals(bucketOf("flare.stage.executor.run_time"), (3600000.0, 10800000.0))
      assertEquals(bucketOf("flare.task.records_throughput"), (1000000.0, 10000000.0))
    }
  }

  test("taskRecordsThroughput histogram records a value") {
    withMetrics { (metrics, reader) =>
      val attrs = MetricAttributes.forTask("exec-0", "SUCCESS")
      metrics.taskRecordsThroughput.record(5000.0, attrs)

      val metricData = reader.collectAllMetrics().asScala
      val histogram = metricData.find(_.getName == "flare.task.records_throughput")
      assert(histogram.isDefined)
    }
  }

  test("taskShuffleReadBytes counter increments") {
    withMetrics { (metrics, reader) =>
      val attrs = MetricAttributes.forTask("exec-1", "SUCCESS")
      metrics.taskShuffleReadBytes.add(1024L, attrs)
      metrics.taskShuffleReadBytes.add(2048L, attrs)

      val metricData = reader.collectAllMetrics().asScala
      val counter = metricData.find(_.getName == "flare.task.shuffle.read_bytes")
      assert(counter.isDefined, s"Expected spark.task.shuffle.read_bytes, got: ${metricData.map(_.getName).mkString(", ")}")
    }
  }

  test("stage metrics record correctly") {
    withMetrics { (metrics, reader) =>
      val attrs = MetricAttributes.forStage("shuffle read", None)
      metrics.stageExecutorRunTime.record(3500.0, attrs)
      metrics.stageInputBytes.add(1048576L, attrs)
      metrics.stageShuffleReadBytes.add(512000L, attrs)

      val metricData = reader.collectAllMetrics().asScala
      val names = metricData.map(_.getName).toSet
      assert(names.contains("flare.stage.executor.run_time"))
      assert(names.contains("flare.stage.input.bytes"))
      assert(names.contains("flare.stage.shuffle.read_bytes"))
    }
  }

  test("noop metrics discard all recordings") {
    val noopMeter = OpenTelemetry.noop().getMeter("io.flare.spark")
    val metrics = new FlareMetrics(noopMeter)

    // These should not throw — no-op instruments silently discard
    val attrs = MetricAttributes.forTask("exec-0", "SUCCESS")
    metrics.taskDuration.record(100.0, attrs)
    metrics.taskShuffleReadBytes.add(1024L, attrs)
    metrics.taskRecordsThroughput.record(5000.0, attrs)
  }

  test("FlareMetrics.create(enabled=false) returns noop instance") {
    // When disabled, instruments are backed by no-op meter.
    // Just verify it doesn't throw.
    val metrics = FlareMetrics.create(enabled = false)
    val attrs = MetricAttributes.forTask("exec-0", "SUCCESS")
    metrics.taskDuration.record(100.0, attrs)
  }

  test("MetricAttributes.forTask includes correct keys") {
    val attrs = MetricAttributes.forTask("exec-2", "FAILED")
    val attrMap = new java.util.HashMap[String, Any]()
    attrs.forEach((k, v) => attrMap.put(k.getKey, v))

    assertEquals(attrMap.get("executor.id"), "exec-2")
    assertEquals(attrMap.get("task.result"), "FAILED")
    // #136: nothing that is new for every stage.
    assertEquals(attrMap.keySet.asScala.toSet, Set("executor.id", "task.result"))
  }

  test("MetricAttributes.forStage includes correct keys") {
    val attrs = MetricAttributes.forStage("shuffle read", None)
    val attrMap = new java.util.HashMap[String, Any]()
    attrs.forEach((k, v) => attrMap.put(k.getKey, v))

    assertEquals(attrMap.get("stage.name"), "shuffle read")
    assertEquals(attrMap.get("stage.id"), null) // #136
  }

  // #75. Spark's own stage name is useless for async subquery / broadcast stages, and the
  // dashboard groups on it. sql.description carries the query label instead.
  test("MetricAttributes.forStage carries sql.description when the stage is part of a query") {
    val attrs = MetricAttributes.forStage(
      "$anonfun$withThreadLocalCaptured$2", Some("show at PipelineJob.scala:58"),
    )
    val attrMap = new java.util.HashMap[String, Any]()
    attrs.forEach((k, v) => attrMap.put(k.getKey, v))

    assertEquals(attrMap.get("sql.description"), "show at PipelineJob.scala:58")
    // Spark's own name is untouched, so Spark UI correlation still works.
    assertEquals(attrMap.get("stage.name"), "$anonfun$withThreadLocalCaptured$2")
  }

  // A pure-RDD stage has no description. Omitted rather than blank, so "no query" never reads
  // as "a query with an empty label".
  test("MetricAttributes.forStage omits sql.description for a stage outside any query") {
    val attrMap = new java.util.HashMap[String, Any]()
    MetricAttributes.forStage("shuffle read", None).forEach((k, v) => attrMap.put(k.getKey, v))
    assertEquals(attrMap.get("sql.description"), null)

    val blank = new java.util.HashMap[String, Any]()
    MetricAttributes.forStage("shuffle read", Some("")).forEach((k, v) => blank.put(k.getKey, v))
    assertEquals(blank.get("sql.description"), null)
  }

  // #49. Spark's removal reason is free text and sometimes embeds ids or hostnames, so it is
  // bucketed before becoming a metric tag. Unbounded tag values on a counter are exactly the
  // cardinality problem these instruments exist to avoid.
  test("removal reason bucketing is total and never returns free text") {
    val cases = Map(
      ""                                 -> "unknown",
      "Executor idle timeout exceeded"   -> "idle_or_decommissioned",
      "Executor decommissioned"          -> "idle_or_decommissioned",
      "Container preempted by scheduler" -> "preempted",
      "Executor heartbeat timed out"     -> "heartbeat_timeout",
      "Slave lost"                       -> "lost",
      "Container killed by YARN"         -> "killed",
      "Container exited with code 137"   -> "exited",
      "something nobody predicted"       -> "other",
    )
    cases.foreach { case (in, want) =>
      assertEquals(MetricAttributes.bucketRemovalReason(in), want, s"input: '$in'")
    }
    assertEquals(MetricAttributes.bucketRemovalReason(null), "unknown")
  }

  test("utf8Prefix bounds UTF-8 bytes and never splits a character") {
    val grin = new String(Character.toChars(0x1F600)) // 4 bytes, two Java chars
    assertEquals(MetricAttributes.utf8Prefix("a" + grin * 3, 6), "a" + grin)
    assertEquals(MetricAttributes.utf8Prefix("数据", 5), "数") // 3 bytes each
    assertEquals(MetricAttributes.utf8Prefix("plain", 2048), "plain")
  }

  test("a character cap never ends inside a surrogate pair") {
    val grin = new String(Character.toChars(0x1F600))
    assertEquals(io.flare.spark.attributes.FailureDetail.capped("ab" + grin, 3), Some("ab"))
  }
}
