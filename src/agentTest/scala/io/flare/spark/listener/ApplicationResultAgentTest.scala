package io.flare.spark.listener

import io.flare.spark.config.{FlareConfig, TraceGranularity}
import io.flare.spark.metrics.FlareMetrics
import io.flare.spark.trace.StubCollector
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import io.opentelemetry.sdk.trace.SdkTracerProvider
import munit.FunSuite
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

/**
 * `application.result` against a real SparkContext's event order (#197).
 *
 * The listener reads a job still in flight at the application's end as a killed run. That is only
 * sound if a normal `sc.stop()` never reaches the application end with a job open: Spark cancels
 * running jobs as it stops, and their ends must reach the listener first. These pin that order.
 */
class ApplicationResultAgentTest extends FunSuite {

  private val config = FlareConfig(
    enabled = true, granularity = TraceGranularity.Stages, maxSpansPerTrace = 10000, slowTaskMs = 0L,
    retryTasksOnly = false, taskStageIds = Set.empty, taskStagePattern = None, metricsEnabled = true,
  )

  // StubCollector: the agent exports while the context runs, and failed exports back off (see it).
  private def resultOf(body: SparkContext => Unit): Seq[String] = StubCollector.during {
    val reader = InMemoryMetricReader.create()
    val mp = SdkMeterProvider.builder().registerMetricReader(reader).build()
    val tp = SdkTracerProvider.builder().build()
    try {
      val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("application-result")
        .set("spark.ui.enabled", "false"))
      sc.addSparkListener(new TracingSparkListener(tp.get("t"), config,
        Some(new FlareMetrics(mp.get("io.flare.spark"))), throwOnError = true))
      body(sc) // stops the context
      reader.collectAllMetrics().asScala.filter(_.getName == "flare.application.duration")
        .flatMap(_.getHistogramData.getPoints.asScala)
        .map(_.getAttributes.asMap.asScala.map(_._2.toString).mkString)
        .toSeq
    } finally { tp.close(); mp.close() }
  }

  test("a normal stop after its jobs finished reads SUCCESS") {
    assertEquals(resultOf { sc => sc.parallelize(1 to 100, 4).count(); sc.stop() }, Seq("SUCCESS"))
  }

  test("a stop that cancels a running job reads FAILED, from the cancelled job's end") {
    assertEquals(resultOf { sc =>
      val running = new Thread(() =>
        try sc.parallelize(1 to 4, 4).foreach(_ => Thread.sleep(60000L)) catch { case _: Exception => () })
      running.start()
      val deadline = System.nanoTime() + 30000000000L
      while (sc.statusTracker.getActiveJobIds().isEmpty && System.nanoTime() < deadline) Thread.sleep(50)
      assert(sc.statusTracker.getActiveJobIds().nonEmpty, "the job never started")
      sc.stop()
      running.join(30000L)
    }, Seq("FAILED"))
  }
}
