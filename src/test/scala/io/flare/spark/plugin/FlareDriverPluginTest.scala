package io.flare.spark.plugin

import io.flare.spark.attributes.SparkAttributes.Application
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import munit.FunSuite
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

/**
 * The driver plugin in a real SparkContext, installed through `spark.plugins` as the quick start
 * documents, with a test SDK as the global OpenTelemetry.
 */
class FlareDriverPluginTest extends FunSuite {

  // Flare's driver state is a JVM-wide singleton that other suites initialise; one they leave
  // behind would make this SparkContext skip Flare's initialisation.
  override def beforeEach(context: BeforeEach): Unit = {
    FlareDriverState.reset()
    super.beforeEach(context)
  }

  override def afterEach(context: AfterEach): Unit = {
    FlareDriverState.reset()
    GlobalOpenTelemetry.resetForTest()
    super.afterEach(context)
  }

  test("the application span carries the application id when Flare is installed with spark.plugins (#240)") {
    // Spark initialises plugins before it assigns the application id, so the id cannot be read
    // when the span is created.
    val exporter = InMemorySpanExporter.create()
    val sdk = OpenTelemetrySdk.builder()
      .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build())
      .buildAndRegisterGlobal()
    try {
      val sc = new SparkContext(
        new SparkConf()
          .setMaster("local[1]")
          .setAppName("flare-application-id")
          .set("spark.ui.enabled", "false")
          .set("spark.plugins", "io.flare.spark.plugin.FlareSparkPlugin"))
      val appId = sc.applicationId
      try sc.parallelize(Seq(1, 2), 1).count()
      finally sc.stop()

      val app = exporter.getFinishedSpanItems.asScala.find(_.getName == "spark.application")
      assert(app.isDefined, "no spark.application span was exported")
      assertEquals(app.get.getAttributes.get(Application.Id), appId)
    } finally sdk.close()
  }
}
