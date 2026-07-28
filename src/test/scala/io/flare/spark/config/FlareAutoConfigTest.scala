package io.flare.spark.config

import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider
import munit.FunSuite

import java.util.ServiceLoader
import scala.collection.JavaConverters._

class FlareAutoConfigTest extends FunSuite {

  private val receiveTelemetryProperty = FlareAutoConfig.RECEIVE_TELEMETRY_PROPERTY
  private val managedProperties = List(
    receiveTelemetryProperty,
    "FLARE_ENABLED",
  )

  override def afterEach(context: AfterEach): Unit = {
    managedProperties.foreach(sys.props.remove)
    super.afterEach(context)
  }

  test("agent auto-configuration SPI discovers the Java Flare provider") {
    val providers = ServiceLoader
      .load(
        classOf[AutoConfigurationCustomizerProvider],
        getClass.getClassLoader,
      )
      .iterator()
      .asScala
      .collect { case provider: FlareAutoConfig => provider }
      .toList

    assertEquals(providers.size, 1)
  }

  test("receive telemetry is enabled by default") {
    assertEquals(resolveReceiveTelemetry(), true)
  }

  test("an explicit OpenTelemetry property overrides the Flare default") {
    sys.props(receiveTelemetryProperty) = "false"

    assertEquals(resolveReceiveTelemetry(), false)
  }

  test("FLARE_ENABLED=false does not change OpenTelemetry messaging defaults") {
    sys.props("FLARE_ENABLED") = "false"

    assertEquals(resolveReceiveTelemetry(), false)
  }

  private def resolveReceiveTelemetry(): Boolean = {
    var resolved = false

    val configured = AutoConfiguredOpenTelemetrySdk.builder()
      .setServiceClassLoader(getClass.getClassLoader)
      .disableShutdownHook()
      .addPropertiesSupplier(() => Map(
        "otel.traces.exporter" -> "none",
        "otel.metrics.exporter" -> "none",
        "otel.logs.exporter" -> "none",
      ).asJava)
      .addResourceCustomizer { (resource, config) =>
        resolved = config.getBoolean(receiveTelemetryProperty, false)
        resource
      }
      .build()

    configured.getOpenTelemetrySdk.close()
    resolved
  }
}
