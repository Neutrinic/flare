package io.flare.spark.config

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider
import munit.FunSuite

import java.util.ServiceLoader

class FlareAutoConfigTest extends FunSuite {

  test("agent auto-configuration SPI discovers the Java Flare provider") {
    val providers = scala.collection.mutable.ListBuffer.empty[FlareAutoConfig]
    val iterator = ServiceLoader
      .load(
        classOf[AutoConfigurationCustomizerProvider],
        getClass.getClassLoader,
      )
      .iterator()

    while (iterator.hasNext) {
      iterator.next() match {
        case provider: FlareAutoConfig => providers += provider
        case _                         => ()
      }
    }

    assertEquals(providers.size, 1)
  }

  test("generated build metadata supplies stable Flare resource attributes") {
    val attributes = FlareAutoConfig.flareResourceAttributes()

    assertEquals(
      attributes.get(AttributeKey.stringKey("flare.version")),
      sys.props("flare.test.expected.version"),
    )
    assert(
      Option(attributes.get(AttributeKey.stringKey("flare.role"))).exists(_.nonEmpty),
      "flare.role must be present",
    )
  }
}
