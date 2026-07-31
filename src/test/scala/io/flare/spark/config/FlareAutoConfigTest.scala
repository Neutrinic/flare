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
    assertEquals(
      attributes.get(AttributeKey.stringKey("flare.role")),
      "driver",
      "a JVM with no executor identity must report the driver role",
    )
  }

  test("standalone executor identity is recovered from the JVM command line") {
    val command =
      "org.apache.spark.executor.CoarseGrainedExecutorBackend " +
        "--driver-url spark://CoarseGrainedScheduler@spark-master:42649 " +
        "--executor-id 7 --hostname 172.19.0.9 --cores 24 " +
        "--app-id app-20260731150530-0000 " +
        "--worker-url spark://Worker@172.19.0.9:43807 --resourceProfileId 0"

    assertEquals(FlareAutoConfig.executorIdFromCommand(command), "7")
  }

  test("YARN executor backend is recognised as an executor") {
    val command =
      "org.apache.spark.executor.YarnCoarseGrainedExecutorBackend " +
        "--driver-url spark://CoarseGrainedScheduler@host:1234 --executor-id 2 --cores 4"

    assertEquals(FlareAutoConfig.executorIdFromCommand(command), "2")
  }

  test("driver command lines yield no executor id") {
    val command =
      "org.apache.spark.deploy.SparkSubmit --master spark://spark-master:7077 " +
        "--class io.flare.examples.PipelineJob /opt/flare/flare-examples.jar"

    assertEquals(FlareAutoConfig.executorIdFromCommand(command), null)
    assertEquals(FlareAutoConfig.executorIdFromCommand(null), null)
  }

  test("executor backend without a parsable id does not fabricate one") {
    assertEquals(
      FlareAutoConfig.executorIdFromCommand(
        "org.apache.spark.executor.CoarseGrainedExecutorBackend --cores 4 --executor-id",
      ),
      null,
    )
  }
}
