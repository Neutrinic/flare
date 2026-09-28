package io.flare.spark.config

import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.sdk.autoconfigure.spi.{AutoConfigurationCustomizer, ConfigProperties}
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SpanProcessor
import io.opentelemetry.sdk.trace.`export`.SpanExporter
import io.opentelemetry.sdk.trace.samplers.Sampler
import munit.FunSuite

import java.util.function.{BiFunction, Supplier}
import java.{util => ju}

/** Which customizations `FlareAutoConfig` installs on the driver and on executors (#123, #127, #122). */
class FlareAutoConfigWiringTest extends FunSuite {

  private final class Recording extends AutoConfigurationCustomizer {
    var sampler = 0
    var spanProcessor = 0
    override def addPropagatorCustomizer(
      f: BiFunction[_ >: TextMapPropagator, ConfigProperties, _ <: TextMapPropagator]
    ): AutoConfigurationCustomizer = this
    override def addResourceCustomizer(
      f: BiFunction[_ >: Resource, ConfigProperties, _ <: Resource]
    ): AutoConfigurationCustomizer = this
    override def addSamplerCustomizer(
      f: BiFunction[_ >: Sampler, ConfigProperties, _ <: Sampler]
    ): AutoConfigurationCustomizer = { sampler += 1; this }
    override def addSpanExporterCustomizer(
      f: BiFunction[_ >: SpanExporter, ConfigProperties, _ <: SpanExporter]
    ): AutoConfigurationCustomizer = this
    override def addPropertiesSupplier(f: Supplier[ju.Map[String, String]]): AutoConfigurationCustomizer = this
    override def addSpanProcessorCustomizer(
      f: BiFunction[_ >: SpanProcessor, ConfigProperties, _ <: SpanProcessor]
    ): AutoConfigurationCustomizer = { spanProcessor += 1; this }
  }

  /** Runs `FlareAutoConfig.customize` as the JVM `command` would present itself. */
  private def customizeAs(command: String, drop: Option[String] = None): Recording = {
    val saved = sys.props.get("sun.java.command") -> sys.props.get("FLARE_DROP_NON_SPARK_ROOTS")
    sys.props("sun.java.command") = command
    drop.fold(sys.props.remove("FLARE_DROP_NON_SPARK_ROOTS"))(v => sys.props.put("FLARE_DROP_NON_SPARK_ROOTS", v))
    try {
      val recording = new Recording
      new FlareAutoConfig().customize(recording)
      recording
    } finally {
      saved._1.fold(sys.props.remove("sun.java.command"))(v => sys.props.put("sun.java.command", v))
      saved._2.fold(sys.props.remove("FLARE_DROP_NON_SPARK_ROOTS"))(v => sys.props.put("FLARE_DROP_NON_SPARK_ROOTS", v))
    }
  }

  private val executorCommand =
    "org.apache.spark.executor.CoarseGrainedExecutorBackend --driver-url spark://x --executor-id 3"
  private val driverCommand = "org.apache.spark.deploy.SparkSubmit --class com.example.Main app.jar"

  override def munitTests(): Seq[Test] =
    if (sys.env.contains("SPARK_EXECUTOR_ID")) Seq.empty // role would come from the environment
    else super.munitTests()

  test("the driver gets the root sampler and the shutdown wrapper") {
    val r = customizeAs(driverCommand)
    assertEquals(r.sampler, 1)
    assertEquals(r.spanProcessor, 1)
  }

  test("an executor gets the root sampler but not the driver's shutdown wrapper (#127)") {
    val r = customizeAs(executorCommand)
    assertEquals(r.sampler, 1, "executors must drop their own start-up traces too")
    assertEquals(r.spanProcessor, 0, "the shutdown wrapper ends driver spans and is driver-only")
  }

  test("FLARE_DROP_NON_SPARK_ROOTS=false turns the sampler off on both") {
    assertEquals(customizeAs(driverCommand, Some("false")).sampler, 0)
    assertEquals(customizeAs(executorCommand, Some("false")).sampler, 0)
  }
}
