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
import scala.collection.JavaConverters._

class FlareAutoConfigWiringTest extends FunSuite {

  private final class Recording extends AutoConfigurationCustomizer {
    var sampler = 0
    var spanProcessor = 0
    var resource = 0
    var properties = Map.empty[String, String]
    override def addPropagatorCustomizer(
      f: BiFunction[_ >: TextMapPropagator, ConfigProperties, _ <: TextMapPropagator]
    ): AutoConfigurationCustomizer = this
    override def addResourceCustomizer(
      f: BiFunction[_ >: Resource, ConfigProperties, _ <: Resource]
    ): AutoConfigurationCustomizer = { resource += 1; this }
    override def addSamplerCustomizer(
      f: BiFunction[_ >: Sampler, ConfigProperties, _ <: Sampler]
    ): AutoConfigurationCustomizer = { sampler += 1; this }
    override def addSpanExporterCustomizer(
      f: BiFunction[_ >: SpanExporter, ConfigProperties, _ <: SpanExporter]
    ): AutoConfigurationCustomizer = this
    override def addPropertiesSupplier(f: Supplier[ju.Map[String, String]]): AutoConfigurationCustomizer = {
      properties ++= f.get().asScala; this
    }
    override def addSpanProcessorCustomizer(
      f: BiFunction[_ >: SpanProcessor, ConfigProperties, _ <: SpanProcessor]
    ): AutoConfigurationCustomizer = { spanProcessor += 1; this }
  }

  /** Runs `FlareAutoConfig.customize` as the JVM `command` would present itself. */
  private def customizeAs(command: String, drop: Option[String] = None, enabled: Option[String] = None): Recording = {
    val saved = sys.props.get("sun.java.command") -> sys.props.get("FLARE_DROP_NON_SPARK_ROOTS")
    val savedEnabled = sys.props.get("FLARE_ENABLED")
    sys.props("sun.java.command") = command
    drop.fold(sys.props.remove("FLARE_DROP_NON_SPARK_ROOTS"))(v => sys.props.put("FLARE_DROP_NON_SPARK_ROOTS", v))
    enabled.fold(sys.props.remove("FLARE_ENABLED"))(v => sys.props.put("FLARE_ENABLED", v))
    try {
      val recording = new Recording
      new FlareAutoConfig().customize(recording)
      recording
    } finally {
      saved._1.fold(sys.props.remove("sun.java.command"))(v => sys.props.put("sun.java.command", v))
      saved._2.fold(sys.props.remove("FLARE_DROP_NON_SPARK_ROOTS"))(v => sys.props.put("FLARE_DROP_NON_SPARK_ROOTS", v))
      savedEnabled.fold(sys.props.remove("FLARE_ENABLED"))(v => sys.props.put("FLARE_ENABLED", v))
    }
  }

  private val executorCommand =
    "org.apache.spark.executor.CoarseGrainedExecutorBackend --driver-url spark://x --executor-id 3"
  private val driverCommand = "org.apache.spark.deploy.SparkSubmit --class com.example.Main app.jar"

  override def munitTests(): Seq[Test] =
    if (sys.env.contains("SPARK_EXECUTOR_ID")) Seq.empty // role would come from the environment
    else super.munitTests()

  // Guard, not evidence: the driver already had both before #127.
  test("the driver gets the root sampler and the shutdown wrapper") {
    val r = customizeAs(driverCommand)
    assertEquals(r.sampler, 1)
    assertEquals(r.spanProcessor, 1)
  }

  // The regression test for #127: the executor had no sampler before.
  test("an executor gets the root sampler but not the driver's shutdown wrapper (#127)") {
    val r = customizeAs(executorCommand)
    assertEquals(r.sampler, 1, "executors must drop their own start-up traces too")
    assertEquals(r.spanProcessor, 0, "the shutdown wrapper ends driver spans and is driver-only")
  }

  // Guard, not evidence: passes before #127 too, since executors had no sampler to turn off.
  test("FLARE_DROP_NON_SPARK_ROOTS=false turns the sampler off on both") {
    assertEquals(customizeAs(driverCommand, Some("false")).sampler, 0)
    assertEquals(customizeAs(executorCommand, Some("false")).sampler, 0)
  }

  test("FLARE_ENABLED=false keeps the agent quiet: Flare's agent defaults and root filter stay (#206)") {
    Seq(driverCommand, executorCommand).foreach { command =>
      val r = customizeAs(command, enabled = Some("false"))
      assertEquals(r.properties.get("otel.instrumentation.common.default-enabled"), Some("false"))
      assertEquals(r.sampler, 1, "the non-Spark root filter was dropped")
      assertEquals(r.resource, 0, "Flare's resource attributes were added while disabled")
      assertEquals(r.spanProcessor, 0, "the driver's shutdown wrapper was added while disabled")
    }
  }

  test("FLARE_ENABLED=false with FLARE_DROP_NON_SPARK_ROOTS=false has no root filter") {
    assertEquals(customizeAs(driverCommand, drop = Some("false"), enabled = Some("false")).sampler, 0)
  }
}
