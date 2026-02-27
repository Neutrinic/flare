package io.flare.spark.config

import io.flare.spark.BuildInfo
import io.opentelemetry.sdk.autoconfigure.spi.{AutoConfigurationCustomizer, AutoConfigurationCustomizerProvider}

import java.util.logging.Logger

/**
 * Registered via ServiceLoader SPI. Called by the OTEL agent during autoconfiguration.
 * Detects driver vs executor role and sets appropriate resource attributes.
 *
 * IMPORTANT: This class is loaded by the OTEL agent's extension classloader BEFORE
 * Spark starts. SLF4J is shaded inside the agent and NOT visible to extensions.
 * Use java.util.logging (always on bootstrap classpath) instead.
 */
class FlareAutoConfig extends AutoConfigurationCustomizerProvider {

  private val logger = Logger.getLogger(classOf[FlareAutoConfig].getName)

  override def customize(customizer: AutoConfigurationCustomizer): Unit = {
    val config = FlareConfig.load()

    if (!config.enabled) {
      logger.info("[Flare] Disabled via FLARE_ENABLED=false")
      return
    }

    val role = detectRole()
    logger.info(s"[Flare] Initializing on $role (granularity=${config.granularity}, " +
      s"sampling=${config.samplingRatio}, maxSpans=${config.maxSpansPerTrace})")

    customizer.addResourceCustomizer { (resource, _) =>
      import io.opentelemetry.sdk.resources.Resource
      Resource.builder()
        .put("flare.role", role)
        .put("flare.version", BuildInfo.version)
        .build()
        .merge(resource)
    }
  }

  private def detectRole(): String = {
    // At agent init time, Spark classes may not be on this classloader.
    // Only use env vars — SparkEnv is not available to extensions.
    sys.env.get("SPARK_EXECUTOR_ID") match {
      case Some(id) if id.nonEmpty => s"executor-$id"
      case _                       => "driver"
    }
  }
}
