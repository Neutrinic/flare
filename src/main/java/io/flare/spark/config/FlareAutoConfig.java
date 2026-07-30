package io.flare.spark.config;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import io.opentelemetry.sdk.resources.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * OpenTelemetry Java agent auto-configuration for Flare.
 *
 * <p>This provider is deliberately implemented in Java. The agent loads extension services before
 * Spark starts, from a class loader that does not contain Scala's runtime.
 *
 * <p>It only contributes JVM-level resource attributes. {@code FLARE_*} parsing and validation
 * remain at the first Spark-side Scala initialization point, where the Scala runtime is available.
 */
public class FlareAutoConfig implements AutoConfigurationCustomizerProvider {

  private static final String BUILD_INFO_RESOURCE =
      "/io/flare/spark/flare-build.properties";
  private static final String FLARE_VERSION_PROPERTY = "flare.version";

  private static final Logger logger = Logger.getLogger(FlareAutoConfig.class.getName());

  @Override
  public void customize(AutoConfigurationCustomizer customizer) {
    if (!isFlareEnabled()) {
      logger.info("[Flare] Disabled via FLARE_ENABLED=false");
      return;
    }

    customizer.addResourceCustomizer(
        (resource, config) -> Resource.create(flareResourceAttributes()).merge(resource));
  }

  static boolean isFlareEnabled() {
    String configured = System.getProperty("FLARE_ENABLED");
    if (configured == null) {
      configured = System.getenv("FLARE_ENABLED");
    }
    return configured == null || !"false".equalsIgnoreCase(configured);
  }

  static Attributes flareResourceAttributes() {
    return Attributes.builder()
        .put("flare.role", detectRole())
        .put(FLARE_VERSION_PROPERTY, loadFlareVersion())
        .build();
  }

  static String loadFlareVersion() {
    Properties properties = new Properties();

    try (InputStream stream = FlareAutoConfig.class.getResourceAsStream(BUILD_INFO_RESOURCE)) {
      if (stream == null) {
        throw new IllegalStateException(
            "Missing Flare build metadata resource " + BUILD_INFO_RESOURCE);
      }
      properties.load(stream);
    } catch (IOException exception) {
      throw new IllegalStateException(
          "Could not read Flare build metadata resource " + BUILD_INFO_RESOURCE,
          exception);
    }

    String version = properties.getProperty(FLARE_VERSION_PROPERTY);
    if (version == null || version.trim().isEmpty()) {
      throw new IllegalStateException(
          "Missing " + FLARE_VERSION_PROPERTY + " in " + BUILD_INFO_RESOURCE);
    }
    return version.trim();
  }

  private static String detectRole() {
    String executorId = System.getenv("SPARK_EXECUTOR_ID");
    return executorId == null || executorId.isEmpty() ? "driver" : "executor-" + executorId;
  }
}
