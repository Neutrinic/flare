package io.flare.spark.config;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import io.opentelemetry.sdk.resources.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
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
  private static final String UNKNOWN_VERSION = "unknown";

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

  /**
   * Reads the build-stamped Flare version, falling back to {@code "unknown"}.
   *
   * <p>This runs inside the agent's premain. Throwing here would abort auto-configuration for the
   * whole JVM, so a repackaged or shaded extension jar that lost the metadata resource would take
   * down the instrumented application rather than just mislabel it. A missing version is a
   * cosmetic problem; it must never be a fatal one.
   */
  static String loadFlareVersion() {
    Properties properties = new Properties();

    try (InputStream stream = FlareAutoConfig.class.getResourceAsStream(BUILD_INFO_RESOURCE)) {
      if (stream == null) {
        logger.warning(
            "[Flare] Missing build metadata resource "
                + BUILD_INFO_RESOURCE
                + ", reporting flare.version="
                + UNKNOWN_VERSION);
        return UNKNOWN_VERSION;
      }
      properties.load(stream);
    } catch (IOException exception) {
      logger.log(
          Level.WARNING,
          "[Flare] Could not read build metadata resource "
              + BUILD_INFO_RESOURCE
              + ", reporting flare.version="
              + UNKNOWN_VERSION,
          exception);
      return UNKNOWN_VERSION;
    }

    String version = properties.getProperty(FLARE_VERSION_PROPERTY);
    if (version == null || version.trim().isEmpty()) {
      logger.warning(
          "[Flare] Missing "
              + FLARE_VERSION_PROPERTY
              + " in "
              + BUILD_INFO_RESOURCE
              + ", reporting flare.version="
              + UNKNOWN_VERSION);
      return UNKNOWN_VERSION;
    }
    return version.trim();
  }

  static String detectRole() {
    String executorId = System.getenv("SPARK_EXECUTOR_ID");
    if (executorId == null || executorId.isEmpty()) {
      executorId = executorIdFromCommand(System.getProperty("sun.java.command"));
    }
    return executorId == null || executorId.isEmpty() ? "driver" : "executor-" + executorId;
  }

  /**
   * Extracts the executor id from the JVM command line.
   *
   * <p>Only Kubernetes sets {@code SPARK_EXECUTOR_ID} in the executor environment. Standalone and
   * YARN pass identity as a {@code --executor-id} argument to the executor backend, so without this
   * fallback every executor would report itself as a driver.
   */
  static String executorIdFromCommand(String command) {
    if (command == null || !command.contains("CoarseGrainedExecutorBackend")) {
      return null;
    }

    String[] tokens = command.trim().split("\\s+");
    for (int i = 0; i < tokens.length - 1; i++) {
      if ("--executor-id".equals(tokens[i])) {
        return tokens[i + 1];
      }
    }
    return null;
  }
}
