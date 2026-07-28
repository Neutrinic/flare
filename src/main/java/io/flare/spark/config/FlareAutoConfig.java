package io.flare.spark.config;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import io.opentelemetry.sdk.resources.Resource;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;

/**
 * OpenTelemetry Java agent auto-configuration for Flare.
 *
 * <p>This provider is deliberately implemented in Java. The agent loads extension services before
 * Spark starts, from a class loader that does not contain Scala's runtime.
 *
 * <p>Kafka's process instrumentation normally adopts the propagated message context as its parent.
 * That disconnects it from the Spark stage or task which is actually processing the record. Receive
 * telemetry keeps the Spark context as the parent and records the propagated message context as a
 * span link instead, which is the OpenTelemetry messaging model for ambient and batch consumers.
 *
 * <p>The value is supplied as a low-precedence default. An explicit system property or environment
 * variable, including an explicit {@code false}, always wins.
 */
public class FlareAutoConfig implements AutoConfigurationCustomizerProvider {

  static final String RECEIVE_TELEMETRY_PROPERTY =
      "otel.instrumentation.messaging.experimental.receive-telemetry.enabled";

  private static final Logger logger = Logger.getLogger(FlareAutoConfig.class.getName());

  @Override
  public void customize(AutoConfigurationCustomizer customizer) {
    if (!isFlareEnabled()) {
      logger.info("[Flare] Disabled via FLARE_ENABLED=false");
      return;
    }

    customizer
        .addPropertiesSupplier(FlareAutoConfig::defaultProperties)
        .addResourceCustomizer(
            (resource, config) ->
                Resource.create(flareResourceAttributes()).merge(resource));
  }

  /**
   * Run before ordinary providers so their property suppliers can override Flare's defaults.
   * System properties and environment variables have higher precedence regardless of this order.
   */
  @Override
  public int order() {
    return Integer.MIN_VALUE;
  }

  static Map<String, String> defaultProperties() {
    return Collections.singletonMap(RECEIVE_TELEMETRY_PROPERTY, "true");
  }

  static boolean isFlareEnabled() {
    String configured = System.getProperty("FLARE_ENABLED");
    if (configured == null) {
      configured = System.getenv("FLARE_ENABLED");
    }
    return configured == null || !"false".equalsIgnoreCase(configured);
  }

  private static Attributes flareResourceAttributes() {
    AttributesBuilder attributes = Attributes.builder().put("flare.role", detectRole());

    Package flarePackage = FlareAutoConfig.class.getPackage();
    String version = flarePackage == null ? null : flarePackage.getImplementationVersion();
    if (version != null && !version.isEmpty()) {
      attributes.put("flare.version", version);
    }
    return attributes.build();
  }

  private static String detectRole() {
    String executorId = System.getenv("SPARK_EXECUTOR_ID");
    return executorId == null || executorId.isEmpty() ? "driver" : "executor-" + executorId;
  }
}
