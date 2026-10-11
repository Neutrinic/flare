package io.flare.spark.config;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import io.opentelemetry.sdk.resources.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OpenTelemetry Java agent auto-configuration for Flare.
 *
 * <p>This provider is deliberately implemented in Java. The agent loads extension services before
 * Spark starts, from a class loader that does not contain Scala's runtime.
 *
 * <p>It contributes JVM-level resource attributes, the samplers and span processors below, and
 * default agent settings. {@code FLARE_*} parsing and validation remain at the first Spark-side
 * Scala initialization point, where the Scala runtime is available.
 */
public class FlareAutoConfig implements AutoConfigurationCustomizerProvider {

  private static final String BUILD_INFO_RESOURCE =
      "/io/flare/spark/flare-build.properties";
  private static final String FLARE_VERSION_PROPERTY = "flare.version";
  private static final String UNKNOWN_VERSION = "unknown";

  private static final Logger logger = Logger.getLogger(FlareAutoConfig.class.getName());

  /**
   * The agent's own instrumentations are off by default, keeping only what Flare and the user's
   * own code need (#145).
   *
   * <p>In a Spark JVM they mostly trace Spark reading its own input: one span per S3 or GCS request,
   * nested under the task that made it, so the non-Spark root filter keeps them. A 20-minute TPC-H
   * run had 12,000 of them on Databricks and 82,000 on Dataproc, more than Flare's own spans, and on
   * Dataproc they cost about 2% CPU for as long as data was read. Turning them off also halved the
   * start-up cost on Databricks.
   *
   * <p>What stays on, each for a reason:
   *
   * <ul>
   *   <li>{@code opentelemetry-api}: bridges the API Flare calls to the agent's SDK. Without it
   *       nothing at all is exported.
   *   <li>{@code opentelemetry-instrumentation-annotations}: the user's own {@code @WithSpan}.
   *   <li>{@code flare-spark}: Flare's own instrumentation.
   *   <li>{@code executors}: carries a task's context into thread pools and futures, so spans made
   *       there keep their parent. It creates no spans of its own.
   *   <li>{@code log4j-appender}: log export. Whether logs are exported is still {@code
   *       otel.logs.exporter}.
   *   <li>{@code runtime-telemetry}: JVM metrics such as heap, GC and threads.
   * </ul>
   *
   * <p>These are defaults, key by key: the SDK ranks system properties, environment variables and
   * the agent's configuration file above a properties supplier, so a user's setting for any of these
   * keys wins. {@code -Dotel.instrumentation.jdbc.enabled=true} turns one instrumentation back on;
   * {@code -Dotel.instrumentation.common.default-enabled=true} restores all of them. The kept six
   * are enabled by their own keys, so turning one off takes that key, such as
   * {@code -Dotel.instrumentation.runtime-telemetry.enabled=false}.
   */
  static final Map<String, String> AGENT_DEFAULTS;

  static {
    Map<String, String> defaults = new LinkedHashMap<>();
    defaults.put("otel.instrumentation.common.default-enabled", "false");
    for (String kept :
        new String[] {
          "opentelemetry-api",
          "opentelemetry-instrumentation-annotations",
          "flare-spark",
          "executors",
          "log4j-appender",
          "runtime-telemetry",
        }) {
      defaults.put("otel.instrumentation." + kept + ".enabled", "true");
    }
    AGENT_DEFAULTS = Collections.unmodifiableMap(defaults);
  }

  @Override
  public void customize(AutoConfigurationCustomizer customizer) {
    // The agent defaults and the root filter stay when Flare is disabled (#206). They are what keeps
    // the agent quiet: without them it is back to its full instrumentation, a span per S3 or GCS
    // request and the platform's HTTP traces, so turning Flare off to cut overhead produced more
    // telemetry than leaving it on. Each still has its own opt-out.
    customizer.addPropertiesSupplier(() -> AGENT_DEFAULTS);

    // Driver and executors alike: each traces its own platform calls, the driver the cluster
    // manager's HTTP traffic (#123) and executors their start-up, such as fetching the application
    // jar from S3 (#127).
    if (dropsNonSparkRoots()) {
      customizer.addSamplerCustomizer((sampler, config) -> new SparkRootSampler(sampler));
    }

    if (!isFlareEnabled()) {
      logger.info("[Flare] Disabled via FLARE_ENABLED=false; the agent's defaults stay quiet");
      return;
    }

    customizer.addResourceCustomizer(
        (resource, config) -> Resource.create(flareResourceAttributes()).merge(resource));

    if ("driver".equals(detectRole())) {
      customizer.addSpanProcessorCustomizer(
          (processor, config) -> new EndDriverSpansOnShutdown(processor));
    }
  }

  /**
   * Reads {@code FLARE_DROP_NON_SPARK_ROOTS}: system property first, then environment. On unless the
   * value is the literal {@code "false"}, like {@code FLARE_ENABLED}. Opt out when a Spark JVM does
   * its own traced work that should start traces, such as a driver serving HTTP.
   */
  static boolean dropsNonSparkRoots() {
    String configured = System.getProperty("FLARE_DROP_NON_SPARK_ROOTS");
    if (configured == null) {
      configured = System.getenv("FLARE_DROP_NON_SPARK_ROOTS");
    }
    return configured == null || !"false".equalsIgnoreCase(configured);
  }

  /**
   * Reads the {@code FLARE_ENABLED} kill switch: system property first, then environment, and only
   * the literal value {@code "false"} disables Flare.
   *
   * <p>{@code FlareConfig.load()} parses the same key independently on the Scala side, because
   * this class cannot reach the Scala runtime from the agent extension classloader. The two must
   * agree; change them together.
   */
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
