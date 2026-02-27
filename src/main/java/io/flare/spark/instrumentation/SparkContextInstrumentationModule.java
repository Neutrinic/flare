package io.flare.spark.instrumentation;

import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;

import java.util.Collections;
import java.util.List;

/**
 * OTEL Java agent InstrumentationModule that auto-registers Flare's driver-side
 * listener and application span when the OTEL agent loads this extension JAR.
 *
 * <p>This is Phase 2: true zero-config. The user no longer needs to set
 * {@code spark.plugins=io.flare.spark.plugin.FlareSparkPlugin} — the ByteBuddy
 * advice hooks SparkContext construction and wires everything automatically.
 *
 * <p>When both SparkPlugin (Phase 1) and ByteBuddy (Phase 2) are active,
 * FlareDriverState ensures only the first path to execute performs initialization.
 *
 * <p>IMPORTANT: Written in Java because the agent's extension classloader does NOT
 * have the Scala runtime library. Only Java classes can be loaded by the agent
 * during instrumentation setup. Scala helper classes are injected into the
 * application classloader via {@link #isHelperClass(String)}.
 *
 * <p>Registered via META-INF/services/io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule
 */
public class SparkContextInstrumentationModule
    extends InstrumentationModule {

    public SparkContextInstrumentationModule() {
        super("flare-spark", "flare-spark-context");
    }

    @Override
    public List<TypeInstrumentation> typeInstrumentations() {
        return Collections.singletonList(new SparkContextInstrumentation());
    }

    /**
     * Tell the agent which classes from this extension must be injected into the
     * target application classloader so that advice bytecode can reference them.
     *
     * <p>ByteBuddy copies advice method bytecode literally into the target class,
     * so all types referenced by the advice must be visible from the target's
     * classloader. The agent calls this method to decide which extension classes
     * to copy over.
     *
     * <p>Only production packages are listed — avoids injecting test helpers or
     * examples that happen to share the {@code io.flare.spark} prefix.
     */
    @Override
    public boolean isHelperClass(String className) {
        return className.startsWith("io.flare.spark.plugin.")
            || className.startsWith("io.flare.spark.listener.")
            || className.startsWith("io.flare.spark.instrumentation.")
            || className.startsWith("io.flare.spark.config.")
            || className.startsWith("io.flare.spark.propagation.")
            || className.startsWith("io.flare.spark.attributes.")
            || className.startsWith("io.flare.spark.SpanCompat")
            || className.startsWith("io.flare.spark.BuildInfo");
    }
}
