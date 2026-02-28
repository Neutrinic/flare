package io.flare.spark.instrumentation;

import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;

import java.util.Collections;
import java.util.List;

/**
 * OTEL Java agent InstrumentationModule targeting Spark's {@code TaskRunner.run()} on the
 * executor JVM.
 *
 * <p>This makes the OTEL parent context (extracted from the task's {@code traceparent} local
 * property) active for the entire duration of {@code TaskRunner.run()}, including Spark's
 * plugin callbacks and the task lambda. Any downstream OTEL-instrumented library (JDBC, HTTP,
 * gRPC) running inside the task will inherit the correct parent context.
 *
 * <p>This does NOT create spans — span lifecycle is managed by {@code FlareExecutorPlugin}.
 * The advice only restores context, keeping the change minimal and avoiding duplicate spans.
 *
 * <p>Written in Java because the agent's extension classloader does not have the Scala runtime.
 *
 * <p>Registered via META-INF/services/io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule
 */
public class TaskRunnerInstrumentationModule
    extends InstrumentationModule {

    public TaskRunnerInstrumentationModule() {
        super("flare-spark", "flare-spark-taskrunner");
    }

    @Override
    public List<TypeInstrumentation> typeInstrumentations() {
        return Collections.singletonList(new TaskRunnerInstrumentation());
    }

    /**
     * Inject all Flare extension classes into the target application classloader.
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
