package io.flare.spark.instrumentation;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;

/**
 * TypeInstrumentation targeting {@code org.apache.spark.executor.Executor$TaskRunner}.
 *
 * <p>Hooks the {@code run()} method so that the OTEL parent context (from the task's
 * {@code traceparent} local property) is active for the entire task execution, including
 * plugin callbacks and the task lambda.
 *
 * <p>{@code TaskRunner} is {@code private[spark]} and an inner class of {@code Executor}.
 * JVM bytecode name is {@code Executor$TaskRunner}. This is a private API — muzzle checks
 * should validate it against each supported Spark version.
 *
 * <p>Written in Java because the agent's extension classloader does not have the Scala runtime.
 */
public class TaskRunnerInstrumentation implements TypeInstrumentation {

    @Override
    public ElementMatcher<TypeDescription> typeMatcher() {
        return ElementMatchers.named("org.apache.spark.executor.Executor$TaskRunner");
    }

    @Override
    public void transform(TypeTransformer transformer) {
        transformer.applyAdviceToMethod(
            ElementMatchers.named("run").and(ElementMatchers.takesNoArguments()),
            TaskRunnerAdvice.class.getName()
        );
    }
}
