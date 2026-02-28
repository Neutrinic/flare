package io.flare.spark.instrumentation;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;

/**
 * TypeInstrumentation targeting {@code DAGScheduler.submitMissingTasks(stage, jobId)}.
 *
 * <p>This is where Spark creates tasks for a specific stage. By hooking here,
 * we inject a per-stage traceparent into {@code ActiveJob.properties} before
 * tasks are created, making executor task spans children of their stage span.
 *
 * <p>Unlike the previous {@code runJob} hook, this captures ALL stages including
 * AQE (Adaptive Query Execution) sub-jobs that bypass {@code SparkContext.runJob}.
 *
 * <p>Written in Java because the agent's extension classloader does not have
 * the Scala runtime.
 */
public class SubmitMissingTasksInstrumentation implements TypeInstrumentation {

    @Override
    public ElementMatcher<TypeDescription> typeMatcher() {
        return ElementMatchers.named("org.apache.spark.scheduler.DAGScheduler");
    }

    @Override
    public void transform(TypeTransformer transformer) {
        transformer.applyAdviceToMethod(
            ElementMatchers.named("submitMissingTasks"),
            SubmitMissingTasksAdvice.class.getName()
        );
    }
}
