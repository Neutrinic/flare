package io.flare.spark.instrumentation;

import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy advice for {@code DAGScheduler.submitMissingTasks(stage, jobId)}.
 *
 * <p>On method enter: creates a per-stage span, injects its W3C traceparent
 * into {@code ActiveJob.properties} so that tasks created during this method
 * inherit the stage-level trace context on the executor.
 *
 * <p>No exit advice is needed — the properties mutation is consumed by the
 * method body when it creates the TaskSet.
 *
 * <p>Written in Java for reliable bytecode inlining. All real logic lives in
 * {@link SubmitMissingTasksAdviceHelper}.
 */
public class SubmitMissingTasksAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(
            @Advice.This Object dagScheduler,
            @Advice.Argument(0) Object stage,
            @Advice.Argument(1) int jobId) {
        SubmitMissingTasksAdviceHelper.onEnter(dagScheduler, stage, jobId);
    }
}
