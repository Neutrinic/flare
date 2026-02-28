package io.flare.spark.instrumentation;

import io.opentelemetry.context.Scope;
import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy advice for {@code Executor$TaskRunner.run()}.
 *
 * <p>On method enter: extracts {@code traceparent} from the task's serialized properties
 * (via {@code taskDescription} field) and makes the parent OTEL context current.
 *
 * <p>On method exit: closes the scope, restoring the previous context.
 *
 * <p>This does NOT create spans — it only restores context. Span lifecycle is managed by
 * {@code FlareExecutorPlugin}, which fires inside {@code run()} after {@code TaskContext}
 * is set up.
 *
 * <p>Written in Java for reliable bytecode inlining. All real logic lives in
 * {@link TaskRunnerAdviceHelper}.
 */
public class TaskRunnerAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static Scope onEnter(
            @Advice.FieldValue("taskDescription") Object taskDescription) {
        return TaskRunnerAdviceHelper.onEnter(taskDescription);
    }

    @Advice.OnMethodExit(suppress = Throwable.class, onThrowable = Throwable.class)
    public static void onExit(@Advice.Enter Scope scope) {
        TaskRunnerAdviceHelper.onExit(scope);
    }
}
