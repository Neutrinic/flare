package io.flare.spark.instrumentation;

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
 *
 * <p>The scope crosses this boundary as {@code Object}, never as an OpenTelemetry type (#173). The
 * agent relocates OpenTelemetry types in this advice to its shaded package before inlining it into
 * Spark, while the helper it calls resolves from Flare's JAR on the application classpath, where
 * they are not relocated. A {@code Scope} in the signature therefore names a method the helper does
 * not have, and the {@code NoSuchMethodError} is swallowed by {@code suppress}, so no context is
 * restored. Keep OpenTelemetry types out of every advice signature.
 */
public class TaskRunnerAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static Object onEnter(
            @Advice.FieldValue("taskDescription") Object taskDescription) {
        return TaskRunnerAdviceHelper.onEnter(taskDescription);
    }

    @Advice.OnMethodExit(suppress = Throwable.class, onThrowable = Throwable.class)
    public static void onExit(@Advice.Enter Object scope) {
        TaskRunnerAdviceHelper.onExit(scope);
    }
}
