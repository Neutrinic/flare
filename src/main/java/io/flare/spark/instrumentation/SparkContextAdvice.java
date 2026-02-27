package io.flare.spark.instrumentation;

import net.bytebuddy.asm.Advice;
import org.apache.spark.SparkContext;

/**
 * ByteBuddy advice class for SparkContext constructor exit.
 *
 * <p>Written in Java because ByteBuddy copies advice bytecode literally into the
 * target class. Scala's companion object and trait linearization produce bytecode
 * patterns that can confuse the advice inlining mechanism. Java gives predictable,
 * simple bytecode.
 *
 * <p>All real logic lives in {@link SparkContextAdviceHelper} (Scala) — this class
 * is a thin delegation shim.
 */
public class SparkContextAdvice {

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static void onExit(@Advice.This SparkContext sparkContext) {
        SparkContextAdviceHelper.onSparkContextInit(sparkContext);
    }
}
