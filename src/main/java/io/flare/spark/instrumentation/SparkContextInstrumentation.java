package io.flare.spark.instrumentation;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;

/**
 * TypeInstrumentation targeting {@code org.apache.spark.SparkContext}.
 *
 * <p>Hooks the constructor exit ({@code <init>}) so that after SparkContext is
 * fully constructed, the ByteBuddy advice fires and registers the Flare listener
 * + application span — no {@code spark.plugins} config needed.
 *
 * <p>Written in Java because the agent's extension classloader does NOT have the
 * Scala runtime. See {@link SparkContextInstrumentationModule} for details.
 */
public class SparkContextInstrumentation implements TypeInstrumentation {

    @Override
    public ElementMatcher<TypeDescription> typeMatcher() {
        return ElementMatchers.named("org.apache.spark.SparkContext");
    }

    @Override
    public void transform(TypeTransformer transformer) {
        transformer.applyAdviceToMethod(
            ElementMatchers.isConstructor(),
            SparkContextAdvice.class.getName()
        );
    }
}
