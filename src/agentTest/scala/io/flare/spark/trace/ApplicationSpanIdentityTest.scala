package io.flare.spark.trace

import io.opentelemetry.api.GlobalOpenTelemetry
import munit.FunSuite

/**
 * Pins what a `Span` actually is when Flare's Spark-side code holds one under the real agent.
 *
 * This is a design constraint, not a feature. `FlareExecutorPlugin` and `TracingSparkListener`
 * load in Spark's classloader and resolve their tracer from `GlobalOpenTelemetry`. With the
 * javaagent attached, that does not return an SDK span — it returns the agent's bridge proxy,
 * `ApplicationSpan`, which implements `io.opentelemetry.api.trace.Span` and nothing else. The
 * real `SdkSpan` lives behind it in the agent's shaded SDK and is never handed out.
 *
 * The consequence is that **no Flare code running on the Spark side can read state back off a
 * span**. `ReadableSpan`, `toSpanData()`, attribute inspection and every similar SDK-side
 * affordance are unavailable, so any design that stores something on a span and reads it later
 * is unimplementable — see #100, where exactly that was proposed as the fix for orphaned
 * descendants and had to be abandoned.
 *
 * Without the agent (plain unit tests) `GlobalOpenTelemetry` hands out a genuine `SdkSpan` and
 * the cast succeeds, so this trap cannot be caught anywhere but here. That is the same failure
 * shape as #41, where a missing SPI descriptor left `FlareAutoConfig` dead in production while
 * every unit test passed.
 *
 * If this test starts failing, the agent's API bridging has changed and the constraint above is
 * worth re-deriving before relying on either outcome.
 */
class ApplicationSpanIdentityTest extends FunSuite {

  private val ReadableSpanClass = "io.opentelemetry.sdk.trace.ReadableSpan"

  test("a Span resolved from GlobalOpenTelemetry under the agent is a bridge proxy, not an SdkSpan") {
    val span = GlobalOpenTelemetry.getTracer("flare-span-identity").spanBuilder("probe").startSpan()

    try {
      // The unshaded SDK is on this test's classpath, so a failed isInstance below is a real
      // statement about the span and not an artefact of the class being absent.
      val readableSpan =
        try Class.forName(ReadableSpanClass)
        catch {
          case _: ClassNotFoundException =>
            fail(s"$ReadableSpanClass is not on the classpath; this test cannot conclude anything")
        }

      assert(
        !readableSpan.isInstance(span),
        s"expected an agent bridge proxy but got something castable to $ReadableSpanClass " +
          s"(${span.getClass.getName}) — the #100 constraint may no longer hold",
      )

      assertEquals(
        span.getClass.getInterfaces.map(_.getName).toSeq,
        Seq("io.opentelemetry.api.trace.Span"),
        "the bridge proxy exposes only the API surface; anything more would widen what Flare can do",
      )
    } finally span.end()
  }
}
