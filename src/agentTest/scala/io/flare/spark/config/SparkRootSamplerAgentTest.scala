package io.flare.spark.config

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import munit.FunSuite

/**
 * Proves the real agent picks up Flare's sampler on the driver (#123). This test JVM has no
 * executor id, so the agent sees a driver. The unit test covers the sampler's decisions; this
 * covers that `FlareAutoConfig` actually installs it. The build runs it a second time with
 * FLARE_ENABLED=false, which must keep the sampler (#206).
 */
class SparkRootSamplerAgentTest extends FunSuite {

  private val tracer = GlobalOpenTelemetry.getTracer("flare-root-sampler-test")

  test("under the agent, a driver root span that is not Spark's is not sampled") {
    val span = tracer.spanBuilder("GET /platform/noise").setSpanKind(SpanKind.SERVER).startSpan()
    try assert(!span.getSpanContext.isSampled, "a non-Spark root span on the driver was sampled")
    finally span.end()
  }

  test("under the agent, Spark's root spans and their children are sampled") {
    val root = tracer.spanBuilder("spark.application").startSpan()
    val scope = root.makeCurrent()
    try {
      assert(root.getSpanContext.isSampled, "spark.application was not sampled")
      val child = tracer.spanBuilder("SELECT orders").setSpanKind(SpanKind.CLIENT).startSpan()
      try assert(child.getSpanContext.isSampled, "a child of a Spark span was not sampled")
      finally child.end()
    } finally {
      scope.close()
      root.end()
    }
  }
}
