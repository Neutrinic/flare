package io.flare.spark.config

import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.{Span, SpanContext, SpanKind, TraceFlags, TraceState}
import io.opentelemetry.context.Context
import io.opentelemetry.sdk.trace.samplers.{Sampler, SamplingDecision}
import munit.FunSuite

import java.util.Collections

class SparkRootSamplerTest extends FunSuite {

  private val sampler = new SparkRootSampler(Sampler.alwaysOn())
  private val traceId = "0af7651916cd43dd8448eb211c80319c"

  private def decide(name: String, parent: Context, kind: SpanKind = SpanKind.SERVER): SamplingDecision =
    sampler
      .shouldSample(parent, traceId, name, kind, Attributes.empty(), Collections.emptyList())
      .getDecision

  private def parent(remote: Boolean): Context = {
    val ctx =
      if (remote) SpanContext.createFromRemoteParent(traceId, "b7ad6b7169203331", TraceFlags.getSampled, TraceState.getDefault)
      else SpanContext.create(traceId, "b7ad6b7169203331", TraceFlags.getSampled, TraceState.getDefault)
    Context.root().`with`(Span.wrap(ctx))
  }

  test("a root span that is not Spark's is dropped") {
    assertEquals(decide("POST", Context.root()), SamplingDecision.DROP)
    assertEquals(decide("GET /metrics", Context.root()), SamplingDecision.DROP)
    assertEquals(decide("GET", Context.root(), SpanKind.CLIENT), SamplingDecision.DROP)
  }

  test("Flare's root spans go to the configured sampler") {
    assertEquals(decide("spark.application", Context.root()), SamplingDecision.RECORD_AND_SAMPLE)
    assertEquals(decide("spark.sql.3", Context.root()), SamplingDecision.RECORD_AND_SAMPLE)
  }

  test("a span inside Spark work is kept, whatever it is") {
    assertEquals(decide("SELECT orders", parent(remote = false), SpanKind.CLIENT), SamplingDecision.RECORD_AND_SAMPLE)
  }

  test("a request that continues a caller's trace is kept") {
    assertEquals(decide("POST /api", parent(remote = true)), SamplingDecision.RECORD_AND_SAMPLE)
  }

  test("the configured sampler still decides what is kept") {
    val never = new SparkRootSampler(Sampler.alwaysOff())
    assertEquals(
      never.shouldSample(Context.root(), traceId, "spark.application", SpanKind.SERVER,
        Attributes.empty(), Collections.emptyList()).getDecision,
      SamplingDecision.DROP,
    )
    assert(never.getDescription.contains("AlwaysOffSampler"))
  }

  test("FLARE_DROP_NON_SPARK_ROOTS is on unless explicitly false") {
    def withValue[A](value: Option[String])(f: => A): A = {
      val key = "FLARE_DROP_NON_SPARK_ROOTS"
      val previous = sys.props.get(key)
      value.fold(sys.props.remove(key))(v => sys.props.put(key, v))
      try f
      finally previous.fold(sys.props.remove(key))(v => sys.props.put(key, v))
    }
    if (sys.env.get("FLARE_DROP_NON_SPARK_ROOTS").isEmpty) {
      assert(withValue(None)(FlareAutoConfig.dropsNonSparkRoots()))
    }
    assert(withValue(Some("true"))(FlareAutoConfig.dropsNonSparkRoots()))
    assert(!withValue(Some("false"))(FlareAutoConfig.dropsNonSparkRoots()))
    assert(!withValue(Some("FALSE"))(FlareAutoConfig.dropsNonSparkRoots()))
  }
}
