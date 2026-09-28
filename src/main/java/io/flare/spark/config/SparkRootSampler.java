package io.flare.spark.config;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.List;

/**
 * Drops traces a Spark JVM starts that are not Spark's (#123, #127).
 *
 * <p>The agent instruments everything, and every platform makes its own calls. On the driver:
 * Databricks' HTTP requests into it (Jetty), Dataproc's metadata and GCS calls
 * (HttpURLConnection), Spark's calls to the Kubernetes API server (OkHttp, Vert.x). A three-minute
 * Databricks run produced about 320 of them next to one Flare trace. On executors: start-up work
 * such as fetching the application jar from S3 on EMR Serverless, three traces per executor. Each
 * becomes a one-span trace.
 *
 * <p>A span with no parent is dropped unless it is one of Flare's {@code spark.*} spans. Anything
 * with a parent goes to the configured sampler as before, so calls made inside Spark work still
 * nest under Flare's spans, and a request arriving with a {@code traceparent} still continues its
 * caller's trace.
 *
 * <p>Work a task hands to an {@code ExecutorService} or {@code CompletableFuture} keeps the task as
 * parent, because the agent carries context there. A raw {@code new Thread} does not get it, so a
 * span created there is dropped; without this sampler it was a detached one-span trace, never a
 * child of the task. {@code AsyncContextAgentTest} pins both.
 */
final class SparkRootSampler implements Sampler {

  static final String SPARK_SPAN_PREFIX = "spark.";

  private final Sampler delegate;

  SparkRootSampler(Sampler delegate) {
    this.delegate = delegate;
  }

  @Override
  public SamplingResult shouldSample(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {
    boolean hasParent = Span.fromContext(parentContext).getSpanContext().isValid();
    if (!hasParent && !name.startsWith(SPARK_SPAN_PREFIX)) {
      return SamplingResult.drop();
    }
    return delegate.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
  }

  @Override
  public String getDescription() {
    return "FlareSparkRootSampler{" + delegate.getDescription() + "}";
  }
}
