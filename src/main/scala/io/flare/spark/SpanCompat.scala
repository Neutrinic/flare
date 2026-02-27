package io.flare.spark

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.{Span, SpanBuilder}

/**
 * Scala-Java interop helpers for OTEL's setAttribute.
 *
 * Java's AttributeKey is invariant in T, so Scala's Long (scala.Long) does not
 * match java.lang.Long in the generic setAttribute[T](AttributeKey[T], T) context.
 * These helpers box explicitly, avoiding the invariance mismatch.
 */
private[spark] object SpanCompat {

  implicit class RichSpan(private val span: Span) extends AnyVal {
    def setLong(key: AttributeKey[java.lang.Long], value: Long): Span =
      span.setAttribute(key, java.lang.Long.valueOf(value))
    def setBool(key: AttributeKey[java.lang.Boolean], value: Boolean): Span =
      span.setAttribute(key, java.lang.Boolean.valueOf(value))
  }

  implicit class RichSpanBuilder(private val sb: SpanBuilder) extends AnyVal {
    def setLong(key: AttributeKey[java.lang.Long], value: Long): SpanBuilder =
      sb.setAttribute(key, java.lang.Long.valueOf(value))
    def setBool(key: AttributeKey[java.lang.Boolean], value: Boolean): SpanBuilder =
      sb.setAttribute(key, java.lang.Boolean.valueOf(value))
  }
}
