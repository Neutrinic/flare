package io.flare.spark.attributes

import io.flare.spark.attributes.SparkAttributes.Error
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.api.trace.{Span, StatusCode}
import org.apache.spark.{ExceptionFailure, TaskFailedReason}

/**
 * Structured detail about a failure, ready to attach to a span.
 *
 * Spark reports failures three different ways — a live `Throwable` on a job, a
 * `TaskFailedReason` on a task, and a bare formatted `String` on a stage — so the extraction
 * differs per call site while the resulting span shape must not. Everything funnels through
 * [[FailureDetail.record]] so a failed job, stage and task all carry the same keys.
 *
 * `errorType` is optional on purpose. It is the key you group failures by, so a guessed value is
 * worse than an absent one: absent reads as "not captured", wrong reads as a real signal.
 */
final case class FailureDetail(
  errorType:  Option[String],
  message:    String,
  stackTrace: Option[String],
)

object FailureDetail {

  /** Matches the message cap already used for `spark.stage.failure_reason`. */
  private val MaxMessageChars = 500

  /**
   * Stack traces are unbounded and the whole point is to avoid a trip to the executor logs, so
   * this is far more generous than the message cap — but still bounded, because a span that
   * blows the exporter's payload limit is dropped, taking the failure signal with it.
   */
  private val MaxStackTraceChars = 8000

  // OTEL semantic conventions for the `exception` span event. Deliberately not reusing
  // Span.recordException: it needs a live Throwable, which only the job path ever has.
  private val ExceptionType       = AttributeKey.stringKey("exception.type")
  private val ExceptionMessage    = AttributeKey.stringKey("exception.message")
  private val ExceptionStackTrace = AttributeKey.stringKey("exception.stacktrace")

  /**
   * A fully-qualified Java class name ending in Exception, Error or Throwable.
   *
   * Only used where Spark hands us a formatted string and nothing else. Requiring at least one
   * package segment and one of those three suffixes keeps it from matching ordinary prose in a
   * failure message.
   */
  private val FqcnPattern =
    """\b([a-zA-Z_$][\w$]*(?:\.[a-zA-Z_$][\w$]*)+(?:Exception|Error|Throwable))\b""".r

  /**
   * Where the root cause starts in a Spark stage failure message.
   *
   * The naive read — first class name in the string — reports the *wrapper*, because Spark
   * composes these as `org.apache.spark.SparkException: Job aborted due to stage failure: …
   * most recent failure: … java.lang.ArithmeticException: / by zero`. That would put
   * `SparkException` on essentially every failed stage, which is confidently wrong and worse
   * than omitting the attribute.
   *
   * Both anchors are formatting conventions rather than API: `most recent failure:` is
   * DAGScheduler's, `Caused by:` is the JVM's. Tried in that order, most specific first.
   */
  private val RootCauseAnchors = Seq("most recent failure:", "Caused by:")

  /**
   * A Spark error class, e.g. `[DIVIDE_BY_ZERO]` or `[CANNOT_PARSE.INVALID_FORMAT]`.
   *
   * Spark 3.4+ formats many failures as an error class rather than an exception trace, and those
   * messages contain no class name at all:
   *
   * {{{
   * [DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 …
   * == SQL (line 1, position 1) ==
   * id div divisor
   * }}}
   *
   * Observed on a real Spark 4.0 stage failure, where the FQCN search correctly found nothing.
   * The error class is a deliberately stable, low-cardinality identifier — a better grouping key
   * than an exception class, not a worse one — so it is used when no class name is available.
   */
  private val ErrorClassPattern = """^\s*\[([A-Z][A-Z0-9_]*(?:\.[A-Z][A-Z0-9_]*)*)\]""".r

  /** From a live exception — the job path, and the only one with a real stack trace object. */
  def fromThrowable(t: Throwable): FailureDetail =
    FailureDetail(
      errorType  = Option(t.getClass.getName),
      message    = Option(t.getMessage).getOrElse(t.toString),
      stackTrace = Some(stackTraceToString(t)),
    )

  /**
   * From a Spark task failure — the executor path.
   *
   * `ExceptionFailure` is the case that matters: it carries the exception class, its description
   * and the full stack trace as separate public fields, so nothing has to be parsed back out of
   * a formatted string. Every other `TaskFailedReason` (fetch failure, executor lost, killed,
   * commit denied) describes a Spark-level condition rather than a user exception, so the reason
   * class itself is the error type and `toErrorString` is the message.
   */
  def fromTaskFailure(reason: TaskFailedReason): FailureDetail = reason match {
    case e: ExceptionFailure =>
      FailureDetail(
        errorType  = Option(e.className),
        message    = Option(e.description).getOrElse(e.toErrorString),
        stackTrace = Option(e.fullStackTrace),
      )
    case other =>
      FailureDetail(
        errorType  = Some(other.getClass.getName.stripSuffix("$")),
        message    = other.toErrorString,
        stackTrace = None,
      )
  }

  /**
   * From a formatted failure string — the stage path.
   *
   * Spark composes stage failure reasons like `"Job aborted due to stage failure: ... Lost task
   * 0.0 ...: java.lang.RuntimeException: boom\n\tat ..."`. There is no structure to read, so the
   * exception class is recovered by pattern, and left unset when nothing matches confidently.
   */
  def fromReasonString(reason: String): FailureDetail =
    FailureDetail(
      errorType  = rootCauseClass(reason),
      message    = reason,
      stackTrace = None,
    )

  /**
   * The root cause class in a formatted failure message, or None when nothing matches.
   *
   * Searches after the last occurrence of the most specific anchor present, so a wrapping
   * `SparkException` before the anchor cannot win. Falls back to scanning the whole string when
   * no anchor is there — in that case the first match is the best available guess, and the
   * suffix requirement in [[FqcnPattern]] is the only thing keeping it honest.
   */
  private def rootCauseClass(reason: String): Option[String] = {
    val searchFrom = RootCauseAnchors
      .map(anchor => reason.lastIndexOf(anchor))
      .find(_ >= 0)
      .getOrElse(0)

    FqcnPattern
      .findFirstMatchIn(reason.substring(searchFrom))
      .map(_.group(1))
      // No class name anywhere — fall back to Spark's own error class if the message leads with
      // one. Checked against the whole string, since the error class prefixes the message rather
      // than following any of the anchors.
      .orElse(ErrorClassPattern.findFirstMatchIn(reason).map(_.group(1)))
  }

  /** Applies the detail to a span: status, attributes, and an `exception` event if we have one. */
  def record(span: Span, detail: FailureDetail): Unit = {
    span.setStatus(StatusCode.ERROR, truncate(detail.message, MaxMessageChars))
    detail.errorType.foreach(span.setAttribute(Error.Type, _))
    span.setAttribute(Error.Message, truncate(detail.message, MaxMessageChars))

    detail.stackTrace.foreach { trace =>
      val builder = Attributes.builder()
      detail.errorType.foreach(builder.put(ExceptionType, _))
      builder.put(ExceptionMessage, truncate(detail.message, MaxMessageChars))
      builder.put(ExceptionStackTrace, truncate(trace, MaxStackTraceChars))
      span.addEvent("exception", builder.build())
    }
  }

  private def stackTraceToString(t: Throwable): String = {
    val writer = new java.io.StringWriter()
    t.printStackTrace(new java.io.PrintWriter(writer))
    writer.toString
  }

  private def truncate(s: String, max: Int): String =
    if (s.length <= max) s else s.take(max)
}
