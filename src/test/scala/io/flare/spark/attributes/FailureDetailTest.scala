package io.flare.spark.attributes

import munit.FunSuite
import org.apache.spark.{FlareTestHelpers, TaskResultLost}

class FailureDetailTest extends FunSuite {

  test("fromThrowable captures class, message and a real stack trace") {
    val detail = FailureDetail.fromThrowable(new IllegalStateException("bad state"))

    assertEquals(detail.errorType, Some("java.lang.IllegalStateException"))
    assertEquals(detail.message, "bad state")
    assert(detail.stackTrace.isDefined)
    assert(detail.stackTrace.get.contains("java.lang.IllegalStateException"))
    assert(detail.stackTrace.get.contains("FailureDetailTest"))
  }

  test("fromThrowable falls back to toString when the message is null") {
    val detail = FailureDetail.fromThrowable(new RuntimeException())

    assertEquals(detail.errorType, Some("java.lang.RuntimeException"))
    assertEquals(detail.message, "java.lang.RuntimeException")
  }

  test("fromTaskFailure reads ExceptionFailure's structured fields, not its toString") {
    val cause   = new NumberFormatException("For input string: \"abc\"")
    val failure = FlareTestHelpers.exceptionFailure(cause)

    val detail = FailureDetail.fromTaskFailure(failure)

    assertEquals(detail.errorType, Some("java.lang.NumberFormatException"))
    assertEquals(detail.message, "For input string: \"abc\"")
    assert(detail.stackTrace.exists(_.contains("java.lang.NumberFormatException")))
  }

  test("fromTaskFailure uses the reason class for non-exception Spark failures") {
    val detail = FailureDetail.fromTaskFailure(TaskResultLost)

    assertEquals(detail.errorType, Some("org.apache.spark.TaskResultLost"))
    assertEquals(detail.message, TaskResultLost.toErrorString)
    assertEquals(detail.stackTrace, None)
  }

  test("fromReasonString recovers the exception class from a Spark stage failure message") {
    val reason =
      "Job aborted due to stage failure: Task 0 in stage 1.0 failed 1 times, most recent " +
        "failure: Lost task 0.0 in stage 1.0: java.lang.ArithmeticException: / by zero\n\tat Foo.bar(Foo.scala:1)"

    val detail = FailureDetail.fromReasonString(reason)

    assertEquals(detail.errorType, Some("java.lang.ArithmeticException"))
    assertEquals(detail.message, reason)
    assertEquals(detail.stackTrace, None)
  }

  // The point of leaving errorType unset: error.type is the grouping key, so a guess is worse
  // than an absence. "OOM" and similar free text must not become an error type.
  // The shape Spark actually produces: the root cause is wrapped, so the FIRST class name in the
  // string is the wrapper. Naively taking the first match reports SparkException for everything.
  test("fromReasonString picks the root cause, not the wrapping SparkException") {
    val reason =
      "org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 1.0 " +
        "failed 4 times, most recent failure: Lost task 0.3 in stage 1.0 (TID 7) (executor 1): " +
        "java.lang.ArithmeticException: / by zero\n" +
        "\tat io.flare.examples.FailingJob$.$anonfun$main$1(FailingJob.scala:47)\n" +
        "Driver stacktrace:\n" +
        "\tat org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2785)"

    assertEquals(
      FailureDetail.fromReasonString(reason).errorType,
      Some("java.lang.ArithmeticException"),
    )
  }

  test("fromReasonString falls back to 'Caused by:' when there is no 'most recent failure:'") {
    val reason =
      "org.apache.spark.SparkException: Task serialization failed\n" +
        "Caused by: java.io.NotSerializableException: io.flare.examples.Widget"

    assertEquals(
      FailureDetail.fromReasonString(reason).errorType,
      Some("java.io.NotSerializableException"),
    )
  }

  // Nested causes: the LAST "Caused by:" is the innermost, which is the one worth grouping on.
  test("fromReasonString takes the innermost cause when causes are nested") {
    val reason =
      "org.apache.spark.SparkException: Job aborted\n" +
        "Caused by: java.lang.RuntimeException: wrapper\n" +
        "Caused by: java.lang.IllegalStateException: root"

    assertEquals(
      FailureDetail.fromReasonString(reason).errorType,
      Some("java.lang.IllegalStateException"),
    )
  }

  // Verbatim from a Spark 4.0 stage failureReason in the dev stack (FailingJob). Spark 3.4+
  // formats many failures as an error class with NO class name anywhere in the text, so without
  // this fallback error.type is empty on exactly the failures the default granularity shows.
  test("fromReasonString uses the Spark error class when the message names no exception") {
    val reason =
      "[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and " +
        "return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass " +
        "this error. SQLSTATE: 22012\n== SQL (line 1, position 1) ==\nid div divisor\n^^^^^^^^^^^^^^\n"

    assertEquals(FailureDetail.fromReasonString(reason).errorType, Some("DIVIDE_BY_ZERO"))
  }

  test("fromReasonString handles a dotted Spark error sub-class") {
    assertEquals(
      FailureDetail.fromReasonString("[CANNOT_PARSE.INVALID_FORMAT] bad input").errorType,
      Some("CANNOT_PARSE.INVALID_FORMAT"),
    )
  }

  // A real class name is more precise than the error class, so it still wins when both appear.
  test("fromReasonString prefers an exception class over a leading error class") {
    val reason =
      "[DIVIDE_BY_ZERO] Division by zero, most recent failure: Lost task 0.0: " +
        "org.apache.spark.SparkArithmeticException: / by zero"

    assertEquals(
      FailureDetail.fromReasonString(reason).errorType,
      Some("org.apache.spark.SparkArithmeticException"),
    )
  }

  test("fromReasonString leaves errorType unset when nothing matches confidently") {
    assertEquals(FailureDetail.fromReasonString("OOM").errorType, None)
    assertEquals(FailureDetail.fromReasonString("executor lost").errorType, None)
    assertEquals(FailureDetail.fromReasonString("Stage cancelled because Error happened").errorType, None)
  }

  test("fromReasonString does not match a bare class name with no package") {
    assertEquals(FailureDetail.fromReasonString("RuntimeException: boom").errorType, None)
  }
}
