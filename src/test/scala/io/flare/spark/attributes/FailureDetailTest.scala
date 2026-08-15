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
  test("fromReasonString leaves errorType unset when nothing matches confidently") {
    assertEquals(FailureDetail.fromReasonString("OOM").errorType, None)
    assertEquals(FailureDetail.fromReasonString("executor lost").errorType, None)
    assertEquals(FailureDetail.fromReasonString("Stage cancelled because Error happened").errorType, None)
  }

  test("fromReasonString does not match a bare class name with no package") {
    assertEquals(FailureDetail.fromReasonString("RuntimeException: boom").errorType, None)
  }
}
