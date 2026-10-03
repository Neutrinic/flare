package org.apache.spark.sql.execution.ui

/**
 * Reads the exception off a SQL execution's end event (#175).
 *
 * `SparkListenerSQLExecutionEnd.executionFailure` is `private[sql]`, so the listener cannot read it
 * from `io.flare.spark.listener`. This object exists in Spark's package for that one access, as
 * `FlareJobResultAccess` does for failed jobs.
 *
 * Spark sets it on the live event when the execution throws, and it holds the real exception on
 * every line Flare supports, 3.3 through 4.x. Newer lines also carry `errorMessage`, but only as a
 * string and not on 3.3, so the exception is the one source that works everywhere. It is not
 * written to event logs, which is irrelevant here: Flare only sees live events.
 */
object FlareSqlEndAccess {

  def failure(event: SparkListenerSQLExecutionEnd): Option[Throwable] =
    Option(event.executionFailure).flatten
}
