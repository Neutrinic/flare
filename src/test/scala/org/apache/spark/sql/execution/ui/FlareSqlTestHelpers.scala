package org.apache.spark.sql.execution.ui

/** Builds SQL end events for tests: `executionFailure` is `private[sql]`. */
object FlareSqlTestHelpers {

  /** The two-argument constructor compiles on every Spark line; 3.4 added a defaulted third. */
  def sqlEnd(executionId: Long, failure: Option[Throwable] = None): SparkListenerSQLExecutionEnd = {
    val event = SparkListenerSQLExecutionEnd(executionId, System.currentTimeMillis())
    event.executionFailure = failure
    event
  }
}
