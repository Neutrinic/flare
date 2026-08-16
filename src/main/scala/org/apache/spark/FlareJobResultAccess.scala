package org.apache.spark

import org.apache.spark.scheduler.{JobFailed, JobResult}

/**
 * Reads the exception out of a failed `JobResult`.
 *
 * `JobSucceeded` is `@DeveloperApi` and freely referenceable, but `JobFailed` is `private[spark]`,
 * so the listener cannot pattern match on it from `io.flare.spark.listener`. This object exists
 * in the Spark package for that one access and nothing else — without it the job span could only
 * re-parse `JobResult.toString`, which is exactly what #46 set out to stop doing.
 *
 * `JobFailed(exception: Exception)` has held that shape since Spark 1.x; the CI matrix compiles
 * this against 3.3 through 4.0.
 */
object FlareJobResultAccess {

  def failureException(result: JobResult): Option[Throwable] = result match {
    case JobFailed(exception) => Option(exception)
    case _                    => None
  }
}
