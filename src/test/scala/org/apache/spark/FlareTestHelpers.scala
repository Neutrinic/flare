package org.apache.spark

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{JobFailed, JobResult, StageInfo}

/**
 * Test helpers in the org.apache.spark package to access private[spark] members.
 * This is a standard pattern for testing Spark internals.
 */
object FlareTestHelpers {

  def emptyTaskMetrics(): TaskMetrics = new TaskMetrics()

  def jobFailed(exception: Exception): JobResult = JobFailed(exception)

  def makeStageInfo(
    stageId:     Int,
    name:        String,
    numTasks:    Int    = 4,
    taskMetrics: TaskMetrics = null,
  ): StageInfo =
    new StageInfo(
      stageId, 0, name, numTasks, Nil, Nil, "",
      taskMetrics, Nil, None, 0, false, 0,
    )
}
