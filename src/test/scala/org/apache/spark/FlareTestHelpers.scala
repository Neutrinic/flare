package org.apache.spark

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{JobFailed, JobResult, StageInfo}

/**
 * Test helpers in the org.apache.spark package to access private[spark] members.
 * This is a standard pattern for testing Spark internals.
 */
object FlareTestHelpers {

  def emptyTaskMetrics(): TaskMetrics = new TaskMetrics()

  /**
   * A real, empty TaskContext bound to the calling thread.
   *
   * `TaskContext.empty()` is private[spark] but its signature is identical across 3.3-4.0,
   * unlike the TaskContextImpl constructor, so this stays compilable on the whole matrix.
   */
  def bindEmptyTaskContext(): TaskContext = {
    val tc = TaskContext.empty()
    TaskContext.setTaskContext(tc)
    tc
  }

  def unbindTaskContext(): Unit = TaskContext.unset()

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
