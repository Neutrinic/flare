package org.apache.spark

import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.scheduler.{JobFailed, JobResult, StageInfo, TaskInfo, TaskLocality}

/**
 * Test helpers in the org.apache.spark package to access private[spark] members.
 * This is a standard pattern for testing Spark internals.
 */
object FlareTestHelpers {

  def emptyTaskMetrics(): TaskMetrics = new TaskMetrics()

  def emptyExecutorMetrics(): ExecutorMetrics = new ExecutorMetrics()

  /**
   * TaskMetrics with the timing breakdown populated.
   *
   * All of these setters are private[spark] but signature-identical on 3.3.4, 3.4.3, 3.5.1 and
   * 4.0.0 (verified with javap), so this compiles across the whole matrix.
   */
  def taskMetrics(
    executorRunTime:            Long = 0L,
    jvmGcTime:                  Long = 0L,
    executorDeserializeTime:    Long = 0L,
    executorDeserializeCpuTime: Long = 0L,
    resultSerializationTime:    Long = 0L,
    memoryBytesSpilled:         Long = 0L,
    diskBytesSpilled:           Long = 0L,
  ): TaskMetrics = {
    val m = new TaskMetrics()
    m.setExecutorRunTime(executorRunTime)
    m.setJvmGCTime(jvmGcTime)
    m.setExecutorDeserializeTime(executorDeserializeTime)
    m.setExecutorDeserializeCpuTime(executorDeserializeCpuTime)
    m.setResultSerializationTime(resultSerializationTime)
    m.incMemoryBytesSpilled(memoryBytesSpilled)
    m.incDiskBytesSpilled(diskBytesSpilled)
    m
  }

  /**
   * A finished TaskInfo whose wall clock is exactly `durationMs`.
   *
   * The 9-argument constructor (with partitionId) and markFinished are identical across
   * 3.3.4-4.0.0, so this needs no per-version handling.
   */
  def finishedTaskInfo(
    taskId:              Long = 0L,
    durationMs:          Long = 0L,
    gettingResultTimeMs: Long = 0L,
  ): TaskInfo = {
    val launchTime = 1000000L
    val info = new TaskInfo(
      taskId, taskId.toInt, 0, taskId.toInt, launchTime,
      "exec-1", "host-1", TaskLocality.PROCESS_LOCAL, false,
    )
    // gettingResultTime is the instant the fetch began, so back it off the finish time to get
    // the elapsed value the caller asked for.
    if (gettingResultTimeMs > 0) {
      info.gettingResultTime = launchTime + durationMs - gettingResultTimeMs
    }
    info.markFinished(TaskState.FINISHED, launchTime + durationMs)
    info
  }

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
