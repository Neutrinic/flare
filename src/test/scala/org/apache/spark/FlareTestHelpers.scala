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

  /**
   * An ExceptionFailure as Spark builds one on a real task failure.
   *
   * The `(Throwable, Seq[AccumulableInfo])` convenience constructor is private[spark] AND its
   * signature has moved across versions, so the primary apply is used instead with
   * `exceptionWrapper = None` — that parameter is a private[spark] type, which is why this has
   * to live in this package at all. The remaining parameters take their defaults.
   */
  def exceptionFailure(t: Throwable): ExceptionFailure = {
    val writer = new java.io.StringWriter()
    t.printStackTrace(new java.io.PrintWriter(writer))
    ExceptionFailure(
      className      = t.getClass.getName,
      description    = t.getMessage,
      stackTrace     = t.getStackTrace,
      fullStackTrace = writer.toString,
      exceptionWrapper = None,
    )
  }

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

  // ── Cluster lifecycle fixtures (#49) ─────────────────────────────────────
  //
  // BlockManagerId's constructor is private[spark]; apply() is the supported route and its
  // (execId, host, port) shape is identical across 3.3.4-4.2.0.

  def blockManagerId(execId: String): org.apache.spark.storage.BlockManagerId =
    org.apache.spark.storage.BlockManagerId(execId, "host-" + execId, 7077)

  def executorAdded(execId: String): scheduler.SparkListenerExecutorAdded =
    scheduler.SparkListenerExecutorAdded(
      System.currentTimeMillis(), execId,
      new scheduler.cluster.ExecutorInfo("host-" + execId, 4, Map.empty[String, String]),
    )

  def executorRemoved(execId: String, reason: String): scheduler.SparkListenerExecutorRemoved =
    scheduler.SparkListenerExecutorRemoved(System.currentTimeMillis(), execId, reason)

  def executorExcluded(execId: String): scheduler.SparkListenerExecutorExcluded =
    scheduler.SparkListenerExecutorExcluded(System.currentTimeMillis(), execId, 1)

  def blockManagerAdded(execId: String): scheduler.SparkListenerBlockManagerAdded =
    scheduler.SparkListenerBlockManagerAdded(
      System.currentTimeMillis(), blockManagerId(execId), 1024L)

  def blockManagerRemoved(execId: String): scheduler.SparkListenerBlockManagerRemoved =
    scheduler.SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId(execId))

  def unpersistRDD(rddId: Int): scheduler.SparkListenerUnpersistRDD =
    scheduler.SparkListenerUnpersistRDD(rddId)

  /** `cached = false` reproduces Spark's drop signal: an invalid StorageLevel with the sizes. */
  def blockUpdated(
    execId: String, memSize: Long, diskSize: Long, cached: Boolean,
  ): scheduler.SparkListenerBlockUpdated = {
    val level =
      if (cached) org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
      else org.apache.spark.storage.StorageLevel.NONE
    scheduler.SparkListenerBlockUpdated(
      org.apache.spark.storage.BlockUpdatedInfo(
        blockManagerId(execId),
        org.apache.spark.storage.RDDBlockId(1, 0),
        level, memSize, diskSize,
      )
    )
  }
}
