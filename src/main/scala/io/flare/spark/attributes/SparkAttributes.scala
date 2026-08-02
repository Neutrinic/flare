package io.flare.spark.attributes

import io.opentelemetry.api.common.AttributeKey

object SparkAttributes {

  object Application {
    val Id     = AttributeKey.stringKey("spark.application.id")
    val Name   = AttributeKey.stringKey("spark.application.name")
    val Master = AttributeKey.stringKey("spark.master.url")
  }

  object Job {
    val Id          = AttributeKey.longKey("spark.job.id")
    val Result      = AttributeKey.stringKey("spark.job.result")
    val Description = AttributeKey.stringKey("spark.job.description")
    val StageCount  = AttributeKey.longKey("spark.job.stage.count")
  }

  object Stage {
    val Id              = AttributeKey.longKey("spark.stage.id")
    val AttemptId       = AttributeKey.longKey("spark.stage.attempt.id")
    val Name            = AttributeKey.stringKey("spark.stage.name")
    val TaskCount       = AttributeKey.longKey("spark.stage.task.count")
    val ExecutorRunTime = AttributeKey.longKey("spark.stage.executor.run_time_ms")
    val ExecutorCpuTime = AttributeKey.longKey("spark.stage.executor.cpu_time_ms")
    val InputBytes      = AttributeKey.longKey("spark.stage.input.bytes")
    val InputRecords    = AttributeKey.longKey("spark.stage.input.records")
    val OutputBytes     = AttributeKey.longKey("spark.stage.output.bytes")
    val OutputRecords   = AttributeKey.longKey("spark.stage.output.records")
    val ShuffleReadBytes  = AttributeKey.longKey("spark.stage.shuffle.read_bytes")
    val ShuffleWriteBytes = AttributeKey.longKey("spark.stage.shuffle.write_bytes")
    val MemorySpilled   = AttributeKey.longKey("spark.stage.memory.spilled_bytes")
    val DiskSpilled     = AttributeKey.longKey("spark.stage.disk.spilled_bytes")
    val FailureReason   = AttributeKey.stringKey("spark.stage.failure_reason")

    /**
     * Where a slow stage's time actually went.
     *
     * Like ExecutorRunTime above, each of these is Spark's own sum across every task in the
     * stage, so they are directly comparable with it — a stage whose GC time rivals its run
     * time has its answer right here, and total duration alone never shows that.
     */
    val JvmGcTime                  = AttributeKey.longKey("spark.stage.jvm.gc_time_ms")
    val ExecutorDeserializeTime    = AttributeKey.longKey("spark.stage.executor.deserialize_time_ms")
    val ExecutorDeserializeCpuTime = AttributeKey.longKey("spark.stage.executor.deserialize_cpu_time_ms")
    val ResultSerializationTime    = AttributeKey.longKey("spark.stage.result.serialization_time_ms")
    // Derived rather than reported — see TracingSparkListener.schedulerDelayMs.
    val SchedulerDelay             = AttributeKey.longKey("spark.stage.scheduler.delay_ms")
  }

  /**
   * Attributes on the `spark.sql.N` span, sourced from SparkListenerSQLExecutionStart.
   *
   * Fields are read by name rather than by position: Spark has both added fields
   * (`modifiedConfigs`, `jobTags`) and inserted them ahead of `description`
   * (`rootExecutionId` in 4.0) across the supported matrix.
   */
  object Sql {
    val ExecutionId = AttributeKey.longKey("spark.sql.execution.id")
    val Description = AttributeKey.stringKey("spark.sql.description")
    val Details     = AttributeKey.stringKey("spark.sql.details")
    // physicalPlanDescription — the formatted plan, truncated on the way in.
    // Holds the LATEST plan seen: the tree from SparkListenerSQLExecutionStart until AQE
    // re-plans, then each SparkListenerSQLAdaptiveExecutionUpdate overwrites it. So this is
    // the plan that ran, not the pre-AQE tree that always reports isFinalPlan=false.
    val Plan          = AttributeKey.stringKey("spark.sql.plan")
    val PlanTruncated = AttributeKey.booleanKey("spark.sql.plan.truncated")
    // The pre-AQE tree, retained separately so the AQE decision is recoverable as a diff.
    // Off by default — see FLARE_SQL_PLAN_INITIAL_MAX_CHARS.
    val PlanInitial          = AttributeKey.stringKey("spark.sql.plan.initial")
    val PlanInitialTruncated = AttributeKey.booleanKey("spark.sql.plan.initial.truncated")
  }

  /**
   * Attributes on the `spark.task.executor` span, which is created on the EXECUTOR JVM from
   * `TaskContext`. That is the constraint on what can live here: host, locality and speculative
   * are `TaskInfo` fields that only the driver ever sees, so they are not available at this
   * span's call site.
   */
  object Task {
    val PartitionId = AttributeKey.longKey("spark.task.partition.id")
    val AttemptId   = AttributeKey.longKey("spark.task.attempt.id")
    val Result      = AttributeKey.stringKey("spark.task.result")
    // v0.2 — task-level metrics (recorded at task end from TaskMetrics)
    val ShuffleReadBytes  = AttributeKey.longKey("spark.task.shuffle.read_bytes")
    val ShuffleWriteBytes = AttributeKey.longKey("spark.task.shuffle.write_bytes")
    val PeakMemory        = AttributeKey.longKey("spark.task.peak_memory_bytes")
    val InputBytes        = AttributeKey.longKey("spark.task.input.bytes")
    val OutputBytes       = AttributeKey.longKey("spark.task.output.bytes")
    val DurationMs        = AttributeKey.longKey("spark.task.duration_ms")
    val SqlExecutionId    = AttributeKey.longKey("spark.task.sql.execution_id")
  }

  // OTEL semantic conventions
  object Error {
    // Not yet set anywhere — see #46, which pairs it with Span.recordException.
    val Type    = AttributeKey.stringKey("error.type")
    val Message = AttributeKey.stringKey("error.message")
  }

  object Flare {
    val Version          = AttributeKey.stringKey("flare.version")
    val TraceGranularity = AttributeKey.stringKey("flare.trace.granularity")
  }
}
