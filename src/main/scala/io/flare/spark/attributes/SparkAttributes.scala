package io.flare.spark.attributes

import io.opentelemetry.api.common.AttributeKey

object SparkAttributes {

  object Application {
    val Id      = AttributeKey.stringKey("spark.application.id")
    val Name    = AttributeKey.stringKey("spark.application.name")
    val Master  = AttributeKey.stringKey("spark.master.url")
    val Version = AttributeKey.stringKey("spark.version")
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
    val FailureReason   = AttributeKey.stringKey("spark.stage.failure_reason")
  }

  object Task {
    val ExecutorId  = AttributeKey.stringKey("spark.task.executor.id")
    val Host        = AttributeKey.stringKey("spark.task.host")
    val PartitionId = AttributeKey.longKey("spark.task.partition.id")
    val AttemptId   = AttributeKey.longKey("spark.task.attempt.id")
    val Speculative = AttributeKey.booleanKey("spark.task.speculative")
    val Result      = AttributeKey.stringKey("spark.task.result")
    val Locality    = AttributeKey.stringKey("spark.task.locality")
  }

  // OTEL semantic conventions
  object Error {
    val Type    = AttributeKey.stringKey("error.type")
    val Message = AttributeKey.stringKey("error.message")
  }

  object Flare {
    val Version          = AttributeKey.stringKey("flare.version")
    val TraceGranularity = AttributeKey.stringKey("flare.trace.granularity")
  }
}
