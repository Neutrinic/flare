package io.flare.spark.metrics

import io.opentelemetry.api.common.{AttributeKey, Attributes}

/**
 * Helpers for building OTEL `Attributes` used as metric dimensional tags.
 *
 * Uses `Attributes.builder()` instead of `Attributes.of()` to avoid
 * Scala-Java boxing ambiguity with `AttributeKey[Long]`.
 */
object MetricAttributes {

  private val ExecutorId = AttributeKey.stringKey("executor.id")
  private val StageName  = AttributeKey.stringKey("stage.name")
  private val Result     = AttributeKey.stringKey("task.result")

  def forTask(executorId: String, stageId: Int, result: String): Attributes =
    Attributes.builder()
      .put(ExecutorId, executorId)
      .put("stage.id", stageId.toLong)
      .put(Result, result)
      .build()

  def forStage(stageId: Int, stageName: String): Attributes =
    Attributes.builder()
      .put("stage.id", stageId.toLong)
      .put(StageName, stageName)
      .build()
}
