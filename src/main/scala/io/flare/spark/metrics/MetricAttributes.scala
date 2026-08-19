package io.flare.spark.metrics

import io.opentelemetry.api.common.{AttributeKey, Attributes}

/**
 * Helpers for building OTEL `Attributes` used as metric dimensional tags.
 *
 * Uses `Attributes.builder()` instead of `Attributes.of()` to avoid
 * Scala-Java boxing ambiguity with `AttributeKey[Long]`.
 */
object MetricAttributes {

  private val ExecutorId     = AttributeKey.stringKey("executor.id")
  private val StageName      = AttributeKey.stringKey("stage.name")
  private val SqlDescription = AttributeKey.stringKey("sql.description")
  private val Result         = AttributeKey.stringKey("task.result")

  def forTask(executorId: String, stageId: Int, result: String): Attributes =
    Attributes.builder()
      .put(ExecutorId, executorId)
      .put("stage.id", stageId.toLong)
      .put(Result, result)
      .build()

  /**
   * Tags for stage-level instruments.
   *
   * `stage.name` is Spark's own `StageInfo.name` and is left exactly as-is, so anything
   * correlating with the Spark UI still matches. For async subquery and broadcast stages it
   * resolves inside a Spark thread pool and reads `$anonfun$withThreadLocalCaptured$2 at
   * CompletableFuture.java:1768`, which identifies nothing — hence `sql.description`, which
   * carries the SQL execution's own label (`show at PipelineJob.scala:58`). Same fix as #48
   * applied to the metric surface; see #75.
   *
   * Adding it costs **zero** additional series. Every series already carries `instance`
   * (`service.instance.id`, a per-JVM UUID), and a given stage in a given JVM has exactly one
   * SQL description — so `sql.description` is functionally determined by labels that are
   * already present. Verified against Mimir: `(stage_id, instance)` and
   * `(stage_id, instance, stage_name)` both yield 6 series over two applications.
   *
   * Omitted rather than defaulted when the stage belongs to no SQL execution — a pure-RDD
   * stage has no description, and an empty tag would read as one that exists and is blank.
   */
  def forStage(stageId: Int, stageName: String, sqlDescription: Option[String]): Attributes = {
    val b = Attributes.builder()
      .put("stage.id", stageId.toLong)
      .put(StageName, stageName)
    sqlDescription.filter(_.nonEmpty).foreach(b.put(SqlDescription, _))
    b.build()
  }
}
