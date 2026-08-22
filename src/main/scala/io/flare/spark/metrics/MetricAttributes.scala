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

  /**
   * Tags for cluster lifecycle instruments (#49).
   *
   * `executor.id` only. Deliberately no host, no block id, no RDD id: these instruments are
   * gauges over the life of an application, so anything unbounded here would accumulate series
   * forever rather than per query. Removal reasons are a separate, low-cardinality tag.
   */
  def forExecutor(executorId: String): Attributes =
    Attributes.builder().put(ExecutorId, executorId).build()

  /**
   * Executor removal, tagged with why.
   *
   * Spark's reason string is free text and sometimes embeds ids or hostnames, so it is
   * bucketed rather than passed through — an unbounded tag on a counter is exactly the
   * cardinality problem these instruments exist to avoid.
   */
  def forExecutorRemoval(executorId: String, reason: String): Attributes =
    Attributes.builder()
      .put(ExecutorId, executorId)
      .put(Reason, bucketRemovalReason(reason))
      .build()

  private val Reason = AttributeKey.stringKey("reason")

  private[metrics] def bucketRemovalReason(reason: String): String = {
    val r = Option(reason).getOrElse("").toLowerCase
    if (r.isEmpty) "unknown"
    // Spark's own wording for a dynamic-allocation scale-down. This is the one that must be
    // distinguishable from a failure, since it is routine rather than a problem.
    else if (r.contains("idle") || r.contains("decommission")) "idle_or_decommissioned"
    else if (r.contains("preempt")) "preempted"
    else if (r.contains("heartbeat")) "heartbeat_timeout"
    else if (r.contains("lost") || r.contains("disconnect")) "lost"
    else if (r.contains("killed") || r.contains("kill")) "killed"
    else if (r.contains("exit")) "exited"
    else "other"
  }
}
