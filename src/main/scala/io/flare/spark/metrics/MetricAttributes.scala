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
  private val JobResult      = AttributeKey.stringKey("job.result")
  private val AppResult      = AttributeKey.stringKey("application.result")

  /**
   * The longest `sql.description` label value, in UTF-8 bytes (#205). Mimir and Cortex reject a
   * label value over 2,048 bytes by default, and they count bytes, not characters: 1,024 Chinese
   * characters, under `FLARE_SQL_DESCRIPTION_MAX_CHARS`'s default, are about 3,000 bytes. This bounds
   * the label whatever the character cap is set to.
   */
  val MaxLabelValueBytes: Int = 2048

  /**
   * The longest prefix of `s` that is at most `maxBytes` long in UTF-8, cut between characters,
   * never inside one, a surrogate pair included.
   */
  def utf8Prefix(s: String, maxBytes: Int): String = {
    var bytes = 0
    var i = 0
    while (i < s.length) {
      val cp = s.codePointAt(i)
      val n = if (cp < 0x80) 1 else if (cp < 0x800) 2 else if (cp < 0x10000) 3 else 4
      if (bytes + n > maxBytes) return s.substring(0, i)
      bytes += n
      i += Character.charCount(cp)
    }
    s
  }

  /**
   * Tags for task-level instruments, recorded on the executor.
   *
   * No stage id, and nothing else that is new for every stage (#136). A stage id is used once and
   * restarts at 0 in every application, so it cannot be compared across runs, and as a tag it only
   * creates series: the SDK re-exports every series it has seen on each cumulative export, and past
   * its limit of 2,000 per instrument folds new ones into a single overflow series. A long-lived
   * application, such as a Databricks all-purpose cluster, reaches that in hours. The executor's
   * TaskContext has no stage name to use instead; per-stage detail is on the stage span.
   */
  def forTask(executorId: String, result: String): Attributes =
    Attributes.builder()
      .put(ExecutorId, executorId)
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
   * Both tags are call sites (`collect at Job.scala:42`), bounded by the code rather than by how
   * long the application runs. There is deliberately no stage id (#136): see [[forTask]]. Stages
   * from the same call site share series, which is the aggregation a metric should give; a single
   * stage is on its span.
   *
   * Omitted rather than defaulted when the stage belongs to no SQL execution — a pure-RDD
   * stage has no description, and an empty tag would read as one that exists and is blank.
   */
  def forStage(stageName: String, sqlDescription: Option[String]): Attributes = {
    val b = Attributes.builder()
      .put(StageName, stageName)
    sqlDescription.filter(_.nonEmpty).foreach(d => b.put(SqlDescription, utf8Prefix(d, MaxLabelValueBytes)))
    b.build()
  }

  /**
   * Tags for the job outcome histogram (#168): the result, and the SQL execution's description when
   * the job belongs to one. Both are bounded by the code, not by how long the application runs, so
   * one query's runs share a series and can be compared with each other.
   */
  def forJob(result: String, sqlDescription: Option[String]): Attributes = {
    val b = Attributes.builder().put(JobResult, result)
    sqlDescription.filter(_.nonEmpty).foreach(d => b.put(SqlDescription, utf8Prefix(d, MaxLabelValueBytes)))
    b.build()
  }

  /** Tags for the application outcome histogram (#168). */
  def forApplication(result: String): Attributes =
    Attributes.builder().put(AppResult, result).build()

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
  /** A task lost with its executor (#200): the executor, and why it was lost, bucketed as below. */
  def forTaskLost(executorId: String, reason: String): Attributes = forExecutorRemoval(executorId, reason)

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
