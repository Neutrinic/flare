package io.flare.spark.config

import scala.util.Try
import scala.util.matching.Regex

sealed trait TraceGranularity
object TraceGranularity {
  case object Jobs   extends TraceGranularity
  case object Stages extends TraceGranularity
  case object Tasks  extends TraceGranularity
  case object All    extends TraceGranularity

  def fromString(s: String): Either[String, TraceGranularity] = s.toLowerCase.trim match {
    case "jobs"   => Right(Jobs)
    case "stages" => Right(Stages)
    case "tasks"  => Right(Tasks)
    case "all"    => Right(All)
    case other    => Left(s"Unknown granularity '$other'. Expected: jobs, stages, tasks, all")
  }
}

final case class FlareConfig(
  enabled:          Boolean,
  granularity:      TraceGranularity,
  samplingRatio:    Double,
  maxSpansPerTrace: Int,
  slowTaskMs:       Long,    // 0 = disabled, >0 = only emit task spans slower than this
  retryTasksOnly:   Boolean, // true = only emit spans for retries and speculative tasks
  // v0.2 — stage-level task filtering
  // Empty set = no filter (all stages). Non-empty = only these stage IDs get task spans.
  taskStageIds:     Set[Int],
  // None = no filter. Some(r) = only stages whose name matches r get task spans.
  // Compiled once at load time, not per task.
  taskStagePattern: Option[Regex],
  metricsEnabled:   Boolean,      // FLARE_METRICS_ENABLED, default true
  // Caps for the free-form strings on SparkListenerSQLExecutionStart. 0 drops the
  // attribute entirely. See FlareConfig.DefaultSql*Chars for why these exist.
  sqlPlanMaxChars:        Int = FlareConfig.DefaultSqlPlanChars,
  sqlDetailsMaxChars:     Int = FlareConfig.DefaultSqlDetailsChars,
  sqlDescriptionMaxChars: Int = FlareConfig.DefaultSqlDescriptionChars,
  // Cap for the retained pre-AQE plan. Defaults to 0 (dropped) because keeping it doubles the
  // worst-case plan payload on every SQL span, and the post-AQE plan is the one that ran.
  // Raise it when you want the AQE decision itself, recoverable as a diff against spark.sql.plan.
  sqlPlanInitialMaxChars: Int = FlareConfig.DefaultSqlPlanInitialChars,
) {
  def tracesJobs:        Boolean = enabled
  def tracesStages:      Boolean = enabled && granularity != TraceGranularity.Jobs
  def tracesTasks:       Boolean = enabled && (granularity == TraceGranularity.Tasks || granularity == TraceGranularity.All)
  def tracesTaskDetails: Boolean = enabled && granularity == TraceGranularity.All

  /**
   * Whether a given task should emit a span.
   *
   * All active filters are AND-ed — every condition that is configured must pass.
   *
   * At task start, durationMs is unknown (-1) — slow task filter deferred to task end.
   * At task end, if durationMs < slowTaskMs the caller should drop the span (close scope,
   * do not call span.end()) rather than recording a span for a fast task.
   *
   * stageId / stageName are available from TaskContext on the executor. Pass them when
   * calling from ExecutorPlugin. Pass -1 / "" when calling from driver-side code (where
   * stage filtering does not apply — driver spans are never filtered by stage).
   */
  def shouldTraceTask(
    attemptNumber: Int,
    speculative:   Boolean,
    stageId:       Int    = -1,
    stageName:     String = "",
    durationMs:    Long   = -1L,
  ): Boolean = {
    if (!tracesTasks) return false
    // Stage ID filter — skip if IDs are configured and this stage is not in the set
    if (taskStageIds.nonEmpty && stageId >= 0 && !taskStageIds.contains(stageId)) return false
    // Stage name pattern filter — skip if pattern configured and name does not match
    if (taskStagePattern.isDefined && stageName.nonEmpty &&
        !taskStagePattern.get.pattern.matcher(stageName).matches()) return false
    // Retry-only filter
    if (retryTasksOnly && attemptNumber == 0 && !speculative) return false
    // Slow task filter — only applied when duration is known (task end)
    if (slowTaskMs > 0 && durationMs >= 0 && durationMs < slowTaskMs) return false
    true
  }
}

object FlareConfig {

  /**
   * Caps for the free-form strings on SparkListenerSQLExecutionStart.
   *
   * These are unbounded at the source. A wide query plan runs to tens of kilobytes, and one
   * attribute per SQL execution at that size is enough to push an OTLP batch over a collector's
   * message limit — at which point the whole batch is dropped, not just the plan. The caps trade
   * a partial plan for a delivered trace.
   *
   * The plan cap is deliberately far larger than the 500 used for `Stage.FailureReason`: a
   * failure reason is readable in its first line, whereas a plan truncated to 500 chars loses
   * every operator that matters.
   *
   * Override per deployment with FLARE_SQL_PLAN_MAX_CHARS / FLARE_SQL_DETAILS_MAX_CHARS /
   * FLARE_SQL_DESCRIPTION_MAX_CHARS. Raise them if your collector accepts larger payloads;
   * set one to 0 to drop that attribute entirely.
   */
  val DefaultSqlPlanChars        = 4096
  val DefaultSqlDetailsChars     = 2048
  val DefaultSqlDescriptionChars = 1024

  /**
   * The pre-AQE plan is off by default.
   *
   * `spark.sql.plan` already carries the plan that ran. Retaining the initial tree as well
   * doubles the worst-case plan payload on every SQL span, and for the common case — "what did
   * this query actually do" — it answers nothing the primary attribute does not.
   *
   * Its value is the diff: what AQE decided. Skew splits, broadcast conversion and partition
   * coalescing are only visible by comparing the two. That is a deliberate investigation, so it
   * is opt-in via FLARE_SQL_PLAN_INITIAL_MAX_CHARS.
   */
  val DefaultSqlPlanInitialChars = 0

  /**
   * Read a config value from system properties first, then environment variables.
   * System properties (-DFLARE_*) are set via spark.driver/executor.extraJavaOptions
   * and are reliably available on both driver and executor JVMs.
   * Environment variables are a fallback for direct container env or shell usage.
   */
  private def envOrProp(key: String): Option[String] =
    sys.props.get(key).orElse(sys.env.get(key))

  /** Load and validate config from system properties / env vars. Throws at startup on invalid config. */
  def load(): FlareConfig = {
    // Kept deliberately in step with FlareAutoConfig.isFlareEnabled(), which parses the same key
    // in Java because the agent extension classloader has no Scala runtime. Change both together.
    val enabled = !envOrProp("FLARE_ENABLED")
      .map(_.toLowerCase).contains("false")

    val granularity = envOrProp("FLARE_TRACE_GRANULARITY")
      .map(TraceGranularity.fromString)
      .getOrElse(Right(TraceGranularity.Stages)) match {
        case Right(g)  => g
        case Left(err) => throw new IllegalArgumentException(s"Flare config error: $err")
      }

    val samplingRatio = envOrProp("FLARE_SAMPLING_RATIO")
      .map { s =>
        val d = Try(s.toDouble).getOrElse(
          throw new IllegalArgumentException(s"Flare config error: FLARE_SAMPLING_RATIO '$s' is not a number")
        )
        if (d < 0.0 || d > 1.0)
          throw new IllegalArgumentException(s"Flare config error: FLARE_SAMPLING_RATIO must be 0.0-1.0, got $d")
        d
      }
      .getOrElse(0.1)

    val maxSpansPerTrace = envOrProp("FLARE_MAX_SPANS_PER_TRACE")
      .map { s =>
        val n = Try(s.toInt).getOrElse(
          throw new IllegalArgumentException(s"Flare config error: FLARE_MAX_SPANS_PER_TRACE '$s' is not an integer")
        )
        if (n <= 0)
          throw new IllegalArgumentException(s"Flare config error: FLARE_MAX_SPANS_PER_TRACE must be > 0, got $n")
        n
      }
      .getOrElse(10000)

    val slowTaskMs = envOrProp("FLARE_SLOW_TASK_MS")
      .map { s =>
        val n = Try(s.toLong).getOrElse(
          throw new IllegalArgumentException(s"Flare config error: FLARE_SLOW_TASK_MS '$s' is not a number")
        )
        if (n < 0)
          throw new IllegalArgumentException(s"Flare config error: FLARE_SLOW_TASK_MS must be >= 0, got $n")
        n
      }
      .getOrElse(0L)

    val retryTasksOnly = envOrProp("FLARE_RETRY_TASKS_ONLY")
      .map(_.toLowerCase == "true")
      .getOrElse(false)

    // v0.2 — parsed now so the fields exist, but shouldTraceTask honours them already.
    // Stage IDs: "7,12,43" → Set(7, 12, 43)
    val taskStageIds = envOrProp("FLARE_TASK_STAGES")
      .map { s =>
        s.split(",").map(_.trim).filter(_.nonEmpty).map { id =>
          Try(id.toInt).getOrElse(
            throw new IllegalArgumentException(s"Flare config error: FLARE_TASK_STAGES '$id' is not an integer")
          )
        }.toSet
      }
      .getOrElse(Set.empty[Int])

    // Stage name pattern: compiled once at startup, not per task
    val taskStagePattern = envOrProp("FLARE_TASK_STAGE_PATTERN")
      .map { s =>
        try s.r
        catch {
          case e: java.util.regex.PatternSyntaxException =>
            throw new IllegalArgumentException(s"Flare config error: FLARE_TASK_STAGE_PATTERN '$s' is not valid regex: ${e.getMessage}")
        }
      }

    val metricsEnabled = !envOrProp("FLARE_METRICS_ENABLED")
      .map(_.toLowerCase).contains("false")

    // 0 is legal and means "drop this attribute"; negative is a typo, not an intent.
    def charCap(key: String, default: Int): Int = envOrProp(key)
      .map { s =>
        val n = Try(s.toInt).getOrElse(
          throw new IllegalArgumentException(s"Flare config error: $key '$s' is not an integer")
        )
        if (n < 0)
          throw new IllegalArgumentException(s"Flare config error: $key must be >= 0, got $n")
        n
      }
      .getOrElse(default)

    FlareConfig(
      enabled          = enabled,
      granularity      = granularity,
      samplingRatio    = samplingRatio,
      maxSpansPerTrace = maxSpansPerTrace,
      slowTaskMs       = slowTaskMs,
      retryTasksOnly   = retryTasksOnly,
      taskStageIds     = taskStageIds,
      taskStagePattern = taskStagePattern,
      metricsEnabled   = metricsEnabled,
      sqlPlanMaxChars        = charCap("FLARE_SQL_PLAN_MAX_CHARS", DefaultSqlPlanChars),
      sqlDetailsMaxChars     = charCap("FLARE_SQL_DETAILS_MAX_CHARS", DefaultSqlDetailsChars),
      sqlDescriptionMaxChars = charCap("FLARE_SQL_DESCRIPTION_MAX_CHARS", DefaultSqlDescriptionChars),
      sqlPlanInitialMaxChars = charCap("FLARE_SQL_PLAN_INITIAL_MAX_CHARS", DefaultSqlPlanInitialChars),
    )
  }
}
