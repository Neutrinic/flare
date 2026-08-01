package io.flare.spark.config

import munit.FunSuite

class FlareConfigTest extends FunSuite {

  // ── Helper: build configs with specific filters ────────────────────────────

  private def baseConfig(
    granularity:    TraceGranularity = TraceGranularity.Tasks,
    retryTasksOnly: Boolean = false,
    slowTaskMs:     Long = 0L,
    taskStageIds:   Set[Int] = Set.empty,
    taskStagePattern: Option[scala.util.matching.Regex] = None,
  ): FlareConfig =
    FlareConfig(
      enabled          = true,
      granularity      = granularity,
      samplingRatio    = 1.0,
      maxSpansPerTrace = 10000,
      slowTaskMs       = slowTaskMs,
      retryTasksOnly   = retryTasksOnly,
      taskStageIds     = taskStageIds,
      taskStagePattern = taskStagePattern,
      metricsEnabled   = true,
    )

  // ── shouldTraceTask tests ──────────────────────────────────────────────────

  test("shouldTraceTask returns false when enabled is false regardless of granularity") {
    val config = baseConfig().copy(enabled = false)
    assert(!config.shouldTraceTask(attemptNumber = 0, speculative = false))
  }

  test("shouldTraceTask passes when no filters configured") {
    val config = baseConfig()
    assert(config.shouldTraceTask(attemptNumber = 0, speculative = false))
  }

  test("shouldTraceTask returns false when granularity is Stages") {
    val config = baseConfig(granularity = TraceGranularity.Stages)
    assert(!config.shouldTraceTask(attemptNumber = 0, speculative = false))
  }

  test("shouldTraceTask returns false when granularity is Jobs") {
    val config = baseConfig(granularity = TraceGranularity.Jobs)
    assert(!config.shouldTraceTask(attemptNumber = 0, speculative = false))
  }

  test("shouldTraceTask filters by stage ID — blocks non-matching") {
    val config = baseConfig(taskStageIds = Set(7, 12))
    assert(!config.shouldTraceTask(attemptNumber = 0, speculative = false, stageId = 5))
  }

  test("shouldTraceTask filters by stage ID — passes matching") {
    val config = baseConfig(taskStageIds = Set(7, 12))
    assert(config.shouldTraceTask(attemptNumber = 0, speculative = false, stageId = 12))
  }

  test("shouldTraceTask skips stage ID filter when stageId is -1") {
    val config = baseConfig(taskStageIds = Set(7, 12))
    assert(config.shouldTraceTask(attemptNumber = 0, speculative = false, stageId = -1))
  }

  test("shouldTraceTask filters by stage name pattern — blocks non-matching") {
    val config = baseConfig(taskStagePattern = Some(".*shuffle.*".r))
    assert(!config.shouldTraceTask(
      attemptNumber = 0, speculative = false, stageName = "mapPartitions"))
  }

  test("shouldTraceTask filters by stage name pattern — passes matching") {
    val config = baseConfig(taskStagePattern = Some(".*shuffle.*".r))
    assert(config.shouldTraceTask(
      attemptNumber = 0, speculative = false, stageName = "shuffle read at MyJob.scala:42"))
  }

  test("shouldTraceTask skips pattern check when stageName is empty") {
    val config = baseConfig(taskStagePattern = Some(".*shuffle.*".r))
    assert(config.shouldTraceTask(attemptNumber = 0, speculative = false, stageName = ""))
  }

  test("shouldTraceTask retryOnly blocks attempt 0 non-speculative") {
    val config = baseConfig(retryTasksOnly = true)
    assert(!config.shouldTraceTask(attemptNumber = 0, speculative = false))
  }

  test("shouldTraceTask retryOnly passes attempt > 0") {
    val config = baseConfig(retryTasksOnly = true)
    assert(config.shouldTraceTask(attemptNumber = 1, speculative = false))
  }

  test("shouldTraceTask retryOnly passes speculative task") {
    val config = baseConfig(retryTasksOnly = true)
    assert(config.shouldTraceTask(attemptNumber = 0, speculative = true))
  }

  test("shouldTraceTask slowTaskMs blocks fast tasks") {
    val config = baseConfig(slowTaskMs = 1000L)
    assert(!config.shouldTraceTask(attemptNumber = 0, speculative = false, durationMs = 500L))
  }

  test("shouldTraceTask slowTaskMs passes slow tasks") {
    val config = baseConfig(slowTaskMs = 1000L)
    assert(config.shouldTraceTask(attemptNumber = 0, speculative = false, durationMs = 1500L))
  }

  test("shouldTraceTask slowTaskMs skipped when durationMs is -1 (unknown at start)") {
    val config = baseConfig(slowTaskMs = 1000L)
    assert(config.shouldTraceTask(attemptNumber = 0, speculative = false, durationMs = -1L))
  }

  test("shouldTraceTask all filters AND together") {
    val config = baseConfig(
      retryTasksOnly = true,
      slowTaskMs     = 500L,
      taskStageIds   = Set(7),
    )
    // Passes all filters: retry attempt, slow enough, correct stage
    assert(config.shouldTraceTask(
      attemptNumber = 1, speculative = false, stageId = 7, durationMs = 1000L))
    // Fails retry filter
    assert(!config.shouldTraceTask(
      attemptNumber = 0, speculative = false, stageId = 7, durationMs = 1000L))
    // Fails stage filter
    assert(!config.shouldTraceTask(
      attemptNumber = 1, speculative = false, stageId = 5, durationMs = 1000L))
    // Fails slow task filter
    assert(!config.shouldTraceTask(
      attemptNumber = 1, speculative = false, stageId = 7, durationMs = 100L))
  }

  // ── FlareConfig.load() parsing tests ───────────────────────────────────────

  // Use system properties to inject test values (FlareConfig reads sys.props first)
  private val testProps = List(
    "FLARE_ENABLED", "FLARE_TRACE_GRANULARITY", "FLARE_SAMPLING_RATIO",
    "FLARE_MAX_SPANS_PER_TRACE", "FLARE_SLOW_TASK_MS", "FLARE_RETRY_TASKS_ONLY",
    "FLARE_TASK_STAGES", "FLARE_TASK_STAGE_PATTERN", "FLARE_METRICS_ENABLED",
    "FLARE_SQL_PLAN_MAX_CHARS", "FLARE_SQL_DETAILS_MAX_CHARS", "FLARE_SQL_DESCRIPTION_MAX_CHARS",
  )

  override def afterEach(context: AfterEach): Unit = {
    testProps.foreach(sys.props.remove)
    super.afterEach(context)
  }

  test("load() parses FLARE_TASK_STAGES correctly") {
    sys.props("FLARE_TASK_STAGES") = "7,12, 43"
    val config = FlareConfig.load()
    assertEquals(config.taskStageIds, Set(7, 12, 43))
  }

  test("load() throws on non-integer FLARE_TASK_STAGES") {
    sys.props("FLARE_TASK_STAGES") = "7,abc,12"
    interceptMessage[IllegalArgumentException]("Flare config error: FLARE_TASK_STAGES 'abc' is not an integer") {
      FlareConfig.load()
    }
  }

  test("load() compiles FLARE_TASK_STAGE_PATTERN as regex") {
    sys.props("FLARE_TASK_STAGE_PATTERN") = ".*shuffle.*"
    val config = FlareConfig.load()
    assert(config.taskStagePattern.isDefined)
    assert(config.taskStagePattern.get.pattern.matcher("shuffle read").matches())
  }

  test("load() throws on bad FLARE_TASK_STAGE_PATTERN regex") {
    sys.props("FLARE_TASK_STAGE_PATTERN") = "[invalid"
    intercept[IllegalArgumentException] {
      FlareConfig.load()
    }
  }

  test("load() throws on out-of-range FLARE_SAMPLING_RATIO") {
    sys.props("FLARE_SAMPLING_RATIO") = "1.5"
    interceptMessage[IllegalArgumentException]("Flare config error: FLARE_SAMPLING_RATIO must be 0.0-1.0, got 1.5") {
      FlareConfig.load()
    }
  }

  test("load() throws on non-numeric FLARE_SAMPLING_RATIO") {
    sys.props("FLARE_SAMPLING_RATIO") = "abc"
    interceptMessage[IllegalArgumentException]("Flare config error: FLARE_SAMPLING_RATIO 'abc' is not a number") {
      FlareConfig.load()
    }
  }

  test("load() FLARE_ENABLED=false disables everything") {
    sys.props("FLARE_ENABLED") = "false"
    val config = FlareConfig.load()
    assert(!config.enabled)
    assert(!config.shouldTraceTask(attemptNumber = 0, speculative = false))
  }

  test("load() defaults are sensible") {
    val config = FlareConfig.load()
    assert(config.enabled)
    assertEquals(config.granularity, TraceGranularity.Stages)
    assertEquals(config.samplingRatio, 0.1)
    assertEquals(config.maxSpansPerTrace, 10000)
    assertEquals(config.slowTaskMs, 0L)
    assert(!config.retryTasksOnly)
    assert(config.taskStageIds.isEmpty)
    assert(config.taskStagePattern.isEmpty)
    assert(config.metricsEnabled)
    assertEquals(config.sqlPlanMaxChars, 4096)
    assertEquals(config.sqlDetailsMaxChars, 2048)
    assertEquals(config.sqlDescriptionMaxChars, 1024)
  }

  test("load() parses the SQL truncation caps") {
    sys.props("FLARE_SQL_PLAN_MAX_CHARS")        = "65536"
    sys.props("FLARE_SQL_DETAILS_MAX_CHARS")     = "0"
    sys.props("FLARE_SQL_DESCRIPTION_MAX_CHARS") = "256"
    val config = FlareConfig.load()
    assertEquals(config.sqlPlanMaxChars, 65536)
    assertEquals(config.sqlDetailsMaxChars, 0)
    assertEquals(config.sqlDescriptionMaxChars, 256)
  }

  test("load() throws on a negative SQL truncation cap") {
    sys.props("FLARE_SQL_PLAN_MAX_CHARS") = "-1"
    interceptMessage[IllegalArgumentException](
      "Flare config error: FLARE_SQL_PLAN_MAX_CHARS must be >= 0, got -1"
    ) {
      FlareConfig.load()
    }
  }

  test("load() throws on a non-integer SQL truncation cap") {
    sys.props("FLARE_SQL_DETAILS_MAX_CHARS") = "4k"
    interceptMessage[IllegalArgumentException](
      "Flare config error: FLARE_SQL_DETAILS_MAX_CHARS '4k' is not an integer"
    ) {
      FlareConfig.load()
    }
  }

  test("load() FLARE_METRICS_ENABLED=false disables metrics") {
    sys.props("FLARE_METRICS_ENABLED") = "false"
    val config = FlareConfig.load()
    assert(!config.metricsEnabled)
  }

  test("load() FLARE_METRICS_ENABLED defaults to true") {
    val config = FlareConfig.load()
    assert(config.metricsEnabled)
  }
}
