# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Stage timing breakdown** — `spark.stage.jvm.gc_time_ms`,
  `spark.stage.executor.deserialize_time_ms`, `spark.stage.executor.deserialize_cpu_time_ms`,
  `spark.stage.result.serialization_time_ms`, `spark.stage.disk.spilled_bytes` and
  `spark.stage.scheduler.delay_ms`. Total stage duration says a stage was slow; these say why,
  and GC time in particular is often the whole answer. All are sums across the stage's tasks, so
  they are directly comparable with the existing `spark.stage.executor.run_time_ms`.
  Scheduler delay is the only one Spark does not report: it is derived per task as the wall clock
  left after deserialization, run time, result serialization and result fetch, then summed.
  It is clamped at zero per task rather than on the sum, because the driver and executor clocks
  are different clocks and a fast task can report slightly more run time than its own duration —
  summing first would let that cancel real queueing measured elsewhere in the stage. The
  attribute is omitted entirely, rather than exported as `0`, for a stage that reported no task
  ends, so "not measured" never reads as "no queueing" ([#45])
- **SQL execution metadata on `spark.sql.N` spans** — the span previously carried no attributes
  at all. It now records `spark.sql.execution.id`, `spark.sql.description`, `spark.sql.details`
  and `spark.sql.plan` (the formatted physical plan), all sourced from
  `SparkListenerSQLExecutionStart`. `spark.sql.description` is Spark's own query label, e.g.
  `count at PipelineJob.scala:43`, which identifies a query far better than the stage names
  derived from `RDD.creationSite`.
  Free-form strings are capped — plan at 4096 characters, details at 2048, description at 1024 —
  because they are unbounded at the source, and one oversized attribute can push an OTLP batch
  past a collector's message limit, dropping the entire batch rather than just the plan. A
  clipped plan sets `spark.sql.plan.truncated=true`, so a partial plan is never mistaken for a
  complete one. Empty or absent values are omitted rather than exported as empty attributes.
  Per-operator metrics from `sparkPlanInfo` are not included ([#44], tracked in [#47])
- **`FLARE_SQL_PLAN_MAX_CHARS` / `FLARE_SQL_DETAILS_MAX_CHARS` /
  `FLARE_SQL_DESCRIPTION_MAX_CHARS`** — the SQL truncation caps above are now configurable rather
  than compiled in, so a deployment whose collector accepts larger payloads can keep the whole
  plan. Setting a cap to `0` drops that attribute entirely; a negative value is rejected at
  startup ([#44])
- **Real-agent extension test** — a forked JVM runs the supported OpenTelemetry Java agent with
  Flare's assembled JAR, then verifies the packaged SPI descriptor, generated version metadata,
  and exported `flare.role` / `flare.version` resource attributes
- **OTEL metrics instruments** — task-level `DoubleHistogram` for duration and throughput,
  `LongCounter` for shuffle bytes, recorded in `FlareExecutorPlugin.endTask()` with automatic
  exemplar linking (metrics recorded while span scope is active). Stage-level counters and
  histograms recorded in `TracingSparkListener.onStageCompleted()` for executor run time,
  input/output bytes, and shuffle bytes ([#10], [#11])
- **`FLARE_METRICS_ENABLED`** — kill switch for OTEL metrics emission (default `true`).
  When disabled, all instruments use a no-op meter with zero overhead
- **`FlareMetrics`** — centralized holder for all OTEL metric instruments, constructed from
  a `Meter` instance for testability. Factory method `FlareMetrics.create(enabled)` selects
  real vs no-op meter
- **`MetricAttributes`** — helper for building `Attributes` with correct Scala→Java Long
  boxing for metric dimensional tags (`executor.id`, `stage.id`, `task.result`, `stage.name`)
- **Per-stage traceparent injection** — `SubmitMissingTasksInstrumentation` hooks
  `DAGScheduler.submitMissingTasks(stage, jobId)` via ByteBuddy to inject a per-stage
  traceparent into `ActiveJob.properties` before tasks are created. Executor task spans now
  nest under their specific stage span (`app → job → stage → task`). Captures ALL stages
  including AQE (Adaptive Query Execution) sub-jobs that bypass `SparkContext.runJob`.
  Job and stage spans are pre-created via reflection on DAGScheduler internals, then adopted
  by `TracingSparkListener` when `onJobStart`/`onStageSubmitted` fire. Backward compatible
  with SparkPlugin-only mode ([#26])
- **ByteBuddy TaskRunner context restoration** — `TaskRunnerInstrumentationModule` hooks
  `Executor$TaskRunner.run()` to extract `traceparent` from `TaskDescription.properties`
  and make the parent OTEL context current for the entire task execution. Downstream
  OTEL-instrumented libraries (JDBC, HTTP, gRPC) inside tasks now inherit the correct
  parent context. Context-only — no span creation, `FlareExecutorPlugin` manages span
  lifecycle as before ([#15])
- **`LocalPropertyPropagator.extractFromProperties`** — overloaded extraction method taking
  `java.util.Properties` directly for use in TaskRunner advice where `TaskContext` is null
- **ByteBuddy SparkContext auto-registration** — `SparkContextInstrumentationModule` hooks
  `SparkContext.<init>` constructor exit via ByteBuddy advice to auto-register
  `TracingSparkListener` and create the application span without `spark.plugins` config.
  True zero-config when using the OTEL agent extension ([#14])
- **FlareDriverState** — shared state holder with `@volatile` + `synchronized` guard for
  dedup between SparkPlugin (Phase 1) and ByteBuddy (Phase 2) paths. First path to
  initialize wins; the other is a no-op
- **InstrumentationModule SPI** — `META-INF/services/` registration for auto-discovery by
  the OTEL Java agent. Module, TypeInstrumentation, and Advice written in Java for agent
  classloader compatibility (Scala runtime not available in extension classloader)
- **FlareDriverState tests** — 8 tests covering initialization, dedup, shutdown idempotency,
  re-initialization, and concurrent initialization race safety

### Changed
- **OpenTelemetry baseline** — Java agent 2.30.0, API/SDK 1.64.0, and
  `byte-buddy-dep` 1.18.11
- **Consumer dependency update** — `opentelemetry-api` and `opentelemetry-context` move from
  1.50.0 to 1.64.0. Both are compile-scoped and bundled in the assembly, so this updates the
  published POM dependency graph and shipped bytecode. OpenTelemetry 1.x compatibility should
  preserve consumers, but applications should validate dependency convergence when upgrading
- **Startup diagnostics** — all live Scala initialization paths log the active trace granularity,
  sampling ratio, and maximum spans per trace after configuration validation
- **Dockerfile stable JAR names** — `COPY flare-spark.jar` and `COPY flare-examples.jar`
  instead of `flare-spark-assembly-${FLARE_VERSION}.jar`; removed `FLARE_VERSION` build arg

### Fixed
- **`FLARE_SLOW_TASK_MS` now actually suppresses spans** — it never has. The abandon path in
  `FlareExecutorPlugin.endTask` ended with a `return` from inside a closure passed to `foreach`,
  which in Scala compiles to a thrown `NonLocalReturnControl`. The enclosing `try`'s `finally`
  therefore still ran and called `span.end()`, exporting the very span the filter had just
  decided to drop. Tasks below the threshold were exported exactly as if the filter were unset,
  with no error to indicate it. Two knock-on effects went with it: `scope.close()` ran twice, and
  `spanCount.decrementAndGet()` released a `FLARE_MAX_SPANS_PER_TRACE` slot for a span that was
  in fact exported, so the circuit breaker admitted more spans than its configured limit.
  Verified against the dev stack: with `FLARE_SLOW_TASK_MS=500`, the same job dropped from 13
  exported task spans to 2, the only two that exceeded the threshold ([#57])
- **Task metrics are no longer dropped when the task span is suppressed** — every guard in
  `FlareExecutorPlugin.onTaskStart()` (granularity, sampling, stage filters, and the
  `FLARE_MAX_SPANS_PER_TRACE` circuit breaker) returned before any measurement state was armed,
  so `flare.task.duration` and the shuffle counters saw nothing for those tasks. On a job that
  tripped the breaker, or at the default `stages` granularity, the task histogram was empty or
  badly undercounted — and a drop caused by span filtering read as a drop in cluster throughput.
  Metric state is now armed independently of the span, so a suppressed span still yields a
  measurement ([#53])
- **Dangling exemplars on tail-filtered tasks** — a task suppressed by `FLARE_SLOW_TASK_MS` has
  its span abandoned rather than ended, so it is never exported. Its metric was nonetheless
  recorded while the span scope was still current, and the SDK's default `trace_based` exemplar
  filter stamped the data point with that span's ID. Clicking the exemplar in Grafana opened a
  trace that does not exist. The scope is now closed before recording in that path ([#52])
- **Agent resource attributes** — register the Java
  `AutoConfigurationCustomizerProvider` through ServiceLoader so `flare.role` and
  `flare.version` are emitted. Version metadata now comes from an sbt-generated classpath
  resource instead of nullable JAR package metadata
- **Executor role detection** — `flare.role` fell back to `driver` on every standalone and YARN
  executor, because only Kubernetes sets `SPARK_EXECUTOR_ID`. `FlareAutoConfig` now also parses
  `--executor-id` out of `sun.java.command`, which is the only executor identity available at
  agent premain ([#42])
- **Missing build metadata is no longer fatal** — `loadFlareVersion()` warns and reports
  `flare.version=unknown` instead of throwing. It runs inside the agent premain, so a repackaged
  extension JAR that lost the resource would have aborted auto-configuration for the whole JVM
  ([#42])
- **Agent test port collision** — the reserved OTLP collector port is released before the test
  JVM forks, so the stub collector now retries the bind and reports the real cause rather than
  surfacing a bare `BindException` ([#42])
- **Dev stack: Mimir now stores exemplars** — `limits.max_global_exemplars_per_user` defaults to
  `0`, meaning disabled, so every exemplar the SDK attached was accepted over OTLP and silently
  discarded. The rest of the chain was correct — the executor plugin records inside the span
  scope, the Mimir datasource declares `exemplarTraceIdDestinations`, the dashboard panel sets
  `"exemplar": true` — so the only symptom was a panel with no exemplar dots, which is
  indistinguishable from having no traffic ([#58])

### Documentation
- **Resource attributes** — README documents `flare.role` and `flare.version`, how the role is
  derived per deployment mode, and its cardinality behaviour under dynamic allocation ([#42])
- **Installation options reordered** — `--packages` was labelled "recommended" but does not load
  the agent extension: `-Dotel.javaagent.extensions` is read at premain from a fixed path, and
  resolved packages are neither present nor at a nameable path by then. Such deployments silently
  lose per-stage traceparent injection (task spans parent to `spark.application` rather than
  their stage), in-task context restoration, and the Flare resource attributes. Manual JAR
  deployment is now the recommended option and the `--packages` caveats are spelled out ([#42])
- **Driver-side extension attachment documented** — `SparkContext` and `DAGScheduler` are both
  driver-side, so attaching the extension on the driver alone restores per-stage traceparent
  injection and `spark.task → spark.stage` parenting. Executors recover the correct parent from
  task properties through the plugin without needing the extension. Useful where a stable JAR
  path exists on the driver but not cluster-wide ([#42])

## [0.2.0] - 2026-02-27

### Added
- **Stage-level task filtering** — `FLARE_TASK_STAGES` (comma-separated stage IDs) and
  `FLARE_TASK_STAGE_PATTERN` (regex on stage name) scope task spans to specific stages
  instead of sampling uniformly ([#3])
- **Log MDC enrichment** — `trace_id` and `span_id` injected into Log4j 2 `ThreadContext`
  on executor threads for log-to-trace correlation ([#4])
- **Loki + Grafana traces-to-logs** — Loki service with OTLP native ingestion, Alloy log
  routing, bidirectional Tempo/Loki correlation in Grafana datasources ([#5])
- **Stage filter tests** — 25 tests covering `shouldTraceTask()` filter combinations and
  `FlareConfig.load()` parsing ([#6])
- **Executor shutdown flush** — `forceFlush()` on `SdkTracerProvider` during executor
  shutdown to push buffered spans before JVM exits (K8s SIGTERM) ([#7])
- **Task span metrics** — `shuffle.read_bytes`, `shuffle.write_bytes`, `peak_memory_bytes`,
  `input.bytes`, `output.bytes`, `duration_ms`, `sql.execution_id` attributes on executor
  task spans from `TaskMetrics` ([#8])
- **sbt-buildinfo** — version injected at compile time, replacing hand-maintained
  `BuildInfo.scala` ([#9])
- **CI pipeline** — GitHub Actions workflow: compile, test, assembly, Scala 2.12
  cross-compile ([#9])
- **Stable assembly JAR names** — `flare-spark.jar` and `flare-examples.jar` instead of
  version-suffixed names; Docker bind mounts no longer break on version bumps ([#9])
- **Slow task filter** — `FLARE_SLOW_TASK_MS` drops task spans faster than the threshold
- **Retry-only filter** — `FLARE_RETRY_TASKS_ONLY` limits task spans to retried/speculative tasks
- **Max spans circuit breaker** — `FLARE_MAX_SPANS_PER_TRACE` hard limit with warning on breach
- **Tempo metrics generator** — service graphs, span metrics, and local-blocks processors
  for Grafana Traces Drilldown

### Changed
- `FlareExecutorPlugin` captures `TaskContext` at `onTaskStart()` and carries it through to
  `endTask()` — Spark 4.0 clears `TaskContext.get()` before `onTaskSucceeded` fires
- Version bumped to `0.2.0-SNAPSHOT`

### Fixed
- Windows CRLF in `entrypoint.sh` breaking Docker bind mounts — added `.gitattributes`
  with `*.sh text eol=lf`

## [0.1.0] - 2026-02-27

### Added
- **Driver-to-executor distributed tracing** — W3C `traceparent` injected into Spark
  `LocalProperty` on the driver, extracted on executor via `ExecutorPlugin` ([#1])
- `FlareSparkPlugin` with `FlareDriverPlugin` (application span, `TracingSparkListener`,
  traceparent injection) and `FlareExecutorPlugin` (executor-side task spans)
- `TracingSparkListener` — application, job, and stage spans on the driver with correct
  stage-to-job attribution via reverse index
- `LocalPropertyPropagator` — W3C inject/extract using `SparkContext.setLocalProperty()`
- `FlareAutoConfig` — `AutoConfigurationCustomizerProvider` for resource attributes and
  role detection (driver vs executor)
- `FlareConfig` — typed configuration from environment variables with startup validation
- `SparkAttributes` — well-namespaced `AttributeKey` constants for all span attributes
- Docker dev stack: Spark master + 2 workers, Alloy, Tempo, Grafana
- `SkewedJob` example with intentional data skew for visual trace inspection
- 8 unit/integration tests (listener, propagator, config, end-to-end)

[Unreleased]: https://github.com/Neutrinic/flare/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/Neutrinic/flare/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Neutrinic/flare/releases/tag/v0.1.0
[#1]: https://github.com/Neutrinic/flare/issues/1
[#3]: https://github.com/Neutrinic/flare/issues/3
[#4]: https://github.com/Neutrinic/flare/issues/4
[#5]: https://github.com/Neutrinic/flare/issues/5
[#6]: https://github.com/Neutrinic/flare/issues/6
[#7]: https://github.com/Neutrinic/flare/issues/7
[#8]: https://github.com/Neutrinic/flare/issues/8
[#9]: https://github.com/Neutrinic/flare/issues/9
[#14]: https://github.com/Neutrinic/flare/issues/14
[#15]: https://github.com/Neutrinic/flare/issues/15
[#10]: https://github.com/Neutrinic/flare/issues/10
[#11]: https://github.com/Neutrinic/flare/issues/11
[#26]: https://github.com/Neutrinic/flare/issues/26
[#42]: https://github.com/Neutrinic/flare/issues/42
[#44]: https://github.com/Neutrinic/flare/issues/44
[#45]: https://github.com/Neutrinic/flare/issues/45
[#47]: https://github.com/Neutrinic/flare/issues/47
[#52]: https://github.com/Neutrinic/flare/issues/52
[#53]: https://github.com/Neutrinic/flare/issues/53
[#57]: https://github.com/Neutrinic/flare/issues/57
[#58]: https://github.com/Neutrinic/flare/issues/58
