# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
