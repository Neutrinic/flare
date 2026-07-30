# Contributing to Flare

Thanks for your interest in contributing to Flare! This guide covers everything you need
to build, test, and submit changes.

## Prerequisites

- **JDK 17+** (21 recommended for Spark 4.0 compatibility)
- **sbt 1.11+**
- **Docker** (for the dev observability stack)

## Build

```bash
# Compile
sbt compile

# Run tests
sbt test

# Run the forked OpenTelemetry agent + assembled extension SPI smoke test
sbt -DsparkVersion=3.5.1 ++2.13.16 "AgentTest / test"

# Build assembly JAR (bundles only Flare code + OTEL API)
sbt assembly
# Output: target/scala-2.13/flare-spark.jar

# Cross-compile against Scala 2.12
sbt ++2.12.18 compile
```

### Building against a specific Spark version

The default build targets Spark 3.5.1. Override with:

```bash
sbt -DsparkVersion=4.0.0 compile test assembly
sbt -DsparkVersion=3.4.3 compile test
sbt -DsparkVersion=3.3.4 compile test
```

### Supported matrix

| Spark | Scala 2.12 | Scala 2.13 |
|-------|------------|------------|
| 3.3.4 | compile    | compile    |
| 3.4.3 | compile    | compile    |
| 3.5.1 | compile    | compile + test |
| 4.0.0 | -          | compile + test |

Full test suite runs against Spark 3.5.1 and 4.0.0 (Scala 2.13). Older Spark versions
are compile-checked only.

## Docker dev stack

The Docker stack runs Spark (master + 2 workers), Alloy, Tempo, Loki, and Grafana.
JARs are bind-mounted from the build tree, so rebuild and restart without a Docker rebuild.

```bash
# Build the assembly JARs
sbt assembly

# Start the stack
docker compose -f docker/docker-compose.yml up -d

# Run the example job
docker exec docker-spark-master-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class io.flare.examples.SkewedJob \
  /opt/flare/flare-examples.jar

# View traces: http://localhost:3000 (Grafana, Explore > Tempo)
# View logs:   http://localhost:3000 (Grafana, Explore > Loki)
# Spark UI:    http://localhost:8080

# Tear down (add -v to clear volumes)
docker compose -f docker/docker-compose.yml down
```

## Project structure

```
src/main/scala/io/flare/spark/
  plugin/          FlareSparkPlugin, FlareDriverPlugin, FlareExecutorPlugin
  listener/        TracingSparkListener (driver-side spans)
  propagation/     LocalPropertyPropagator (W3C traceparent inject/extract)
  config/          FlareConfig, FlareAutoConfig
  attributes/      SparkAttributes (AttributeKey constants)
examples/          SkewedJob (example with data skew)
docker/            Docker Compose stack, Spark config, Alloy/Tempo/Loki/Grafana config
```

## Making changes

### Workflow

1. Open an issue describing the work
2. Create a branch: `git checkout -b feature/<issue>-short-description`
3. Make changes, commit with issue reference: `git commit -m "Description (#<issue>)"`
4. Push and open a PR with `Closes #<issue>` as the first line of the body
5. All CI checks must pass before merge

### PR checklist

- [ ] `sbt compile` succeeds (no warnings on new code)
- [ ] `sbt test` passes (all existing + new tests)
- [ ] `sbt assembly` produces a clean JAR
- [ ] `sbt ++2.12.18 compile` cross-compiles
- [ ] No new runtime dependencies added (see below)

### Dependency policy

The assembly JAR must remain minimal. Flare bundles only its own code plus `opentelemetry-api`
and `opentelemetry-context`. Everything else is `provided`:

- **Spark** (`spark-core`, `spark-sql`) — provided by the cluster
- **OTEL SDK** (`opentelemetry-sdk`, autoconfigure SPI) — provided by the Java agent
- **ByteBuddy** — provided by the Java agent
- **SLF4J** — provided by both Spark and the agent
- **Log4j 2** — provided transitively by Spark

Do not add: Cats, Ciris, Refined, JSON libraries, or any other runtime dependency.

### Code style

- Scala 2.13 idioms, cross-compilable with 2.12
- `TrieMap` or `ConcurrentHashMap` for shared state (not `mutable.Map`)
- `AttributeKey` constants in `SparkAttributes` — never inline string keys
- Use `SpanCompat.setLong()` / `SpanCompat.setBool()` for typed OTEL attributes
  (Scala `Long` != `java.lang.Long` in generic context)
- Tests use munit
- Private Spark APIs (`private[spark]`) accessed via test helpers in
  `src/test/scala/org/apache/spark/FlareTestHelpers.scala`

### Configuration

All configuration is via environment variables (or JVM system properties). No `SparkConf`
wrapping. See `FlareConfig.scala` for the full list. New config variables must:

- Have a sensible default
- Be validated at startup (fail fast with `IllegalArgumentException`)
## License

By contributing, you agree that your contributions will be licensed under the project's
[Apache 2.0 License](LICENSE).
