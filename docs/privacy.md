# Privacy

## Flare sends nothing on its own

Flare contains no usage reporting and makes no network calls of its own. Everything it produces
goes through the OpenTelemetry agent to the exporter **you** configure, and nowhere else. There is
nothing to opt out of.

## What your telemetry can contain

Your backend receives what Spark reveals about your jobs. Some of it can be sensitive:

| What | Where | How to limit it |
|---|---|---|
| SQL physical plans, descriptions and call sites | `spark.sql.*` span attributes | `FLARE_SQL_PLAN_MAX_CHARS`, `FLARE_SQL_DETAILS_MAX_CHARS`, `FLARE_SQL_DESCRIPTION_MAX_CHARS`; `0` drops the attribute. See [Configuration](configuration/index.md) |
| Exception messages and stack traces | failed job, stage and task spans | Spark messages can quote the data being processed, in parse errors or type mismatches for example |
| The JVM command line, including the classpath | resource attributes on every span, metric and log record | The agent's resource detectors; see the [agent configuration](https://opentelemetry.io/docs/zero-code/java/agent/configuration/) |
| Spark's full configuration, logged at start-up | logs, when log export is on | Keep credentials out of Spark config; see [Credentials](configuration/exporting.md#credentials). Or turn log export off |
| Spark and application logs | logs, when log export is on | `-Dotel.logs.exporter=none`, or filter in a collector |

Secure your backend as you would the data the jobs process.

## Download statistics

Downloads from Maven Central are counted from Maven Central's own download data, which Sonatype
shares with [Scarf](https://about.scarf.sh/) for maintainers. That is data about requests to
Maven Central, not something Flare collects: the JAR does nothing at download or at run time to
report it.
