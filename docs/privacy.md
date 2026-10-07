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
| Database statements, HTTP URLs, storage object keys, message topics | spans from the agent's own instrumentation | Off by default since 1.3.0; only present for the instrumentations you turn back on. See [Agent instrumentation](configuration/volume.md#agent-instrumentation) |

Secure your backend as you would the data the jobs process.

If Flare exports something sensitive that this table does not list, report it privately as a
vulnerability: see the [security policy](https://github.com/Neutrinic/flare/blob/main/SECURITY.md).

## This site counts page views

Each page of this site loads a one-pixel image from [Scarf](https://about.scarf.sh/), which
counts page views for the maintainers. It sets no cookies. Scarf uses the request's IP address to
look up the organisation it belongs to, then discards it, and uses the referrer to tell which page
was viewed.

## Download statistics

Downloads from Maven Central are counted from Maven Central's own download data, which Sonatype
shares with [Scarf](https://about.scarf.sh/) for maintainers. That is data about requests to
Maven Central, not something Flare collects: the JAR does nothing at download or at run time to
report it.
