# Privacy

## Flare sends nothing on its own

Flare contains no usage reporting and makes no network calls of its own. Everything it produces
goes through the OpenTelemetry agent to the exporter **you** configure, and nowhere else. There is
nothing to opt out of.

## What your telemetry can contain

Your backend receives what Spark reveals about your jobs. Some of it can be sensitive:

| What | Where | How to limit it |
|---|---|---|
| SQL physical plans, descriptions and call sites | `spark.sql.*` span attributes; the description also as the `sql.description` metric label | `FLARE_SQL_PLAN_MAX_CHARS`, `FLARE_SQL_DETAILS_MAX_CHARS`, `FLARE_SQL_DESCRIPTION_MAX_CHARS` set to `0`. Under the Thrift server or `spark-sql`, the description is the SQL statement itself, literals included |
| Exception messages and stack traces | failed job, stage and task spans | `FLARE_ERROR_MESSAGE_MAX_CHARS` and `FLARE_STACKTRACE_MAX_CHARS` set to `0`. Spark messages can quote the data being processed, in parse errors or type mismatches for example, and a connector's can quote a credential |
| The JVM command line, including the classpath | `process.command_args` and related resource attributes on every span, metric and log record | Set by the agent, not Flare. `-Dotel.java.disabled.resource.providers=io.opentelemetry.instrumentation.resources.ProcessResourceProvider` removes them, on the driver and the executors. It is where a credential passed as a system property ends up |
| Spark's full configuration, logged at start-up | logs, when log export is on | Keep credentials out of Spark config; see [Credentials](configuration/exporting.md#credentials). Or turn log export off |
| Spark and application logs | logs, when log export is on | `-Dotel.logs.exporter=none`, or filter in a collector |
| Database statements, HTTP URLs, storage object keys, message topics | spans from the agent's own instrumentation | Off by default since 1.3.0; only present for the instrumentations you turn back on. See [Agent instrumentation](configuration/volume.md#agent-instrumentation) |

Secure your backend as you would the data the jobs process.

If Flare exports something sensitive that this table does not list, report it privately as a
vulnerability: see the [security policy](https://github.com/Neutrinic/flare/blob/main/SECURITY.md).

## Limiting what Flare sends

Flare can limit the size of a field or stop emitting it. It does not look inside a field for
sensitive values.

| Capability | How | What it guarantees |
|---|---|---|
| Size limiting | A `*_MAX_CHARS` cap above `0` | The field is at most that long. It keeps the first characters, whatever they contain, so a credential at the start of a message survives it. Not a privacy control |
| Field omission | A `*_MAX_CHARS` cap of `0` | Flare does not emit the field at all. The only setting that keeps a value inside the process |
| Content redaction | An OpenTelemetry Collector `transform` or `redaction` processor | Masks sensitive values and keeps the rest, for Flare's telemetry and the agent's alike. It runs after the data has left the process, so it does not meet a requirement that the data never leave the host |

A capped `sql.description` is still one series per distinct statement on the metrics: a cap limits
length, not cardinality. Where descriptions carry changing literals, give queries a stable name with
`setJobDescription`, or set `FLARE_SQL_DESCRIPTION_MAX_CHARS=0`.

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
