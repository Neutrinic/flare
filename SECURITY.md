# Security policy

## Reporting a vulnerability

Please don't open a public issue for a security problem. Report it privately through
GitHub: on the repository's **Security** tab, choose **Report a vulnerability**, or go
straight to <https://github.com/Neutrinic/flare/security/advisories/new>. Only the
maintainers can see the report.

Include what you can of:

- the versions of Flare, the OpenTelemetry Java agent, Spark and Java, and where it runs
  (Kubernetes, standalone, YARN, Databricks, EMR, Dataproc)
- the agent and Spark configuration, with credentials removed
- what an attacker could do, and the steps that show it

Flare is maintained by volunteers, so there's no guaranteed response time, but reports are
dealt with before other work. You'll hear back on the report itself, and you'll be credited
in the advisory unless you'd rather not be.

## What happens next

A confirmed vulnerability is fixed in a release of each supported version. A GitHub security
advisory is then published with the fix, describing the problem, the versions affected and
how to upgrade, and the changelog lists the fix under **Security**.

## Supported versions

| Version | Supported |
|---|---|
| The latest 1.x minor release | Yes |
| Older 1.x minor releases | No: upgrade to the latest |
| 0.x | No |

## Scope

In scope is anything Flare itself does: the extension jar (`flare-spark-*` on Maven Central
and the jars on GitHub Releases), and the dashboards, alert rules and example configurations
this repository ships. For example:

- credentials or secrets, such as OTLP headers or tokens in Spark configuration, that Flare
  puts into spans, metrics or logs
- telemetry that Flare exports beyond what the [privacy page](docs/privacy.md) documents
- values from a job, such as local properties or job descriptions, changing what Flare does
  beyond describing that job
- the JMX bean Flare registers on the driver (`io.flare.spark:type=DriverSpans`) being usable
  to do more than end that driver's open spans

Out of scope:

- vulnerabilities in Apache Spark, the JVM or the OpenTelemetry Java agent, unless Flare's use
  of them is what makes them exploitable. Report those upstream. Dependencies are scanned in CI,
  and a known vulnerability in something Flare bundles is welcome as an ordinary issue.
- the collector or backend the telemetry is sent to, and who can read it there
- telemetry content the [privacy page](docs/privacy.md) documents, such as SQL plans or
  exception messages quoting the data being processed. Missing controls for it are welcome as
  ordinary issues.
