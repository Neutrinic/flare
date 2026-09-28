# Platforms that cannot run Flare

Flare needs the OpenTelemetry agent attached to the JVM with `-javaagent` at start-up. That needs a
file on the machine before the JVM starts and a way to pass JVM options. These platforms offer
neither, so Flare cannot run there:

| Platform | Why |
|---|---|
| Databricks serverless compute, including Free Edition | No init scripts, no JVM options |
| AWS Glue | No init scripts or custom images |
| Azure Synapse Spark pools | No init scripts or custom images |
| Microsoft Fabric Spark | No init scripts or custom images |

These come from each platform's documented options, not from runs. If one of them gains init
scripts or custom images, it becomes possible.

Serverless platforms that accept a **custom image** can run Flare:
[EMR Serverless](emr-serverless.md) and [Dataproc Serverless](dataproc-serverless.md).
