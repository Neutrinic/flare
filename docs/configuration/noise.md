# Noise

The OpenTelemetry agent instruments everything in the JVM, not only Spark. Every platform makes its
own calls from the driver and executors, and each would become a separate one-span trace next to
Flare's:

| Platform | Source | Without filtering |
|---|---|---|
| Databricks | the platform's HTTP calls into the driver (Jetty) | about 320 traces in a three-minute run |
| Dataproc | metadata server and GCS calls (HttpURLConnection) | 63 to 85 per job |
| Kubernetes | Spark's calls to the API server (OkHttp on Spark 3.5, Vert.x on 4.0) | about 10 per run |
| EMR Serverless | each executor fetching the application JAR from S3 | 3 per executor |

## What Flare does about it

By default Flare drops any span that has **no parent** and is **not one of Flare's** `spark.*` spans,
on the driver and on executors. After this the verified platforms show one or two driver traces per
run.

What is kept:

- **Calls made inside Spark work.** A JDBC query or an S3 read inside a task has the task span as
  parent, so it is kept and nests under the task.
- **Work a task hands to another thread,** through an `ExecutorService` or `CompletableFuture`: the
  agent carries the task's context there, so the child keeps its parent. A raw `new Thread` gets no
  context; its spans have no parent and are dropped. Without this filter they were exported as
  separate traces, never under the task.
- **Requests that continue another trace.** A request arriving with a `traceparent` header has a
  parent, so it is kept. This is why Databricks runs still show one small extra trace: Databricks
  sends its own `traceparent` on some calls into the driver.

## Turning it off

Set `FLARE_DROP_NON_SPARK_ROOTS=false` when a Spark JVM does its own traced work that should start
traces, such as a driver that also serves HTTP.

To keep the filter but also silence an instrumentation entirely, use the agent's switches, for
example on the driver only:

```text
-Dotel.instrumentation.http-url-connection.enabled=false
```

Disabling an instrumentation also hides your own calls through that library, which is why the
filter is the default rather than these switches.

The filter keeps calls made inside Spark work, and Spark's own reads of its input are such calls:
one span per S3 or GCS request, under the task that made it. To keep only Flare's spans, see the
[lean agent](volume.md#traces-use-the-lean-agent).
