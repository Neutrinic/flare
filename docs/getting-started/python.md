# Python applications

Flare instruments the JVMs, so a PySpark application gets the same telemetry as a Scala one wherever
the work runs in the JVM, which is most of it.

## What works as in Scala

- **DataFrame, SQL and notebook code.** A PySpark driver sends query plans to the driver JVM, and
  the queries run in the executor JVMs. Application, SQL, job, stage and task spans, and every
  metric, are the same as for a Scala application running the same queries. The
  [overhead](../reference/overhead.md) figures apply unchanged: the benchmark workloads are PySpark.
- **Spark's own logs** from the driver and executor JVMs, with the trace and span id of the task that
  wrote them.

## What is not captured on its own

Python user-defined functions (`udf`, `pandas_udf`), `mapInPandas`, `applyInPandas` and RDD
lambdas run in Python worker processes that the executor JVM starts. The agent cannot see into
them:

- **Timing is still right.** The task span covers the whole task, including the time spent in
  Python, so a slow UDF shows as a slow task.
- **No spans or logs from inside Python.** Python `logging` output and anything printed goes to the
  worker's standard error, not to OTLP.
- **Python driver code that is not Spark,** such as HTTP calls or `boto3`, is not traced by the Java
  agent either.

## Adding your own spans from Python

Flare puts the trace context of each task's stage into the task's local properties as
`traceparent`, and Python can read it. A span started with it as parent joins the application's
trace, under the stage span and beside that stage's task spans.

Install the OpenTelemetry SDK and OTLP exporter into the Python environment Spark uses, the same on
every node:

```bash
pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http
```

Inside the Python code that runs on executors:

```python
from pyspark import TaskContext
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor


def traced(rows):
    # Once per Python worker process.
    if not isinstance(trace.get_tracer_provider(), TracerProvider):
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)

    parent = extract({"traceparent": TaskContext.get().getLocalProperty("traceparent") or ""})
    with trace.get_tracer("my-app").start_as_current_span("parse", context=parent):
        for row in rows:
            yield row


result = df.rdd.mapPartitions(traced)
```

The same `TaskContext.get().getLocalProperty("traceparent")` works inside a `udf` or `pandas_udf`.

Tell the Python workers where to send spans and what to call them, in the executors' environment:

```text
--conf spark.executorEnv.OTEL_EXPORTER_OTLP_ENDPOINT=http://your-collector:4318
--conf spark.executorEnv.OTEL_SERVICE_NAME=my-app-python
```

- **Sampling follows the application.** The `traceparent` carries the driver's sampling decision,
  and the SDK's default sampler respects it, so an unsampled application produces no Python spans
  either.
- **Use `SimpleSpanProcessor`, not `BatchSpanProcessor`.** Spark stops its Python workers without
  waiting for background exports, and `BatchSpanProcessor` sends on a timer: in a lab run with it,
  none of the Python spans arrived. `SimpleSpanProcessor` exports each span as it ends.
- **Keep the spans coarse,** one per partition or per batch rather than per row, since each one is
  exported as it ends.

## Collecting Python logs

Python logging and standard error from the workers are not exported over OTLP. Collect them with
your platform's log collection, such as YARN log aggregation or the Kubernetes node's log agent, or
an OpenTelemetry Collector `filelog` receiver. Those lines carry no trace id unless your code adds
it.

Tracing inside Python workers without code changes is tracked in
[#143](https://github.com/Neutrinic/flare/issues/143).
