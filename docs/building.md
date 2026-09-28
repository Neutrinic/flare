# Building Flare

Requires Java 17 or later and sbt.

```bash
sbt compile
sbt assembly            # target/scala-2.13/flare-spark-3.5.jar
sbt test
sbt -DsparkVersion=3.5.1 ++2.13.16 "AgentTest / test"   # against the real agent
```

Build for another Spark line or Scala version:

```bash
sbt -DsparkVersion=3.3.4 ++2.12.18 assembly   # Spark 3.3, Scala 2.12
sbt -DsparkVersion=4.0.0 ++2.13.16 assembly   # Spark 4.0, Scala 2.13
```

`sbt test` does not run the agent tests; they need an assembled JAR and a separate JVM with the
agent attached, so they run as `AgentTest / test`.

Every push to `main` publishes one JAR per coordinate to the
[`dev` pre-release](https://github.com/Neutrinic/flare/releases/tag/dev), for trying unreleased
changes on a cluster by URL. It is never published to Maven Central.

## Architecture

```text
Agent extension (loaded by -Dotel.javaagent.extensions)
├── SparkContextInstrumentation        hooks SparkContext start-up
├── SubmitMissingTasksInstrumentation  hooks DAGScheduler.submitMissingTasks
│   └── SubmitMissingTasksAdviceHelper creates job and stage spans, injects traceparent
├── TaskRunnerInstrumentation          hooks Executor$TaskRunner.run
│   └── TaskRunnerAdviceHelper         extracts traceparent, restores context
└── FlareAutoConfig                    resource attributes, root sampler, shutdown ordering

Spark plugin (loaded by spark.plugins)
├── FlareDriverPlugin                  application span, TracingSparkListener
└── FlareExecutorPlugin                task spans and task metrics on each executor
```

How context crosses from driver to executor:

```text
Driver:   DAGScheduler.submitMissingTasks(stage, jobId)
            → stage span created
            → traceparent injected into the job's properties
            → properties serialised into each TaskDescription and sent to the executor
Executor: task start
            → traceparent read from the task's properties
            → context restored, task span opened with the executor's own timing
```
