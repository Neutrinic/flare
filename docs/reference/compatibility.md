# Compatibility

## Spark, Scala and Java

| Spark | Scala 2.12 | Scala 2.13 | Java |
|---|:---:|:---:|---|
| 3.3 | ✓ | ✓ | 8 and later |
| 3.4 | ✓ | ✓ | 8 and later |
| 3.5 | ✓ | ✓ | 8 and later |
| 4.0 | | ✓ | 17 and later |

The Spark 4.0 artifact has also been run on Spark 4.1.2. Runs covered Java 8, 11, 17 and 21.

## OpenTelemetry agent

Flare is an agent **extension**, not a library. The agent loads it into its own classloader,
where it shares the agent's SDK and ByteBuddy, so the agent version matters.

| Component | Version |
|---|---|
| OpenTelemetry Java agent | 2.31.1, built and tested against |
| OpenTelemetry API and SDK | 1.65.0, matching the agent |
| ByteBuddy | 1.18.12, matching the agent |

- **Same agent version:** recommended. The extension API is published with an `-alpha` suffix and
  may change between minor releases.
- **Newer agent:** may work if the extension API has not changed. Test before deploying.
- **Older agent:** likely to fail. Flare uses extension API types that have changed across agent
  releases.

A mismatch usually shows up at Spark start-up as a `NoSuchMethodError` or `ClassNotFoundException`
in Flare's instrumentation.

Check an agent's version:

```bash
unzip -p opentelemetry-javaagent.jar META-INF/MANIFEST.MF | grep Implementation-Version
```

## Why the agent's dependencies are not bundled

Flare shares classes with the agent at runtime. Bundling its own copies of the SDK or ByteBuddy would
put two versions of the same classes side by side and fail with `LinkageError` or
`ClassCastException`. The one exception is the OpenTelemetry **API**, which the Flare JAR does
bundle, unshaded: `spark.plugins` loads Flare into Spark's classloader, where neither Spark nor the
agent provides an API, and the agent's bridge recognises the API by its real package names.
