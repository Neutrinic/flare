import sbt._

object Dependencies {

  val scala212 = "2.12.18"
  val scala213 = "2.13.16"

  val spark33 = "3.3.4"
  val spark34 = "3.4.3"
  val spark35 = "3.5.1"
  val spark40 = "4.0.0"

  val otelVersion      = "1.50.0"
  val otelAgentVersion = "2.16.0"
  val byteBuddyVersion = "1.14.19"
  val slf4jVersion     = "2.0.16"
  val munitVersion     = "1.1.1"

  // OTEL API + context — BUNDLED in the assembly JAR.
  // Phase 1 uses Spark's PluginContainer which loads classes from the application
  // classloader. The OTEL agent's extension classloader is separate, so the API
  // must be on the app classpath for FlareSparkPlugin to resolve OTEL types.
  // Phase 2 (InstrumentationModule via ByteBuddy) will move these back to provided.
  val otelBundled = Seq(
    "io.opentelemetry" % "opentelemetry-api"     % otelVersion,
    "io.opentelemetry" % "opentelemetry-context"  % otelVersion,
  )

  // OTEL SDK + autoconfigure — provided by the agent, not bundled
  val otelProvided = Seq(
    "io.opentelemetry" % "opentelemetry-sdk"                             % otelVersion % "provided",
    "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure-spi" % otelVersion % "provided",
  )

  // OTEL Java agent extension API — provides InstrumentationModule, TypeInstrumentation.
  // Versioned with the agent (not SDK) and published with -alpha suffix.
  val otelExtensionApi = Seq(
    "io.opentelemetry.javaagent" % "opentelemetry-javaagent-extension-api" % (otelAgentVersion + "-alpha") % "provided",
  )

  // Provided by the OTEL agent at runtime
  val byteBuddyCompileOnly = Seq(
    "net.bytebuddy" % "byte-buddy"       % byteBuddyVersion % "provided",
    "net.bytebuddy" % "byte-buddy-agent" % byteBuddyVersion % "provided",
  )

  def testDeps(sparkVersion: String) = Seq(
    "org.scalameta"    %% "munit"                     % munitVersion % Test,
    "io.opentelemetry"  % "opentelemetry-sdk-testing" % otelVersion  % Test,
    "org.apache.spark" %% "spark-core"                % sparkVersion % Test,
    "org.apache.spark" %% "spark-sql"                 % sparkVersion % Test,
  )
}
