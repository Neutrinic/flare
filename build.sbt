import Dependencies._

ThisBuild / organization := "io.github.neutrinic"
ThisBuild / scalaVersion := scala213
ThisBuild / crossScalaVersions := Seq(scala212, scala213)
ThisBuild / versionScheme := Some("semver-spec")

lazy val AgentTest = config("agentTest") extend Test

// Maven Central publishing metadata
ThisBuild / homepage := Some(url("https://github.com/Neutrinic/flare"))
ThisBuild / licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer("neutrinic", "Neutrinic", "neutrinic@users.noreply.github.com", url("https://github.com/Neutrinic"))
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/Neutrinic/flare"),
    "scm:git@github.com:Neutrinic/flare.git"
  )
)

ThisBuild / scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
)

// NOTE: version-dependent scalacOptions are in root project settings, not ThisBuild,
// because `++2.12.18` changes ThisBuild/scalaVersion which would leak -Ypartial-unification
// to the examples sub-project (which stays on 2.13 and rejects that flag).

// ── OWASP dependency check ────────────────────────────────────────────────
import net.nmoncho.sbt.dependencycheck.settings._
dependencyCheckFailBuildOnCVSS := 7  // fail on high + critical
// Only scan compile + runtime scope — skip provided (Spark, OTEL agent, ByteBuddy)
// and test deps since Flare doesn't ship them.
dependencyCheckScopes := ScopesSettings(
  compile  = true,
  optional = false,
  provided = false,
  runtime  = true,
  test     = false
)
dependencyCheckOutputDirectory := target.value / "dependency-check"
dependencyCheckFormats := {
  import org.owasp.dependencycheck.reporting.ReportGenerator.Format
  Seq(Format.HTML, Format.JSON)
}
dependencyCheckNvdApi := {
  val key = sys.env.getOrElse("NVD_API_KEY", "")
  if (key.nonEmpty) NvdApiSettings(apiKey = key, requestDelay = Some(java.time.Duration.ofSeconds(4)))
  else NvdApiSettings()
}

// Spark version to build against (override with -DsparkVersion=3.5.1)
val sparkBuildVersion = sys.props.getOrElse("sparkVersion", spark35)
val sparkMajorMinor   = sparkBuildVersion.split('.').take(2).mkString(".")

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .aggregate(examples)
  .configs(AgentTest)
  .settings(inConfig(AgentTest)(Defaults.testSettings))
  .settings(
    name := s"flare-spark-$sparkMajorMinor",

    // Spark 4.0 dropped Scala 2.12
    crossScalaVersions := (if (sparkMajorMinor == "4.0") Seq(scala213) else Seq(scala212, scala213)),

    scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => Seq("-Ypartial-unification", "-Ywarn-unused:imports")
      case Some((2, 13)) => Seq("-Ywarn-unused:imports")
      case _             => Seq.empty
    }),

    // sbt-buildinfo — generates io.flare.spark.BuildInfo with version at compile time
    buildInfoKeys    := Seq[BuildInfoKey](name, version, "sparkVersion" -> sparkBuildVersion),
    buildInfoPackage := "io.flare.spark",
    buildInfoObject  := "BuildInfo",

    // The agent extension classloader has no Scala runtime, so the Java
    // AutoConfigurationCustomizerProvider cannot reference BuildInfo directly.
    // Generate the same version as a classpath resource for that provider.
    Compile / resourceGenerators += Def.task {
      val output =
        (Compile / resourceManaged).value / "io" / "flare" / "spark" / "flare-build.properties"
      IO.createDirectory(output.getParentFile)
      IO.write(output, s"flare.version=${version.value}\n")
      Seq(output)
    }.taskValue,

    libraryDependencies ++= otelBundled ++ otelProvided ++ otelExtensionApi ++ byteBuddyCompileOnly ++ Seq(
      "org.apache.spark" %% "spark-core" % sparkBuildVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkBuildVersion % "provided",
      "org.slf4j"                % "slf4j-api"  % slf4jVersion % "provided",
      // log4j-api is provided transitively by spark-core — no explicit dep needed.
      // MdcEnricher references ThreadContext at compile time; Spark's log4j-api satisfies it.
    ) ++ testDeps(sparkBuildVersion) ++ Seq(
      "io.opentelemetry.javaagent" % "opentelemetry-javaagent" % otelAgentVersion % AgentTest,
    ),

    // SPI files — do not let assembly deduplicate these
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case _                                    => MergeStrategy.first
    },

    // Scaladoc crashes on OTEL/Spark types — publish empty javadoc JAR
    Compile / doc / sources := Seq.empty,

    // Bundle OTEL API + context (needed on Spark's app classloader for SparkPlugin).
    // Everything else (Spark, SLF4J, ByteBuddy, OTEL SDK) is provided.
    assembly / assemblyJarName := s"flare-spark-$sparkMajorMinor.jar",
    assembly / assemblyOption  := (assembly / assemblyOption).value
      .withIncludeScala(false),

    Test / fork := true,
    Test / javaOptions ++= Seq(
      s"-Dspark.version=$sparkBuildVersion",
      s"-Dflare.test.expected.version=${version.value}",
    ),

    testFrameworks := Seq(new TestFramework("munit.Framework")),

    // A separate fork is required: the OTEL agent installs GlobalOpenTelemetry at
    // JVM startup, while the regular unit tests install isolated in-memory SDKs.
    AgentTest / fork := true,
    AgentTest / parallelExecution := false,
    AgentTest / testFrameworks := Seq(new TestFramework("munit.Framework")),
    AgentTest / javaOptions ++= {
      val agentJar = (AgentTest / dependencyClasspath).value
        .map(_.data)
        .find(_.getName == s"opentelemetry-javaagent-$otelAgentVersion.jar")
        .getOrElse(sys.error(s"Could not resolve OpenTelemetry Java agent $otelAgentVersion"))
      val extensionJar = (Compile / assembly).value
      val collectorSocket =
        new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress)
      val collectorPort = try collectorSocket.getLocalPort finally collectorSocket.close()

      Seq(
        s"-javaagent:${agentJar.getAbsolutePath}",
        s"-Dotel.javaagent.extensions=${extensionJar.getAbsolutePath}",
        "-Dotel.traces.exporter=otlp",
        "-Dotel.exporter.otlp.protocol=http/protobuf",
        s"-Dotel.exporter.otlp.traces.endpoint=http://127.0.0.1:$collectorPort/v1/traces",
        "-Dotel.exporter.otlp.traces.compression=none",
        "-Dotel.bsp.schedule.delay=100",
        "-Dotel.instrumentation.java-http-server.enabled=false",
        "-Dotel.metrics.exporter=none",
        "-Dotel.logs.exporter=none",
        "-Dotel.traces.sampler=always_on",
        "-Dotel.service.name=flare-agent-test",
        s"-Dflare.agent.test.collector.port=$collectorPort",
        s"-Dflare.agent.test.expected.version=${version.value}",
        s"-Dflare.agent.test.extension.jar=${extensionJar.getAbsolutePath}",
        "--add-modules=jdk.httpserver",
      )
    },
  )

lazy val examples = (project in file("examples"))
  .settings(
    name := "flare-examples",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkBuildVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkBuildVersion % "provided",
    ),

    assembly / assemblyJarName := "flare-examples.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _                        => MergeStrategy.first
    },

    assembly / assemblyOption := (assembly / assemblyOption).value
      .withIncludeScala(false),

    // No cross-compilation for examples — Scala 2.13 only
    crossScalaVersions := Seq(scala213),

    // No tests in examples
    Test / test := {},
  )
