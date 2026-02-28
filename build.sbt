import Dependencies._

ThisBuild / organization := "io.github.neutrinic"
ThisBuild / version      := "0.2.0-SNAPSHOT"
ThisBuild / scalaVersion := scala213
ThisBuild / crossScalaVersions := Seq(scala212, scala213)
ThisBuild / versionScheme := Some("semver-spec")

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
dependencyCheckSuppressions := SuppressionSettings(
  files = SuppressionFilesSettings.files()(file("dependency-check-suppression.xml"))
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

    libraryDependencies ++= otelBundled ++ otelProvided ++ otelExtensionApi ++ byteBuddyCompileOnly ++ Seq(
      "org.apache.spark" %% "spark-core" % sparkBuildVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkBuildVersion % "provided",
      "org.slf4j"                % "slf4j-api"  % slf4jVersion % "provided",
      // log4j-api is provided transitively by spark-core — no explicit dep needed.
      // MdcEnricher references ThreadContext at compile time; Spark's log4j-api satisfies it.
    ) ++ testDeps(sparkBuildVersion),

    // SPI files — do not let assembly deduplicate these
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case _                                    => MergeStrategy.first
    },

    // Bundle OTEL API + context (needed on Spark's app classloader for SparkPlugin).
    // Everything else (Spark, SLF4J, ByteBuddy, OTEL SDK) is provided.
    assembly / assemblyJarName := s"flare-spark-$sparkMajorMinor.jar",
    assembly / assemblyOption  := (assembly / assemblyOption).value
      .withIncludeScala(false),

    Test / fork := true,
    Test / javaOptions ++= Seq(
      s"-Dspark.version=$sparkBuildVersion",
    ),

    testFrameworks := Seq(new TestFramework("munit.Framework")),
  )

lazy val examples = (project in file("examples"))
  .settings(
    name := "flare-examples",
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
