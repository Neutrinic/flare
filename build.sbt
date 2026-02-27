import Dependencies._

ThisBuild / organization := "io.github.neutrinic"
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

ThisBuild / scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
  case Some((2, 12)) => Seq("-Ypartial-unification", "-Ywarn-unused:imports")
  case Some((2, 13)) => Seq("-Ywarn-unused:imports")
  case _             => Seq.empty
})

// Spark version to build against (override with -DsparkVersion=3.5.1)
val sparkBuildVersion = sys.props.getOrElse("sparkVersion", spark35)

lazy val root = (project in file("."))
  .aggregate(examples)
  .settings(
    name := "flare-spark",
    libraryDependencies ++= otelBundled ++ otelProvided ++ byteBuddyCompileOnly ++ Seq(
      "org.apache.spark" %% "spark-core" % sparkBuildVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkBuildVersion % "provided",
      "org.slf4j"         % "slf4j-api"  % slf4jVersion      % "provided",
    ) ++ testDeps(sparkBuildVersion),

    // SPI files — do not let assembly deduplicate these
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case _                                    => MergeStrategy.first
    },

    // Bundle OTEL API + context (needed on Spark's app classloader for SparkPlugin).
    // Everything else (Spark, SLF4J, ByteBuddy, OTEL SDK) is provided.
    assembly / assemblyOption := (assembly / assemblyOption).value
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
