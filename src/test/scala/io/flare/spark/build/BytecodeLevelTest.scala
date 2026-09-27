package io.flare.spark.build

import munit.FunSuite

import java.io.DataInputStream
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters._

/**
 * Pins every compiled Flare class to the oldest Java its Spark line runs on (#120).
 *
 * javac targets the JDK that runs the build unless told otherwise, so without `--release` the Java
 * sources came out as Java 17. Those sources are the agent-extension half — `FlareAutoConfig` and the
 * ByteBuddy instrumentation — and on Java 8 or 11 the agent cannot load them. That does not degrade
 * Flare, it disables the agent's SDK outright: nothing is exported from that JVM at all, not even the
 * agent's own instrumentation, and the Spark job still succeeds.
 *
 * CI runs Java 17, where the same classes load fine, so this failure cannot surface as a runtime
 * test here. Checking the class file versions directly catches it regardless of the build's JDK, and
 * scanning every class rather than a fixed list means a future Java source cannot slip past.
 */
class BytecodeLevelTest extends FunSuite {

  private val sparkVersion = sys.props.getOrElse("spark.version", fail("spark.version is not set"))

  /** Spark 3.x runs on Java 8+; Spark 4.x requires 17. */
  private val maxMajor = if (sparkVersion.startsWith("3.")) 52 else 61

  private val javaRelease = Map(52 -> "8", 55 -> "11", 61 -> "17", 65 -> "21")

  /** Directory holding Flare's compiled main classes, located through a Java-compiled class. */
  private def classesRoot: Path = {
    val location = classOf[io.flare.spark.config.FlareAutoConfig]
      .getProtectionDomain.getCodeSource.getLocation.toURI
    val root = Paths.get(location)
    assert(Files.isDirectory(root), s"expected a classes directory, found $root")
    root
  }

  private def majorVersion(classFile: Path): Int = {
    val in = new DataInputStream(Files.newInputStream(classFile))
    try {
      assertEquals(in.readInt(), 0xCAFEBABE, s"$classFile is not a class file")
      in.readUnsignedShort() // minor
      in.readUnsignedShort()
    } finally in.close()
  }

  test(s"every Flare class is compiled for Java ${javaRelease(maxMajor)} or older") {
    val root = classesRoot
    val classes = Files.walk(root.resolve("io/flare")).iterator().asScala
      .filter(_.toString.endsWith(".class"))
      .toList

    assert(classes.nonEmpty, s"no classes found under $root")
    // The agent-extension classes must be among those scanned, or the check proves nothing.
    assert(
      classes.exists(_.getFileName.toString == "FlareAutoConfig.class"),
      "FlareAutoConfig.class was not scanned",
    )

    val tooNew = classes
      .map(c => root.relativize(c).toString.replace('\\', '/') -> majorVersion(c))
      .filter { case (_, major) => major > maxMajor }
      .sorted

    assertEquals(
      tooNew.map { case (c, m) => s"$c = Java ${javaRelease.getOrElse(m, m.toString)}" },
      Nil,
      s"Spark $sparkVersion runs on Java ${javaRelease(maxMajor)}, and these classes would fail to " +
        "load there. On an older JVM the agent then exports nothing at all (#120).",
    )
  }
}
