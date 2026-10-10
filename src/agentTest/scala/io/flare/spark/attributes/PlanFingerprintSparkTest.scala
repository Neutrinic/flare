package io.flare.spark.attributes

import io.flare.spark.trace.StubCollector
import munit.FunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.types._

/**
 * Plan fingerprints against plans real Spark prints (#207), for the forms only Spark's optimizer
 * produces. From Codex's review of #231. Here rather than in the unit tests because a SparkSession
 * needs the agent-test JVM's module options.
 */
class PlanFingerprintSparkTest extends FunSuite {

  private var spark: SparkSession = _
  private var input: DataFrame = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[1]").appName("flare-plan-fingerprint")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
    val schema = StructType(Seq("a", "b", "c", "d").map(StructField(_, IntegerType, nullable = false)) :+
      StructField("s", StringType, nullable = false))
    input = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1, 2, 3, 4, "value")), 1), schema)
  }

  override def afterAll(): Unit = StubCollector.during { if (spark != null) spark.stop() }

  private def plan(condition: String): String =
    input.filter(condition).queryExecution.explainString(ExplainMode.fromString("formatted"))

  test("IN over different columns keeps different fingerprints: `a IN (b, c)`") {
    assertNotEquals(PlanFingerprint.of(plan("a IN (b, c)")), PlanFingerprint.of(plan("a IN (b, d)")))
  }

  test("a literal IN list Spark rewrites to INSET ignores its values") {
    assertEquals(PlanFingerprint.of(plan("a IN (1,2,3,4,5,6,7,8,9,10,11)")),
      PlanFingerprint.of(plan("a IN (21,22,23,24,25,26,27,28,29,30,31)")))
  }

  test("a string value printed with parentheses is one literal: `s = 'prefix(A)'`") {
    assertEquals(PlanFingerprint.of(plan("s = 'prefix(A)'")), PlanFingerprint.of(plan("s = 'prefix(B)'")))
  }

  test("a literal IN list of a different length is the same query") {
    assertEquals(PlanFingerprint.of(plan("a IN (1, 2)")), PlanFingerprint.of(plan("a IN (5, 6, 7)")))
  }

  private val eleven = "1,2,3,4,5,6,7,8,9,10,11"

  test("a predicate after an INSET still counts: `... AND b > 100` against `... AND c > 100`") {
    assertNotEquals(PlanFingerprint.of(plan(s"a IN ($eleven) AND b > 100")),
      PlanFingerprint.of(plan(s"a IN ($eleven) AND c > 100")))
    assertNotEquals(PlanFingerprint.of(plan(s"a IN ($eleven) OR b > 100")),
      PlanFingerprint.of(plan(s"a IN ($eleven) OR c > 100")))
  }

  test("an INSET is the same query as an IN: two values against eleven") {
    assertEquals(PlanFingerprint.of(plan("a IN (1, 2)")), PlanFingerprint.of(plan(s"a IN ($eleven)")))
  }

  /**
   * A Parquet table partitioned by `a`, `b` and `c`, so filters on them are `PartitionFilters`.
   * Planning lists files but reads none, so an empty file in `a=1/b=200/c=300` and an explicit schema
   * are enough,
   * and nothing goes through Hadoop's file writer (which needs winutils on Windows).
   */
  private lazy val partitioned: DataFrame = {
    val dir = java.nio.file.Files.createTempDirectory("flare-fp-partitioned")
    val leaf = java.nio.file.Files.createDirectories(dir.resolve("a=1").resolve("b=200").resolve("c=300"))
    java.nio.file.Files.createFile(leaf.resolve("part-00000.parquet"))
    spark.read.schema("id LONG").parquet(dir.toString)
  }

  private def partitionPlan(condition: String): String =
    partitioned.filter(condition).queryExecution.explainString(ExplainMode.fromString("formatted"))

  test("in PartitionFilters, a filter listed after an INSET still counts") {
    // Listing files through Hadoop needs its native Windows libraries; CI runs on Linux, and the unit
    // test covers the same line taken from real Spark.
    assume(!sys.props.getOrElse("os.name", "").toLowerCase.contains("windows"), "Hadoop file listing needs winutils on Windows")
    val gt = partitionPlan(s"a IN ($eleven) AND b > 100")
    assert(gt.contains("PartitionFilters:") && gt.contains("INSET"), s"not the plan this test is about:\n$gt")
    assertNotEquals(PlanFingerprint.of(gt), PlanFingerprint.of(partitionPlan(s"a IN ($eleven) AND b < 100")))
  }

  test("in PartitionFilters, a NOT IN listed after an INSET still counts") {
    assume(!sys.props.getOrElse("os.name", "").toLowerCase.contains("windows"), "Hadoop file listing needs winutils on Windows")
    assertNotEquals(PlanFingerprint.of(partitionPlan(s"a IN ($eleven) AND b NOT IN ($eleven)")),
      PlanFingerprint.of(partitionPlan(s"a IN ($eleven) AND c NOT IN ($eleven)")))
  }
}
