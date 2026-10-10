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
}
