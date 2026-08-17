package io.flare.spark.attributes

import munit.FunSuite

class PlanFingerprintTest extends FunSuite {

  /**
   * The case that actually matters. Normalisation buys nothing for `spark-submit` batch — a
   * fresh JVM replays the same expression ids, because Catalyst's `curId` is a JVM-global
   * monotonic counter, so raw hashing already matches there. It is the long-lived JVM (Thrift
   * server, notebook, streaming) where the 2nd and 50th execution of one query get different
   * ids. So the test is: same shape, ids advanced, one fingerprint.
   */
  test("the same query shape fingerprints identically as expression ids advance") {
    val second = """
      |== Physical Plan ==
      |*(2) HashAggregate(keys=[partition_key#12], functions=[count(1)#15L])
      |+- Exchange hashpartitioning(partition_key#12, 200), ENSURE_REQUIREMENTS, [plan_id=41]
      |   +- *(1) Project [partition_key#12, value#13]
      |""".stripMargin

    val fiftieth = """
      |== Physical Plan ==
      |*(2) HashAggregate(keys=[partition_key#8842], functions=[count(1)#8845L])
      |+- Exchange hashpartitioning(partition_key#8842, 200), ENSURE_REQUIREMENTS, [plan_id=2317]
      |   +- *(1) Project [partition_key#8842, value#8843]
      |""".stripMargin

    assertEquals(PlanFingerprint.of(second), PlanFingerprint.of(fiftieth))
    assert(PlanFingerprint.of(second).isDefined)
  }

  test("codegen stage numbering does not change the fingerprint") {
    val a = "*(1) Project [x#1]\n+- Scan [codegen id : 1]"
    val b = "*(1) Project [x#9]\n+- Scan [codegen id : 7]"
    assertEquals(PlanFingerprint.of(a), PlanFingerprint.of(b))
  }

  // The whole value proposition collapses if unrelated queries collide, so assert the
  // normalisation is not so aggressive that it erases the shape itself.
  test("a genuinely different plan fingerprints differently") {
    val agg  = "*(2) HashAggregate(keys=[k#1], functions=[count(1)#2L])"
    val join = "*(2) SortMergeJoin [k#1], [k#2], Inner"
    assertNotEquals(PlanFingerprint.of(agg), PlanFingerprint.of(join))
  }

  test("column names still matter — normalisation strips ids, not identifiers") {
    val byUser    = "*(1) Project [user_id#1]"
    val byAccount = "*(1) Project [account_id#1]"
    assertNotEquals(PlanFingerprint.of(byUser), PlanFingerprint.of(byAccount))
  }

  /**
   * The defect that motivated stripping statistics, taken verbatim from a dev-stack final plan.
   * Once AQE materialises query stages it stamps the observed size and row count into the tree.
   * Those track the DATA, not the query — so without this the same query fingerprints differently
   * on a busy day than a quiet one, which is exactly the grouping the attribute exists to provide.
   */
  test("runtime statistics do not change the fingerprint") {
    val quietDay = """
      |AdaptiveSparkPlan (15)
      |+- == Final Plan ==
      |   ResultQueryStage (11), Statistics(sizeInBytes=8.0 EiB)
      |   +- ShuffleQueryStage (9), Statistics(sizeInBytes=32.0 B, rowCount=2)
      |""".stripMargin

    val busyDay = """
      |AdaptiveSparkPlan (15)
      |+- == Final Plan ==
      |   ResultQueryStage (11), Statistics(sizeInBytes=8.0 EiB)
      |   +- ShuffleQueryStage (9), Statistics(sizeInBytes=904.1 MiB, rowCount=17400229)
      |""".stripMargin

    assertEquals(PlanFingerprint.of(quietDay), PlanFingerprint.of(busyDay))
  }

  test("empty, blank and null plans yield no fingerprint") {
    assertEquals(PlanFingerprint.of(""), None)
    assertEquals(PlanFingerprint.of("   \n  "), None)
    assertEquals(PlanFingerprint.of(null), None)
  }

  test("fingerprint is 16 hex characters") {
    val fp = PlanFingerprint.of("*(1) Project [x#1]").get
    assertEquals(fp.length, 16)
    assert(fp.forall(c => c.isDigit || ('a' to 'f').contains(c)), s"not lowercase hex: $fp")
  }

  test("normalise strips ids but leaves structure intact") {
    val normalised = PlanFingerprint.normalise(
      "HashAggregate(keys=[k#12], functions=[count(1)#15L]) [plan_id=41] [codegen id : 3]"
    )
    assertEquals(
      normalised,
      "HashAggregate(keys=[k#], functions=[count(1)#]) [plan_id=] [codegen id]",
    )
  }

  // Truncating before hashing would make the fingerprint depend on FLARE_SQL_PLAN_MAX_CHARS,
  // so the same query would group differently between two differently-configured deployments.
  test("fingerprint of a full plan differs from its truncated prefix") {
    val full = "*(1) Project [a#1, b#2, c#3]\n+- Scan parquet [a#1, b#2, c#3]"
    assertNotEquals(PlanFingerprint.of(full), PlanFingerprint.of(full.take(20)))
  }
}
