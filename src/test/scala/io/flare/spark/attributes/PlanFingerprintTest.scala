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

  // ── Literals and paths (#207) ─────────────────────────────────────────────

  /**
   * A real Spark 4.0.4 `EXPLAIN FORMATTED` scan and filter, from the lab, on a table partitioned by
   * `dt`, with the run's date, a numeric bound, a string and a decimal as parameters.
   */
  private def ordersPlan(dt: String, minId: String, name: String, maxAmount: String, exprBase: Int = 16): String = {
    val (id, nm, amt, d) = (s"id#${exprBase}L", s"name#${exprBase + 1}", s"amount#${exprBase + 2}", s"dt#${exprBase + 3}")
    s"""
      |== Physical Plan ==
      |AdaptiveSparkPlan (7)
      |+- HashAggregate (6)
      |   +- Exchange (5)
      |      +- HashAggregate (4)
      |         +- Project (3)
      |            +- Filter (2)
      |               +- Scan parquet spark_catalog.default.orders (1)
      |
      |(1) Scan parquet spark_catalog.default.orders
      |Output [4]: [$id, $nm, $amt, $d]
      |Batched: true
      |Location: InMemoryFileIndex [file:/data/wh/orders/dt=$dt]
      |PartitionFilters: [isnotnull($d), ($d = $dt)]
      |PushedFilters: [IsNotNull(id), IsNotNull(name), IsNotNull(amount), GreaterThan(id,$minId), EqualTo(name,$name), LessThan(amount,$maxAmount)]
      |ReadSchema: struct<id:bigint,name:string,amount:decimal(12,2)>
      |
      |(2) Filter
      |Input [4]: [$id, $nm, $amt, $d]
      |Condition : (((((isnotnull($id) AND isnotnull($nm)) AND isnotnull($amt)) AND ($id > $minId)) AND ($nm = $name)) AND ($amt < $maxAmount))
      |""".stripMargin
  }

  test("a daily run on a new date, with new values, fingerprints the same as yesterday's") {
    val yesterday = ordersPlan("2024-01-01", "100", "abc", "9.99")
    val today     = ordersPlan("2024-01-02", "250", "xyz", "19.99", exprBase = 4100)
    assertEquals(PlanFingerprint.of(today), PlanFingerprint.of(yesterday))
  }

  test("filtering a different column, or with a different operator, still fingerprints differently") {
    val base      = ordersPlan("2024-01-01", "100", "abc", "9.99")
    val otherCol  = base.replace("(id#16L > 100)", "(name#17 > 100)").replace("GreaterThan(id,100)", "GreaterThan(name,100)")
    val otherOp   = base.replace("(id#16L > 100)", "(id#16L < 100)").replace("GreaterThan(id,100)", "LessThan(id,100)")
    assertNotEquals(PlanFingerprint.of(otherCol), PlanFingerprint.of(base))
    assertNotEquals(PlanFingerprint.of(otherOp), PlanFingerprint.of(base))
  }

  test("a comparison between two columns, such as a join key, is kept") {
    val byCustomer = "SortMergeJoin [o_custkey#1L], [c_custkey#2L], Inner, ((o_custkey#1L = c_custkey#2L))"
    val byNation   = "SortMergeJoin [o_custkey#1L], [c_custkey#2L], Inner, ((o_custkey#1L = c_nationkey#2L))"
    assertNotEquals(PlanFingerprint.of(byCustomer), PlanFingerprint.of(byNation))
  }

  test("IN lists, pushed IN filters and multi-path locations normalise too") {
    val a = "Condition : id#1L IN (1,2,3)\nPushedFilters: [In(id, [1,2,3])]\nLocation: InMemoryFileIndex(2 paths)[s3://b/t/dt=1, s3://b/t/dt=2]"
    val b = "Condition : id#9L IN (7,8)\nPushedFilters: [In(id, [7,8])]\nLocation: InMemoryFileIndex(1 paths)[s3://b/t/dt=3]"
    assertEquals(PlanFingerprint.of(a), PlanFingerprint.of(b))
  }

  test("a pushed-down filter value with parentheses is one value") {
    val a = "PushedFilters: [IsNotNull(s), EqualTo(s,prefix(A)), GreaterThan(a,1)]"
    val b = "PushedFilters: [IsNotNull(s), EqualTo(s,prefix(B)), GreaterThan(a,9)]"
    assertEquals(PlanFingerprint.of(a), PlanFingerprint.of(b))
  }

  test("an operand that mentions a column is kept, however it is wrapped") {
    val cast  = "Condition : (a#1 = cast(b#2 as int))"
    val other = "Condition : (a#1 = cast(c#3 as int))"
    assertNotEquals(PlanFingerprint.of(cast), PlanFingerprint.of(other))
  }

  /** Spark 4.0.4's `PartitionFilters` for a table partitioned by `a` and `b`, from the lab. */
  private def partitionFilters(op: String) =
    s"PartitionFilters: [isnotnull(b#15), a#14 INSET 1, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9, (b#15 $op 100)]"

  test("in a PartitionFilters list, an INSET's values end where the next filter starts") {
    assertEquals(PlanFingerprint.normalise(partitionFilters(">")),
      "PartitionFilters: [isnotnull(b#), a# IN (?), (b# > ?)]")
    assertNotEquals(PlanFingerprint.of(partitionFilters(">")), PlanFingerprint.of(partitionFilters("<")))
  }

  test("an INSET's values end before a negated or function filter too (Spark 4.0.4 lines)") {
    val notIn = (col: String) =>
      s"PartitionFilters: [a#17 INSET 1, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9, NOT $col#18 INSET 1, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9]"
    assertEquals(PlanFingerprint.normalise(notIn("b")), "PartitionFilters: [a# IN (?), NOT b# IN (?)]")
    assertNotEquals(PlanFingerprint.of(notIn("b")), PlanFingerprint.of(notIn("c")))
    val function = "PartitionFilters: [isnotnull(b#18), a#17 INSET 1, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9, isnotnull(c#19), (b#18 > 1)]"
    assertEquals(PlanFingerprint.normalise(function), "PartitionFilters: [isnotnull(b#), a# IN (?), isnotnull(c#), (b# > ?)]")
  }

  test("an INSET's last value does not survive a predicate after it (Spark's form)") {
    val cond = (values: String) => s"Condition : (a#5 INSET $values AND (b#6 > 100))"
    assertEquals(PlanFingerprint.normalise(cond("1, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9")), "Condition : (a# IN (?) AND (b# > ?))")
    assertEquals(PlanFingerprint.of(cond("1, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9")),
      PlanFingerprint.of(cond("21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31")))
  }

  test("a literal containing # is still a literal: `(s# = tag#east)`") {
    assertEquals(PlanFingerprint.of("Condition : (s#9 = tag#east)"), PlanFingerprint.of("Condition : (s#9 = tag#west)"))
    assertEquals(PlanFingerprint.of("Condition : s#9 IN (tag#east,tag#north)"), PlanFingerprint.of("Condition : s#9 IN (tag#west)"))
  }
}
