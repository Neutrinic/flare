package io.flare.spark.attributes

import java.security.MessageDigest

/**
 * A stable hash of a physical plan's *shape*, for grouping the same query across executions and
 * across applications — something the Spark UI structurally cannot do.
 *
 * ==Why normalisation is needed==
 *
 * Not for the reason it first appears. Comparing plans across two separate `spark-submit` runs of
 * the same job returns byte-identical strings, so raw hashing would work there. Expression ids
 * are not random:
 *
 * {{{
 * object NamedExpression {
 *   private val curId = new java.util.concurrent.atomic.AtomicLong()
 *   def newExprId: ExprId = ExprId(curId.getAndIncrement(), jvmId)
 * }
 * }}}
 *
 * `curId` is a JVM-global monotonic counter, so a fresh JVM running an identical query sequence
 * produces identical ids. That is exactly the batch case.
 *
 * The problem is the long-lived JVM — Thrift server, notebook, structured streaming — where the
 * same query shape gets different ids on its 2nd execution than its 50th. Raw hashing there
 * fragments one query into unbounded distinct fingerprints, which is worse than useless.
 * So normalisation buys nothing for batch and is essential for sessions; it must be tested
 * against the latter.
 *
 * ==Why a fingerprint rather than a bigger plan cap==
 *
 * Tempo's binding limit is per *trace*, not per span (`max_bytes_per_trace`, 5 MB on the pinned
 * 2.6.1). A 60-node plan with wide schemas runs 100 kB+, so roughly 50 SQL executions in one
 * application overflow it and Tempo drops the excess — trivially reachable in a Thrift server.
 * Raising `FLARE_SQL_PLAN_MAX_CHARS` makes that worse. A fingerprint gives the grouping at 16
 * bytes with no plan text at all, which is why it is emitted independently of that cap.
 */
object PlanFingerprint {

  /**
   * Catalyst expression ids: `#3`, `#133`, `#522L`. The `L` suffix marks a long-typed attribute
   * reference and varies with the id, not the shape.
   */
  private val ExprIdPattern = """#\d+L?""".r

  /** AQE / exchange reuse identifiers: `plan_id=142`. */
  private val PlanIdPattern = """plan_id=\d+""".r

  /** Whole-stage codegen stage numbering: `[codegen id : 1]`. */
  private val CodegenIdPattern = """\[codegen id : \d+\]""".r

  /**
   * AQE runtime statistics: `Statistics(sizeInBytes=32.0 B, rowCount=2)`.
   *
   * This one is not cosmetic. Once AQE materialises query stages, the final plan carries the
   * observed size and row count of each stage — values that track the DATA, not the query. Left
   * in, the same query fingerprints differently on a busy day than a quiet one, which defeats the
   * entire point of grouping across executions. Observed in the dev stack:
   * `ResultQueryStage (11), Statistics(sizeInBytes=8.0 EiB)`.
   */
  private val StatisticsPattern = """Statistics\([^)]*\)""".r

  /**
   * Where a scan read from: `Location: InMemoryFileIndex [file:/data/orders/dt=2024-01-01]`, or
   * `InMemoryFileIndex(1 paths)[...]`. The paths change with every run of a date-partitioned job,
   * so only the index's class is kept (#207).
   */
  private val LocationPattern = """Location: (\w+)(?:\(\d+ paths\))? ?\[[^\]]*\]""".r

  /**
   * The start of a comparison with a column, in Catalyst's form, as in `Condition :` and
   * `PartitionFilters:` (after expression ids are stripped): `(dt# = 2024-01-01)`, `(id# > 100)`,
   * `(name# = abc)`. The operand after it is a literal unless it starts with a column reference, as a
   * join key does; it runs to the comparison's closing parenthesis, so a string value printed with
   * parentheses, `(s# = prefix(A))`, is one operand (#207). The space before the operand is matched
   * possessively, or the engine backtracks over it and the column check sees a space.
   */
  private val ColumnComparison = """[\w.]+#\s*(?:<=>|<=|>=|!=|=|<|>)\s*+""".r

  /** `a# IN (b#,1,2)` in Catalyst's form: columns in the list stay, literals do not. */
  private val ColumnIn = """[\w.]+#\s+IN\s+\(""".r

  /** `a# INSET 1, 10, 11`: Spark's form for a literal list of more than ten values, all literals. */
  private val ColumnInSet = """[\w.]+#\s+INSET\s+""".r

  /**
   * A pushed-down filter, in the data source's form, up to its value: `GreaterThan(id,` in
   * `GreaterThan(id,100)`, `EqualTo(name,` in `EqualTo(name,abc)`. The value goes; the filter and its
   * column stay.
   */
  private val PushedFilter =
    """\b(?:EqualTo|EqualNullSafe|GreaterThan|GreaterThanOrEqual|LessThan|LessThanOrEqual|StringStartsWith|StringEndsWith|StringContains)\([^,()]+,""".r
  private val PushedInPattern = """\bIn\(([^,()]+), \[[^\]]*\]\)""".r

  /**
   * Where the operand starting at `from` ends: at the first closing parenthesis that is not its own,
   * or at the end of the line, whichever comes first. Parentheses inside the operand, as in a string
   * value `prefix(A)`, are its own.
   */
  private def operandEnd(s: String, from: Int): Int = {
    var depth = 0
    var i = from
    while (i < s.length && s.charAt(i) != '\n') {
      val c = s.charAt(i)
      if (c == '(') depth += 1
      else if (c == ')') { if (depth == 0) return i; depth -= 1 }
      i += 1
    }
    i
  }

  /**
   * Rewrites, after every match of `start`, the operand that follows: `rewrite` gets the operand's
   * text and returns its replacement, or None to leave it.
   */
  private def rewriteOperands(s: String, start: scala.util.matching.Regex)(rewrite: String => Option[String]): String = {
    // Java's builder: Scala's has no append(CharSequence, start, end), and would append a tuple.
    val out = new java.lang.StringBuilder
    val m = start.pattern.matcher(s)
    var copied = 0
    var from = 0
    // `from` only moves forward: an operand starts where its match ends, after the match's start.
    while (from < s.length && m.find(from)) {
      val opStart = m.end()
      val opEnd = operandEnd(s, opStart)
      rewrite(s.substring(opStart, opEnd)).foreach { r =>
        out.append(s, copied, opStart).append(r)
        copied = opEnd
      }
      from = opEnd max opStart
    }
    out.append(s, copied, s.length).toString
  }

  /**
   * `?` for a literal operand. One that mentions a column, `b#` or `cast(b# as int)`, is kept: after
   * expression ids are stripped, every column reference ends in `#`.
   */
  private def literal(operand: String): Option[String] =
    if (operand.contains('#')) None else Some("?")

  /**
   * The elements of an IN list, split on top-level commas: columns kept, each run of literals one
   * `?`, so the number of values does not matter either.
   */
  private def inList(elements: String): Option[String] = {
    val parts = new scala.collection.mutable.ListBuffer[String]
    var depth = 0
    val cur = new StringBuilder
    elements.foreach { c =>
      if (c == ',' && depth == 0) { parts += cur.toString; cur.clear() }
      else {
        if (c == '(') depth += 1 else if (c == ')') depth -= 1
        cur.append(c)
      }
    }
    parts += cur.toString
    val normalised = parts.toList.map(p => if (p.contains('#')) p.trim else "?")
    val collapsed = normalised.foldRight(List.empty[String]) {
      case ("?", "?" :: rest) => "?" :: rest
      case (e, acc)           => e :: acc
    }
    Some(collapsed.mkString(","))
  }

  /**
   * 64 bits of SHA-256, hex encoded. Long enough that collisions between query shapes in one
   * deployment are not a practical concern, short enough to stay a cheap label.
   */
  private val FingerprintHexChars = 16

  /** The normalised plan text. Exposed for tests; the fingerprint is what callers want. */
  private[attributes] def normalise(plan: String): String = {
    val noExprIds = ExprIdPattern.replaceAllIn(plan, "#")
    val noPlanIds = PlanIdPattern.replaceAllIn(noExprIds, "plan_id=")
    val noCodegen = CodegenIdPattern.replaceAllIn(noPlanIds, "[codegen id]")
    val noStats   = StatisticsPattern.replaceAllIn(noCodegen, "Statistics()")
    // Literals and paths (#207): run after expression ids, which the column patterns rely on.
    val noPaths   = LocationPattern.replaceAllIn(noStats, m => scala.util.matching.Regex.quoteReplacement(s"Location: ${m.group(1)}"))
    val noValues  = rewriteOperands(noPaths, ColumnComparison)(literal)
    val noInLists = rewriteOperands(noValues, ColumnIn)(inList)
    val noInSets  = rewriteOperands(noInLists, ColumnInSet)(_ => Some("?"))
    val noPushed  = rewriteOperands(noInSets, PushedFilter)(_ => Some("?"))
    PushedInPattern.replaceAllIn(noPushed, m => scala.util.matching.Regex.quoteReplacement(s"In(${m.group(1)}, [?])"))
  }

  /**
   * Fingerprint of a plan, or None when there is no plan to fingerprint.
   *
   * Callers must pass the FULL plan, never the truncated attribute value: hashing after
   * truncation would make the fingerprint depend on `FLARE_SQL_PLAN_MAX_CHARS`, so the same
   * query would group differently between two deployments configured differently.
   */
  def of(plan: String): Option[String] =
    Option(plan).map(_.trim).filter(_.nonEmpty).map { p =>
      val digest = MessageDigest.getInstance("SHA-256").digest(normalise(p).getBytes("UTF-8"))
      digest.take(FingerprintHexChars / 2).map(b => f"${b & 0xff}%02x").mkString
    }
}
