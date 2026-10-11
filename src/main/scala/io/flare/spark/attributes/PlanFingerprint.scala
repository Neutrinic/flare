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

  /**
   * An AQE query stage's id, the `Arguments` line of its details:
   * {{{
   * (22) BroadcastQueryStage
   * Output [1]: [ss_customer_sk#212L]
   * Arguments: 12
   * }}}
   * AQE numbers stages as it creates them, which follows which stages finish first, so the same
   * query gets different ids from run to run (#244). `TableCacheQueryStage` prints `Output: []`.
   * The id is the group. The pattern starts with its literal, so Java searches for it by
   * Boyer-Moore instead of trying the pattern at every character (#246).
   */
  private val QueryStageIdPattern = """QueryStage\nOutput(?: \[\d+\])?: [^\n]*\nArguments: (\d+)""".r

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
   * The end of a column and the operator after it, in Catalyst's form, as in `Condition :` and
   * `PartitionFilters:`: `#19 = ` in `(dt#19 = 2024-01-01)`, `#16L > ` in `(id#16L > 100)`. The
   * operand after it is a literal unless it mentions a column ([[ColumnMention]]), as a join key
   * does; it runs to the comparison's closing parenthesis, so a string value printed with
   * parentheses, `(s#9 = prefix(A))`, is one operand (#207). The space before the operand is
   * matched possessively, so the check sees the operand, not a space.
   */
  private val ColumnComparison = """#\d+L?\s*(?:<=>|<=|>=|!=|=|<|>)\s*+""".r

  /**
   * A column reference: Catalyst prints one as its name and expression id, `b#12` or `id#16L`. The
   * literal patterns run before ids are stripped (#207), because only the id tells a column from a
   * literal: after stripping, the column `b#` and the literal `C#` (`s = 'C#'`) look alike. Matching
   * the id, not the name, also covers names `\w` would miss, such as `甲#7`.
   */
  private val ColumnMention = """#\d+L?(?!\w)""".r

  private def mentionsColumn(s: String): Boolean = ColumnMention.findFirstIn(s).isDefined

  /** `a# IN (b#,1,2)` in Catalyst's form: columns in the list stay, literals do not. */
  private val ColumnIn = """#\d+L?\s+IN\s+\(""".r

  /**
   * `a# INSET 1, 10, 11`: Spark's form for a literal list of more than ten values, all literals. Its
   * values are not parenthesised, so a predicate after it shares the enclosing parentheses:
   * `(a# INSET 1, 10, 11 AND (b# > 100))`. The values end at a top-level ` AND ` or ` OR ` too, and
   * in a list such as `PartitionFilters: [a# INSET 1, 10, 11, (b# > 100)]` at the list's `]` or at a
   * `, ` whose next list element mentions a column ([[ColumnMention]]). Every filter does,
   * `NOT b# INSET …` and `isnotnull(c#)` included, and a literal value does not, even one with a `#`
   * in it.
   */
  private val ColumnInSet = """#\d+L?\s+INSET\s+""".r
  private val InSetPattern = """\s+INSET\s+\?""".r


  /**
   * A pushed-down filter, in the data source's form, up to its value: `GreaterThan(id,` in
   * `GreaterThan(id,100)`, `EqualTo(name,` in `EqualTo(name,abc)`. The value goes; the filter and its
   * column stay.
   */
  private val PushedFilter =
    """\b(?:EqualTo|EqualNullSafe|GreaterThan|GreaterThanOrEqual|LessThan|LessThanOrEqual|StringStartsWith|StringEndsWith|StringContains)\([^,()]+,""".r
  private val PushedInPattern = """\bIn\(([^,()]+), \[[^\]]*\]\)""".r

  /**
   * The two patterns above, tried only where a match can start: every pushed filter starts with
   * one of these, and every pushed `IN` with `In(` (#246). Tried at every character, the word
   * boundary and the alternation cost about a second for each pass over the plans of a TPC-DS run.
   */
  private val PushedFilterStarts = new LiteralLed(PushedFilter, "Equal", "GreaterThan", "LessThan", "String")
  private val PushedInStarts     = new LiteralLed(PushedInPattern, "In(")

  /**
   * Finds what `pattern.pattern.matcher(s).find(from)` finds, trying the pattern only where one of
   * `prefixes` occurs, which `indexOf` locates far faster than the regex engine scans. Correct only
   * if every match of `pattern` starts with one of `prefixes`. The pattern is anchored at each
   * candidate with transparent bounds, so a `\b` there still sees the character before it.
   */
  private final class LiteralLed(pattern: scala.util.matching.Regex, prefixes: String*) {
    /** A search over `s` for ascending `from`: each prefix's next occurrence is looked up once. */
    def in(s: String): Int => Option[java.util.regex.MatchResult] = {
      val m = pattern.pattern.matcher(s).useTransparentBounds(true).useAnchoringBounds(false)
      val next = Array.fill(prefixes.length)(-2) // -2: not looked up yet; -1: none left
      from => {
        var at = from
        var found: Option[java.util.regex.MatchResult] = None
        var done = false
        while (!done) {
          var best = -1
          var i = 0
          while (i < prefixes.length) {
            if (next(i) != -1 && next(i) < at) next(i) = s.indexOf(prefixes(i), at)
            if (next(i) >= 0 && (best < 0 || next(i) < best)) best = next(i)
            i += 1
          }
          if (best < 0) done = true
          else {
            m.region(best, s.length)
            if (m.lookingAt()) { found = Some(m.toMatchResult); done = true }
            else at = best + 1
          }
        }
        found
      }
    }
  }

  /**
   * Where the operand starting at `from` ends: at the first closing parenthesis that is not its own,
   * or at the end of the line, whichever comes first. Parentheses inside the operand, as in a string
   * value `prefix(A)`, are its own.
   */
  private def operandEnd(s: String, from: Int, atConnective: Boolean = false): Int = {
    var depth = 0
    var i = from
    while (i < s.length && s.charAt(i) != '\n') {
      val c = s.charAt(i)
      if (c == '(') depth += 1
      else if (c == ')') { if (depth == 0) return i; depth -= 1 }
      else if (atConnective && depth == 0 &&
          (s.startsWith(" AND ", i) || s.startsWith(" OR ", i) || c == ']' ||
            (s.startsWith(", ", i) && mentionsColumn(listElement(s, i + 2))))) return i
      i += 1
    }
    i
  }

  /**
   * The filter-list element starting at `from`: up to the next top-level `, `, ` AND `, ` OR `, `]`
   * or end of line. Stopping at ` AND ` keeps `9 AND (b# > 100)`, an INSET's last value followed by
   * a predicate, from reading as an element that mentions a column.
   */
  private def listElement(s: String, from: Int): String = {
    var depth = 0
    var i = from
    while (i < s.length && s.charAt(i) != '\n' &&
        !(depth == 0 && (s.charAt(i) == ']' || s.startsWith(", ", i) || s.startsWith(" AND ", i) ||
          s.startsWith(" OR ", i)))) {
      val c = s.charAt(i)
      if (c == '(') depth += 1 else if (c == ')') depth -= 1
      i += 1
    }
    s.substring(from, i)
  }

  /**
   * Rewrites, after every match of `start`, the operand that follows: `rewrite` gets the operand's
   * text and returns its replacement, or None to leave it.
   */
  private def rewriteOperands(s: String, start: scala.util.matching.Regex, atConnective: Boolean = false)(
    rewrite: String => Option[String]
  ): String = {
    val m = start.pattern.matcher(s)
    rewriteOperandsAt(s, from => if (m.find(from)) m.end() else -1, atConnective)(rewrite)
  }

  /** As above, with the matches found by `start` (#246). */
  private def rewriteOperands(s: String, start: LiteralLed)(rewrite: String => Option[String]): String = {
    val find = start.in(s)
    rewriteOperandsAt(s, from => find(from).fold(-1)(_.end()), atConnective = false)(rewrite)
  }

  /** `operandAfter(from)`: where the first operand at or after `from` starts, or -1 when none does. */
  private def rewriteOperandsAt(s: String, operandAfter: Int => Int, atConnective: Boolean)(
    rewrite: String => Option[String]
  ): String = {
    // Java's builder: Scala's has no append(CharSequence, start, end), and would append a tuple.
    val out = new java.lang.StringBuilder
    var copied = 0
    var from = 0
    var opStart = -1
    // `from` only moves forward: an operand starts where its match ends, after the match's start.
    while (from < s.length && { opStart = operandAfter(from); opStart >= 0 }) {
      val opEnd = operandEnd(s, opStart, atConnective)
      rewrite(s.substring(opStart, opEnd)).foreach { r =>
        out.append(s, copied, opStart).append(r)
        copied = opEnd
      }
      from = opEnd max opStart
    }
    out.append(s, copied, s.length).toString
  }

  /** `Regex.replaceAllIn` with the matches found by `find`, and `replacement` taken literally (#246). */
  private def replaceAll(s: String, find: LiteralLed)(replacement: java.util.regex.MatchResult => String): String = {
    val next = find.in(s)
    val out = new java.lang.StringBuilder
    var copied = 0
    var found = next(0)
    while (found.isDefined) {
      val m = found.get
      out.append(s, copied, m.start).append(replacement(m))
      copied = m.end
      // The patterns never match empty text, so the next match starts after this one.
      found = if (m.end < s.length) next(m.end) else None
    }
    out.append(s, copied, s.length).toString
  }

  /**
   * `?` for a literal operand. One that mentions a column ([[ColumnMention]]), `b#` or
   * `cast(b# as int)`, is kept.
   */
  private def literal(operand: String): Option[String] =
    if (mentionsColumn(operand)) None else Some("?")

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
    val normalised = parts.toList.map(p => if (mentionsColumn(p)) p.trim else "?")
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
    // Literals and paths first (#207), while expression ids still mark which operands are columns.
    val noPaths   = LocationPattern.replaceAllIn(plan, m => scala.util.matching.Regex.quoteReplacement(s"Location: ${m.group(1)}"))
    val noValues  = rewriteOperands(noPaths, ColumnComparison)(literal)
    val noInLists = rewriteOperands(noValues, ColumnIn)(inList)
    // An all-literal INSET normalises exactly as an all-literal IN, so a list's length does not
    // matter either side of the ten values at which Spark switches from one to the other. Guarded
    // like any operand: Spark prints INSET strings unquoted, so a value such as `(` can carry the
    // scan past the ` AND ` after it, and an operand that took in a column is kept whole.
    // Both INSET patterns need the word, and a few plans in a hundred have it (#246); the second
    // backtracks through every run of indentation, at a cost of seconds over a TPC-DS run.
    val noInSets  = if (!noInLists.contains("INSET")) noInLists else InSetPattern.replaceAllIn(
      rewriteOperands(noInLists, ColumnInSet, atConnective = true)(literal), " IN (?)")
    val noPushed  = rewriteOperands(noInSets, PushedFilterStarts)(_ => Some("?"))
    val noLiterals = replaceAll(noPushed, PushedInStarts)(m => s"In(${m.group(1)}, [?])")
    val noExprIds = ExprIdPattern.replaceAllIn(noLiterals, "#")
    val noPlanIds = QueryStageIdPattern.replaceAllIn(PlanIdPattern.replaceAllIn(noExprIds, "plan_id="),
      m => scala.util.matching.Regex.quoteReplacement(m.matched.substring(0, m.start(1) - m.start) + "?"))
    val noCodegen = CodegenIdPattern.replaceAllIn(noPlanIds, "[codegen id]")
    StatisticsPattern.replaceAllIn(noCodegen, "Statistics()")
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
