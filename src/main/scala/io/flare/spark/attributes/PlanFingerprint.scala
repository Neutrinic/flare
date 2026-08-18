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
   * 64 bits of SHA-256, hex encoded. Long enough that collisions between query shapes in one
   * deployment are not a practical concern, short enough to stay a cheap label.
   */
  private val FingerprintHexChars = 16

  /** The normalised plan text. Exposed for tests; the fingerprint is what callers want. */
  private[attributes] def normalise(plan: String): String = {
    val noExprIds = ExprIdPattern.replaceAllIn(plan, "#")
    val noPlanIds = PlanIdPattern.replaceAllIn(noExprIds, "plan_id=")
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
