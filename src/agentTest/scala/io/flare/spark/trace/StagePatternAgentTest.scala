package io.flare.spark.trace

import io.opentelemetry.api.trace.Span
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import munit.FunSuite

/**
 * `FLARE_TASK_STAGE_PATTERN` under the real agent (#208): the driver decides per stage, from its
 * name, as the stage is submitted, and the stage's tasks read the decision.
 *
 * A task is traced when the plugin opened a task span and made it current, so the span current
 * inside the task differs from the stage span named in the stage's own context key. A join's two
 * map stages are submitted together, and here end in differently named operations,
 * `map at StagePatternAgentTest.scala` and `keyBy at StagePatternAgentTest.scala`, so the pattern
 * gives them opposite decisions in the same job.
 *
 * Task spans need `spark.plugins`: initialised by the agent alone, Flare registers its driver
 * listener but no executor plugin, so there are no task spans to filter. What that path does
 * decide is the marker each stage's tasks receive, which the tests check on both paths.
 */
class StagePatternAgentTest extends FunSuite {

  /** Per task attempt: (operation the stage ends in, attempt number, traced, the stage's marker). */
  private def run(pattern: String, plugins: Boolean, stageIds: Option[String] = None, granularity: String = "all",
                  failFirstAttempt: Boolean = false, slowListener: Boolean = false): Seq[(String, Int, Boolean, String)] = {
    val props = Map("FLARE_TRACE_GRANULARITY" -> granularity, "FLARE_TASK_STAGE_PATTERN" -> pattern) ++
      stageIds.map("FLARE_TASK_STAGES" -> _)
    props.foreach { case (k, v) => sys.props(k) = v }
    StagePatternAgentTest.seen.clear()
    try StubCollector.during {
      val conf = new SparkConf()
        .setMaster(if (failFirstAttempt) "local[2,2]" else "local[2]") // [threads, maxFailures]
        .setAppName("flare-stage-pattern").set("spark.ui.enabled", "false")
      if (plugins) conf.set("spark.plugins", "io.flare.spark.plugin.FlareSparkPlugin")
      val sc = new SparkContext(conf)
      try {
        if (slowListener) sc.addSparkListener(new SparkListener {
          override def onStageSubmitted(e: SparkListenerStageSubmitted): Unit = Thread.sleep(300)
        })
        def record(op: String)(it: Iterator[Int]): Iterator[Int] = {
          val tc = TaskContext.get()
          if (failFirstAttempt && tc.attemptNumber() == 0 && tc.partitionId() == 0)
            throw new RuntimeException("first attempt fails")
          val stageSpan = Option(tc.getLocalProperty(s"flare.stage.${tc.stageId()}.traceparent")).map(_.split("-")(2))
          val traced = stageSpan.exists(_ != Span.current().getSpanContext.getSpanId)
          StagePatternAgentTest.seen.add((op, tc.attemptNumber(), traced, tc.getLocalProperty(s"flare.stage.${tc.stageId()}.tasks")))
          it
        }
        val mapped = sc.parallelize(1 to 40, 4).mapPartitions(record("map")).map(i => (i % 5, i))
        val keyed  = sc.parallelize(1 to 40, 4).mapPartitions(record("keyBy")).keyBy(_ % 5)
        mapped.join(keyed).count()
      } finally sc.stop()
      StagePatternAgentTest.seen.toArray(Array.empty[(String, Int, Boolean, String)]).toSeq
    } finally props.keys.foreach(sys.props.remove)
  }

  private def tracedBy(rows: Seq[(String, Int, Boolean, String)]): Map[String, Set[Boolean]] =
    rows.groupBy(_._1).map { case (op, rs) => op -> rs.map(_._3).toSet }

  /** The marker each stage's tasks received: absent (null) admits, `false` excludes. */
  private def markersBy(rows: Seq[(String, Int, Boolean, String)]): Map[String, Set[String]] =
    rows.groupBy(_._1).map { case (op, rs) => op -> rs.map(_._4).toSet }

  for (plugins <- Seq(true, false)) {
    val path = if (plugins) "with spark.plugins" else "initialised by the agent alone"
    test(s"two stages submitted together get opposite decisions ($path)") {
      val rows = run("map at .*", plugins)
      assertEquals(markersBy(rows), Map("map" -> Set[String](null), "keyBy" -> Set("false")), s"rows: $rows")
      if (plugins) assertEquals(tracedBy(rows), Map("map" -> Set(true), "keyBy" -> Set(false)), s"rows: $rows")
    }
  }

  test("the ID filter and the pattern combine with AND") {
    // The result stage is 2; the map stages are 0 and 1. Restricting IDs to it leaves no map stage
    // traced, although the pattern admits the map side.
    assertEquals(tracedBy(run("map at .*", plugins = true, stageIds = Some("2"))),
      Map("map" -> Set(false), "keyBy" -> Set(false)))
    // IDs admitting both map stages leave the pattern to decide.
    assertEquals(tracedBy(run("map at .*", plugins = true, stageIds = Some("0,1"))),
      Map("map" -> Set(true), "keyBy" -> Set(false)))
  }

  test("a retried task gets its stage's decision too") {
    val rows = run("map at .*", plugins = true, failFirstAttempt = true)
    assert(rows.exists { case (op, attempt, _, _) => op == "map" && attempt == 1 }, s"no retry ran: $rows")
    assertEquals(tracedBy(rows), Map("map" -> Set(true), "keyBy" -> Set(false)), s"rows: $rows")
  }

  test("a lagging listener bus does not change the decisions") {
    assertEquals(tracedBy(run("map at .*", plugins = true, slowListener = true)),
      Map("map" -> Set(true), "keyBy" -> Set(false)))
  }

  test("a driver at jobs granularity still decides, for executors that trace tasks") {
    // Driver and executors read their granularity separately; this JVM is both, so the driver's
    // decision is checked as the marker the tasks receive.
    assertEquals(markersBy(run("map at .*", plugins = true, granularity = "jobs")),
      Map("map" -> Set[String](null), "keyBy" -> Set("false")))
  }
}

object StagePatternAgentTest {
  // Written from inside tasks, which run in this JVM under local mode.
  val seen = new java.util.concurrent.ConcurrentLinkedQueue[(String, Int, Boolean, String)]()
}
