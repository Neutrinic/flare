package io.flare.spark.trace

import io.opentelemetry.api.trace.Span
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import munit.FunSuite

/**
 * Two stages of one job submitted together each run their tasks in their own stage's context (#204).
 *
 * A join's two map stages are submitted together, and Spark hands both TaskSets the job's one
 * `Properties` instance, read only when each task launches. With a single `traceparent` key, the
 * second stage's injection overwrote the first's before the first stage's tasks launched, so both
 * stages' tasks ran under the second stage. Runs at the default granularity, where the TaskRunner
 * advice is what makes the stage's context current inside the task.
 */
class StageContextAgentTest extends FunSuite {

  test("the map stages of a join each run under their own stage span") {
    StubCollector.during {
      val sc = new SparkContext(
        new SparkConf()
          .setMaster("local[2]")
          .setAppName("flare-stage-context-probe")
          .set("spark.ui.enabled", "false")
          .set("spark.plugins", "io.flare.spark.plugin.FlareSparkPlugin"))
      try {
        def side(n: Int) = sc.parallelize(1 to 40, 4).mapPartitions { it =>
          StageContextAgentTest.seen.add((TaskContext.get().stageId(), Span.current().getSpanContext.getSpanId))
          it.map(i => (i % 5, i * n))
        }
        // Several rounds: whether a stage's tasks launch before the next stage is submitted is a race.
        for (round <- 1 to 3) {
          StageContextAgentTest.seen.clear()
          side(1).join(side(2)).count()
          val spanIdsByStage = StageContextAgentTest.seen.toArray(Array.empty[(Int, String)])
            .groupBy(_._1).map { case (stage, rows) => stage -> rows.map(_._2).toSet }
          assertEquals(spanIdsByStage.size, 2, s"round $round: expected two map stages, saw $spanIdsByStage")
          spanIdsByStage.foreach { case (stage, ids) =>
            assertEquals(ids.size, 1, s"round $round: stage $stage's tasks ran under several spans: $ids")
            assert(Span.getInvalid.getSpanContext.getSpanId != ids.head, s"round $round: stage $stage ran with no context")
          }
          assertNotEquals(spanIdsByStage.values.head, spanIdsByStage.values.last,
            s"round $round: both map stages' tasks ran under the same span: $spanIdsByStage")
        }
      } finally sc.stop()
    }
  }
}

object StageContextAgentTest {
  // Written from inside tasks, which run in this JVM under local[2]; a field captured by the task
  // closure would be a serialized copy.
  val seen = new java.util.concurrent.ConcurrentLinkedQueue[(Int, String)]()
}
