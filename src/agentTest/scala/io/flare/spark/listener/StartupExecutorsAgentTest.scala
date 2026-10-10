package io.flare.spark.listener

import io.flare.spark.trace.StubCollector
import munit.FunSuite
import org.apache.spark.{FlareTestHelpers, SparkConf, SparkContext}

/**
 * The automatically registered listener knows the executors announced before it existed (#224).
 *
 * The SparkContext advice adds the listener as the constructor returns, after local mode's executor
 * was announced. Without seeding, the listener never counted it. Reproduces Codex's review probe
 * on #223.
 */
class StartupExecutorsAgentTest extends FunSuite {

  test("the automatically registered listener counts the executor announced before it") {
    StubCollector.during {
      val sc = new SparkContext(new SparkConf().setMaster("local[1]")
        .setAppName("flare-startup-executors").set("spark.ui.enabled", "false"))
      try {
        val listeners = FlareTestHelpers.flareListeners(sc)
        assertEquals(listeners.size, 1, "the advice registered no listener, or more than one")
        assertEquals(listeners.head.liveExecutorCount, 1, "the startup executor is not counted")
      } finally sc.stop()
    }
  }
}
