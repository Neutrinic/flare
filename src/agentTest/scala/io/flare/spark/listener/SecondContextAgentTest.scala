package io.flare.spark.listener

import io.flare.spark.plugin.FlareDriverState
import io.flare.spark.trace.StubCollector
import munit.FunSuite
import org.apache.spark.{FlareTestHelpers, SparkConf, SparkContext}

/**
 * A second SparkContext in one JVM gets Flare's driver telemetry after an agent-initialised one
 * stops (#232). Installed through the agent alone, nothing reset the driver state when a context
 * stopped, so the next one found Flare already initialised with the stopped context's listener.
 */
class SecondContextAgentTest extends FunSuite {

  private def context(name: String) = new SparkContext(new SparkConf().setMaster("local[1]")
    .setAppName(name).set("spark.ui.enabled", "false"))

  test("a SparkContext started after an agent-initialised one stopped gets its own listener") {
    StubCollector.during {
      FlareDriverState.shutdown() // whatever an earlier test in this JVM left
      val first = context("flare-first-context")
      val firstListener = try {
        val ls = FlareTestHelpers.flareListeners(first)
        assertEquals(ls.size, 1)
        assertEquals(FlareDriverState.currentListener, ls.headOption, "the first context does not own the state")
        ls.head
      } finally first.stop()
      assert(!FlareDriverState.initialized, "stopping the first context left Flare's driver state initialised")

      val second = context("flare-second-context")
      try {
        val ls = FlareTestHelpers.flareListeners(second)
        assertEquals(ls.size, 1, "the second context has no Flare listener")
        assert(ls.head ne firstListener)
        assertEquals(FlareDriverState.currentListener, ls.headOption, "the driver state is not the second context's")
      } finally second.stop()
    }
  }
}
