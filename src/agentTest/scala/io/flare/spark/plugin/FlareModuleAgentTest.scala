package io.flare.spark.plugin

import io.flare.spark.trace.StubCollector
import org.apache.spark.{SparkConf, SparkContext}
import munit.FunSuite

/**
 * Flare's own instrumentation module really loads under Flare's agent defaults (#145).
 *
 * Those defaults turn the agent's instrumentation off and enable `flare-spark` by name, so a
 * misspelled or dropped name would switch Flare's ByteBuddy hooks off with nothing else failing:
 * spans named like Flare's still export through the API. This starts a SparkContext with no
 * `spark.plugins`, so only the SparkContext advice can initialise Flare's driver state.
 */
class FlareModuleAgentTest extends FunSuite {

  test("the SparkContext hook runs, so the flare-spark module is enabled") {
    FlareDriverState.reset()
    // Stopping the context flushes at its application's end (#232); without a collector the export
    // fails and the agent's exporter backs off, delaying AgentFlushTest's flush later in this JVM.
    StubCollector.during {
      val sc = new SparkContext(
        new SparkConf().setMaster("local[1]").setAppName("flare-module-probe").set("spark.ui.enabled", "false"))
      try {
        assert(FlareDriverState.initialized,
          "Flare's driver state was not initialised: the SparkContext advice did not run")
      } finally {
        sc.stop()
        FlareDriverState.reset()
      }
    }
  }
}
