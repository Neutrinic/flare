package io.flare.spark.plugin

import munit.FunSuite

import java.util.concurrent.atomic.AtomicInteger

class TelemetryFlushTest extends FunSuite {

  test("without the agent there is nothing to flush through") {
    // Unit tests run without -javaagent, so the bootstrap flush entry point is absent. The agent
    // path is exercised for real in AgentFlushTest.
    assert(!TelemetryFlush.flushAgent(100L))
  }

  test("flushing with no global set does not install one") {
    // The executor flushes from a timer thread, so it can run while nothing is registered. If
    // that locked in a no-op global, a later GlobalOpenTelemetry.set would throw.
    io.opentelemetry.api.GlobalOpenTelemetry.resetForTest()
    TelemetryFlush.flush("test")
    assert(!io.opentelemetry.api.GlobalOpenTelemetry.isSet())
  }

  // ── QuietPeriodAction ───────────────────────────────────────────────────────

  private def awaitCount(count: AtomicInteger, expected: Int, withinMs: Long): Unit = {
    val deadline = System.currentTimeMillis() + withinMs
    while (count.get() < expected && System.currentTimeMillis() < deadline) Thread.sleep(10)
  }

  test("a burst of requests runs the action once, after the burst goes quiet") {
    val runs = new AtomicInteger(0)
    val quiet = new QuietPeriodAction(200L, () => { runs.incrementAndGet(); () }, "test-quiet")
    try {
      // Each request lands well inside the previous one's delay, so each restarts the timer.
      (1 to 10).foreach { _ =>
        quiet.request()
        Thread.sleep(20)
      }
      assertEquals(runs.get(), 0, "must not run while requests keep arriving")

      awaitCount(runs, 1, 2000L)
      Thread.sleep(400) // longer than the delay, so a second run would have happened by now
      assertEquals(runs.get(), 1)
    } finally quiet.close()
  }

  test("a request after the quiet period runs the action again") {
    val runs = new AtomicInteger(0)
    val quiet = new QuietPeriodAction(50L, () => { runs.incrementAndGet(); () }, "test-quiet")
    try {
      quiet.request()
      awaitCount(runs, 1, 2000L)
      quiet.request()
      awaitCount(runs, 2, 2000L)
      assertEquals(runs.get(), 2)
    } finally quiet.close()
  }

  test("close drops a pending run and ignores later requests") {
    val runs = new AtomicInteger(0)
    val quiet = new QuietPeriodAction(100L, () => { runs.incrementAndGet(); () }, "test-quiet")
    quiet.request()
    quiet.close()
    quiet.request()
    Thread.sleep(300)
    assertEquals(runs.get(), 0)
  }

  test("close without any request is a no-op") {
    val quiet = new QuietPeriodAction(100L, () => (), "test-quiet")
    quiet.close()
    quiet.close()
  }
}
