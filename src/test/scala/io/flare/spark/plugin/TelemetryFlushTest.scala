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

  // #139. Executors go quiet between the stages of every query, so with a one-second quiet they
  // flushed every few seconds, each flush re-sending every cumulative metric series.
  test("within the interval, short gaps do not run the action and a longer quiet still does") {
    val runs = new AtomicInteger(0)
    val quiet = new QuietPeriodAction(20L, () => { runs.incrementAndGet(); () }, "test-quiet",
      minIntervalMs = 10000L, throttledDelayMs = 300L)
    try {
      quiet.request()
      awaitCount(runs, 1, 2000L)
      // Gaps of 100ms, like the pauses between stages: past the 20ms quiet, short of the 300ms one.
      (1 to 8).foreach { _ =>
        quiet.request()
        Thread.sleep(100)
      }
      assertEquals(runs.get(), 1, "a short gap inside the interval must not run the action")

      val lastRequest = System.nanoTime()
      quiet.request() // the end of the run: nothing follows
      awaitCount(runs, 2, 2000L)
      assertEquals(runs.get(), 2, "the longer quiet must still run it, like the end of a run")
      val tookMs = (System.nanoTime() - lastRequest) / 1000000L
      assert(tookMs >= 280L && tookMs < 1000L, s"ran ${tookMs}ms after the last request, not ~300ms")
    } finally quiet.close()
  }

  // The least obvious branch: late in the interval, its end comes before the longer quiet would,
  // and wins. Neither the full longer quiet nor the short one applies.
  test("late in the interval, the action runs when the interval ends if that is sooner") {
    val runs  = new AtomicInteger(0)
    val times = new java.util.concurrent.ConcurrentLinkedQueue[java.lang.Long]()
    val quiet = new QuietPeriodAction(20L, () => {
      times.add(System.nanoTime()); runs.incrementAndGet(); ()
    }, "test-quiet", minIntervalMs = 400L, throttledDelayMs = 300L)
    try {
      quiet.request()
      awaitCount(runs, 1, 2000L)
      val first = times.peek().longValue
      Thread.sleep(250) // 250ms into the 400ms interval: 150ms left, less than the 300ms quiet
      val requested = System.nanoTime()
      quiet.request()
      awaitCount(runs, 2, 2000L)
      val second = times.toArray.last.asInstanceOf[java.lang.Long].longValue
      val afterRequestMs = (second - requested) / 1000000L
      val afterFirstMs   = (second - first) / 1000000L
      assert(afterRequestMs < 280L, s"waited ${afterRequestMs}ms, the full 300ms quiet instead of the interval's end")
      assert(afterFirstMs >= 380L, s"ran ${afterFirstMs}ms after the previous run, before the 400ms interval ended")
    } finally quiet.close()
  }

  test("once the interval has passed, the short quiet applies again") {
    val runs = new AtomicInteger(0)
    val quiet = new QuietPeriodAction(20L, () => { runs.incrementAndGet(); () }, "test-quiet",
      minIntervalMs = 200L, throttledDelayMs = 1000L)
    try {
      quiet.request()
      awaitCount(runs, 1, 2000L)
      Thread.sleep(400) // past the interval
      val requested = System.nanoTime()
      quiet.request()
      awaitCount(runs, 2, 2000L)
      val tookMs = (System.nanoTime() - requested) / 1000000L
      assert(tookMs < 150L, s"took ${tookMs}ms; only the 20ms quiet should apply")
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
