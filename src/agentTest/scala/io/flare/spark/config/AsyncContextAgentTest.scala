package io.flare.spark.config

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{Span, SpanContext}
import munit.FunSuite

import java.util.concurrent.{Callable, CompletableFuture, Executors, TimeUnit}
import java.util.function.Supplier

/**
 * What happens to work a task hands to another thread, now that Flare drops root spans that are
 * not Spark's (#123 on the driver, #127 on executors).
 *
 * A span created on another thread is only kept if the agent carried the task's context there.
 * If it did not, the span has no parent, is not a `spark.*` span, and is dropped. Before the
 * sampler it would have been exported as a one-span trace detached from the task, so this is a
 * trade between an orphan and nothing, not between a child and nothing. The tests pin which
 * handoffs the agent covers.
 */
class AsyncContextAgentTest extends FunSuite {

  private val tracer = GlobalOpenTelemetry.getTracer("flare-async-context-test")

  /** Runs `handoff` inside a stand-in task span and returns the task and the async child contexts. */
  private def inTask(handoff: (() => SpanContext) => SpanContext): (SpanContext, SpanContext) = {
    val task = tracer.spanBuilder("spark.task.executor").startSpan()
    val scope = task.makeCurrent()
    try {
      val child = handoff { () =>
        val s = tracer.spanBuilder("SELECT orders").startSpan()
        try s.getSpanContext
        finally s.end()
      }
      (task.getSpanContext, child)
    } finally {
      scope.close()
      task.end()
    }
  }

  private def assertKeptUnder(task: SpanContext, child: SpanContext, via: String): Unit = {
    assert(task.isSampled, "the stand-in task span must itself be sampled")
    assertEquals(child.getTraceId, task.getTraceId, s"$via: the child is not in the task's trace")
    assert(child.isSampled, s"$via: the child was dropped")
  }

  test("an ExecutorService carries the task's context, so the child is kept") {
    val pool = Executors.newFixedThreadPool(1)
    try {
      val (task, child) = inTask { work =>
        pool.submit(new Callable[SpanContext] { override def call(): SpanContext = work() })
          .get(10, TimeUnit.SECONDS)
      }
      assertKeptUnder(task, child, "ExecutorService")
    } finally pool.shutdownNow()
  }

  test("CompletableFuture.supplyAsync carries the task's context, so the child is kept") {
    val (task, child) = inTask { work =>
      CompletableFuture.supplyAsync(new Supplier[SpanContext] { override def get(): SpanContext = work() })
        .get(10, TimeUnit.SECONDS)
    }
    assertKeptUnder(task, child, "CompletableFuture")
  }

  test("a raw Thread does not carry context, so its span has no parent and is dropped") {
    val (task, child) = inTask { work =>
      @volatile var ctx: SpanContext = SpanContext.getInvalid
      val t = new Thread(new Runnable { override def run(): Unit = ctx = work() })
      t.start()
      t.join(10000)
      // Without these, a thread that had not finished or never ran would leave the invalid context,
      // which passes the checks below without a span having been made at all.
      assert(!t.isAlive, "the raw thread did not finish")
      assert(ctx.isValid, "the raw thread did not produce a span context")
      ctx
    }
    assert(task.isSampled)
    // Without the agent's propagation the child starts its own trace, and the sampler drops it.
    // Before #123 this was exported as a detached one-span trace; it was never under the task.
    assertNotEquals(child.getTraceId, task.getTraceId, "a raw Thread unexpectedly inherited context")
    assert(!child.isSampled, "a parentless non-Spark span on another thread was not dropped")
    assert(!Span.current().getSpanContext.isValid)
  }
}
