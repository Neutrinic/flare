package io.flare.spark.config

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.opentelemetry.api.GlobalOpenTelemetry
import munit.FunSuite

import java.net.{BindException, HttpURLConnection, InetSocketAddress, URL}
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

/**
 * Flare turns the agent's own instrumentations off by default, keeping only what Flare and the
 * user's code need (#145). These run under the real agent with Flare as its extension, and none of
 * the build's options touch the instrumentation switches, so what they see is Flare's defaults. The
 * build runs them a second time with FLARE_ENABLED=false, which must keep the defaults (#206).
 */
class AgentDefaultsAgentTest extends FunSuite {

  private val port = sys.props.getOrElse("flare.agent.test.collector.port", fail("collector port not set")).toInt

  /** A stub collector that also answers `/probe`, the target of the HTTP call made inside a task. */
  private def withCollector(body: (LinkedBlockingQueue[String], LinkedBlockingQueue[String]) => Unit): Unit = {
    val traces  = new LinkedBlockingQueue[String]()
    val metrics = new LinkedBlockingQueue[String]()
    val executor = Executors.newFixedThreadPool(2)
    val server = bind()
    def sink(queue: LinkedBlockingQueue[String]) = new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val payload = exchange.getRequestBody.readAllBytes()
        exchange.sendResponseHeaders(200, -1)
        exchange.close()
        queue.offer(new String(payload, ISO_8859_1))
      }
    }
    server.createContext("/v1/traces", sink(traces))
    server.createContext("/v1/metrics", sink(metrics))
    server.createContext("/probe", sink(new LinkedBlockingQueue[String]()))
    server.setExecutor(executor)
    server.start()
    try body(traces, metrics)
    finally {
      server.stop(0)
      executor.shutdownNow()
    }
  }

  /** Collects payloads until one contains `marker`, then keeps collecting for `settleMs` more. */
  private def collectUntil(queue: LinkedBlockingQueue[String], marker: String, settleMs: Long): String = {
    val seen = new StringBuilder
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (!seen.toString.contains(marker) && System.nanoTime() < deadline) {
      Option(queue.poll(100, TimeUnit.MILLISECONDS)).foreach(seen.append)
    }
    val settle = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(settleMs)
    while (System.nanoTime() < settle) Option(queue.poll(50, TimeUnit.MILLISECONDS)).foreach(seen.append)
    seen.toString
  }

  test("an HTTP call inside a task makes no span, while Flare's spans still export") {
    withCollector { (traces, _) =>
      val task = GlobalOpenTelemetry.getTracer("flare-agent-defaults-test").spanBuilder("spark.task.executor").startSpan()
      val scope = task.makeCurrent()
      try {
        val connection = new URL(s"http://127.0.0.1:$port/probe").openConnection().asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("POST")
        connection.setDoOutput(true)
        connection.getOutputStream.close()
        assertEquals(connection.getResponseCode, 200)
      } finally {
        scope.close()
        task.end()
      }

      val exported = collectUntil(traces, "spark.task.executor", settleMs = 1500L)
      assert(exported.contains("spark.task.executor"),
        "Flare's span did not arrive: the opentelemetry-api bridge must stay on")
      assert(!exported.contains("/probe"),
        "the HTTP call was traced: the agent's own instrumentations are not off by default")
    }
  }

  test("JVM runtime metrics stay on") {
    withCollector { (_, metrics) =>
      // The build sets a ten-minute metric interval, so flush through the agent's own entry point.
      Class.forName("io.opentelemetry.javaagent.bootstrap.OpenTelemetrySdkAccess")
        .getMethod("forceFlush", classOf[Long], classOf[TimeUnit])
        .invoke(null, Long.box(5000L), TimeUnit.MILLISECONDS)
      val exported = collectUntil(metrics, "jvm.memory.used", settleMs = 0L)
      assert(exported.contains("jvm.memory.used"), "no jvm.memory.used: runtime-telemetry must stay on")
    }
  }

  /** The build reserves the port before forking; retry briefly in case it was taken meanwhile. */
  private def bind(): HttpServer = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    var last: BindException = null
    while (System.nanoTime() < deadline) {
      try return HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0)
      catch {
        case e: BindException =>
          last = e
          Thread.sleep(250)
      }
    }
    fail(s"could not bind the stub collector on 127.0.0.1:$port ($last)")
  }
}
