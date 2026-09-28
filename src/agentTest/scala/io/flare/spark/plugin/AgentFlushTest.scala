package io.flare.spark.plugin

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.opentelemetry.api.GlobalOpenTelemetry
import munit.FunSuite

import java.net.{BindException, InetSocketAddress}
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

/**
 * Proves Flare's Spark-side code can flush the agent's SDK (#122).
 *
 * This test runs on the application classloader, where Spark loads Flare's plugins, and there
 * `GlobalOpenTelemetry` is only the agent's bridge. The build sets the metric export interval to
 * ten minutes, so a metric that reaches the stub collector within seconds can only have been sent
 * by the flush. Without it, a JVM killed right after a run loses whatever is still buffered.
 */
class AgentFlushTest extends FunSuite {

  test("TelemetryFlush reaches the agent's SDK from the application classloader") {
    val interval = sys.props.getOrElse("otel.metric.export.interval", "")
    assert(
      interval.nonEmpty && interval.toLong >= 60000L,
      s"otel.metric.export.interval must be long for this test to mean anything, was '$interval'",
    )

    val port = sys.props.getOrElse("flare.agent.test.collector.port", fail("collector port not set")).toInt
    val requests = new LinkedBlockingQueue[Array[Byte]]()
    val executor = Executors.newSingleThreadExecutor()
    val server = bind(port)
    server.createContext(
      "/v1/metrics",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val payload = exchange.getRequestBody.readAllBytes()
          exchange.sendResponseHeaders(200, -1)
          exchange.close()
          requests.offer(payload)
        }
      },
    )
    server.setExecutor(executor)
    server.start()

    try {
      GlobalOpenTelemetry.getMeter("flare-agent-flush-test")
        .counterBuilder("flare.agent.flush.probe").build()
        .add(1L)

      assert(TelemetryFlush.flushAgent(5000L), "the agent's flush entry point was not found")

      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
      val seen = new StringBuilder
      while (!seen.toString.contains("flare.agent.flush.probe") && System.nanoTime() < deadline) {
        val request = requests.poll(100, TimeUnit.MILLISECONDS)
        if (request != null) seen.append(new String(request, ISO_8859_1))
      }
      assert(
        seen.toString.contains("flare.agent.flush.probe"),
        "the probe metric did not arrive, so the flush did not reach the agent's meter provider",
      )
    } finally {
      server.stop(0)
      executor.shutdownNow()
    }
  }

  /** The build reserves the port before forking; retry briefly in case it was taken meanwhile. */
  private def bind(port: Int): HttpServer = {
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
