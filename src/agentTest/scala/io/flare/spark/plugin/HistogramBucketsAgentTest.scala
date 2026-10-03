package io.flare.spark.plugin

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.flare.spark.metrics.{FlareMetrics, MetricAttributes}
import munit.FunSuite

import java.net.{BindException, InetSocketAddress}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

/**
 * Flare's histogram bucket advice survives the agent (#180).
 *
 * Flare records through the application's OpenTelemetry API, which the agent bridges to its own
 * SDK. Advice set on the application-side builder only takes effect if the bridge carries it
 * across; if it did not, the SDK's defaults would apply and every task over ten seconds would
 * land in the overflow bucket again, with every unit test still passing.
 *
 * Checked on the wire: 1,800,000 ms is a boundary only Flare's advice contains, so its encoding in
 * the exported histogram's explicit bounds proves the advice arrived.
 */
class HistogramBucketsAgentTest extends FunSuite {

  test("the task duration histogram is exported with Flare's bucket boundaries") {
    val port = sys.props.getOrElse("flare.agent.test.collector.port", fail("collector port not set")).toInt
    val requests = new LinkedBlockingQueue[Array[Byte]]()
    val executor = Executors.newSingleThreadExecutor()
    val server = bind(port)
    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val payload = exchange.getRequestBody.readAllBytes()
        exchange.sendResponseHeaders(200, -1)
        exchange.close()
        if (exchange.getRequestURI.getPath == "/v1/metrics") requests.offer(payload)
      }
    })
    server.setExecutor(executor)
    server.start()

    try {
      FlareMetrics.create(enabled = true).taskDuration
        .record(90000.0, MetricAttributes.forTask("1", "SUCCESS"))
      assert(TelemetryFlush.flushAgent(5000L), "the agent's flush entry point was not found")

      // OTLP encodes explicit_bounds as packed little-endian doubles.
      val bound = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(1800000.0).array()
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
      var seen = false
      while (!seen && System.nanoTime() < deadline) {
        val request = requests.poll(100, TimeUnit.MILLISECONDS)
        if (request != null && contains(request, "flare.task.duration".getBytes("UTF-8")))
          seen = contains(request, bound)
      }
      assert(seen, "flare.task.duration was not exported with Flare's boundaries: the agent dropped the advice")
    } finally {
      server.stop(0)
      executor.shutdownNow()
    }
  }

  private def contains(haystack: Array[Byte], needle: Array[Byte]): Boolean =
    haystack.indices.exists(i => i + needle.length <= haystack.length &&
      needle.indices.forall(j => haystack(i + j) == needle(j)))

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
