package io.flare.spark.trace

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import munit.Assertions.fail

import java.net.{BindException, InetSocketAddress}
import java.util.concurrent.{Executors, TimeUnit}

/**
 * Accepts every OTLP request on the build's collector port while a test body runs.
 *
 * Agent tests that start a SparkContext need it: Flare's plugins flush on shutdown and when
 * executors go idle, and without a collector those exports fail and the agent's exporter backs
 * off, which delays AgentFlushTest's flush later in the same JVM.
 */
object StubCollector {

  def during[T](body: => T): T = {
    val port = sys.props.getOrElse("flare.agent.test.collector.port", fail("collector port not set")).toInt
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    var server: HttpServer = null
    while (server == null) {
      try server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0)
      catch {
        case e: BindException =>
          if (System.nanoTime() > deadline) fail(s"could not bind the stub collector on 127.0.0.1:$port ($e)")
          Thread.sleep(250)
      }
    }
    val executor = Executors.newSingleThreadExecutor()
    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        exchange.getRequestBody.readAllBytes()
        exchange.sendResponseHeaders(200, -1)
        exchange.close()
      }
    })
    server.setExecutor(executor)
    server.start()
    try body
    finally {
      server.stop(0)
      executor.shutdownNow()
    }
  }
}
