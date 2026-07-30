package io.flare.spark.config

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.opentelemetry.api.GlobalOpenTelemetry
import munit.FunSuite

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.{ISO_8859_1, UTF_8}
import java.util.Properties
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import java.util.jar.JarFile

class FlareAgentExtensionTest extends FunSuite {

  private val spiDescriptor =
    "META-INF/services/io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider"
  private val providerClass = "io.flare.spark.config.FlareAutoConfig"
  private val buildMetadata = "io/flare/spark/flare-build.properties"

  test("real agent loads the assembled SPI provider and exports Flare resource attributes") {
    val expectedVersion = requiredProperty("flare.agent.test.expected.version")
    verifyAssembly(expectedVersion)

    val collectorPort = requiredProperty("flare.agent.test.collector.port").toInt
    val requests = new LinkedBlockingQueue[Array[Byte]]()
    val executor = Executors.newSingleThreadExecutor()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", collectorPort), 0)

    server.createContext(
      "/v1/traces",
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
      GlobalOpenTelemetry
        .getTracer("flare-agent-test-probe")
        .spanBuilder("flare.agent.spi.probe")
        .startSpan()
        .end()

      val requiredStrings = Seq(
        "flare.agent.spi.probe",
        "flare.role",
        "driver",
        "flare.version",
        expectedVersion,
      )
      awaitPayload(requests, requiredStrings)
    } finally {
      server.stop(0)
      executor.shutdownNow()
    }
  }

  private def verifyAssembly(expectedVersion: String): Unit = {
    val jar = new JarFile(requiredProperty("flare.agent.test.extension.jar"))
    try {
      val descriptor = readEntry(jar, spiDescriptor)
      assert(
        descriptor.linesIterator.map(_.trim).contains(providerClass),
        s"$spiDescriptor must register $providerClass",
      )

      val properties = new Properties()
      val metadataEntry = Option(jar.getJarEntry(buildMetadata))
        .getOrElse(fail(s"assembled extension is missing $buildMetadata"))
      val stream = jar.getInputStream(metadataEntry)
      try properties.load(stream)
      finally stream.close()

      assertEquals(properties.getProperty("flare.version"), expectedVersion)
    } finally {
      jar.close()
    }
  }

  private def readEntry(jar: JarFile, path: String): String = {
    val entry = Option(jar.getJarEntry(path))
      .getOrElse(fail(s"assembled extension is missing $path"))
    val stream = jar.getInputStream(entry)
    try new String(stream.readAllBytes(), UTF_8)
    finally stream.close()
  }

  private def awaitPayload(
    requests: LinkedBlockingQueue[Array[Byte]],
    requiredStrings: Seq[String],
  ): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15)
    val payload = new StringBuilder
    var missing = requiredStrings

    while (missing.nonEmpty && System.nanoTime() < deadline) {
      val request = requests.poll(250, TimeUnit.MILLISECONDS)
      if (request != null) {
        payload.append(new String(request, ISO_8859_1))
        missing = requiredStrings.filterNot(payload.toString.contains)
      }
    }

    assert(
      missing.isEmpty,
      s"OTLP payload did not contain ${missing.mkString(", ")} " +
        s"(${requests.size()} queued requests remain)",
    )
  }

  private def requiredProperty(name: String): String =
    sys.props.getOrElse(name, fail(s"missing required system property $name"))
}
