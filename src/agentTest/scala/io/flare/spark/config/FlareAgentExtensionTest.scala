package io.flare.spark.config

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.opentelemetry.api.GlobalOpenTelemetry
import munit.FunSuite

import java.net.{BindException, InetSocketAddress}
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
    val server = bindCollector(collectorPort)

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

  /**
   * Binds the stub collector on the port the build reserved.
   *
   * <p>The build has to pick the port before this JVM forks, because the agent resolves the OTLP
   * endpoint during premain. It therefore opens an ephemeral port and closes it again, which
   * leaves a window for another process to take it. Retry briefly to ride out a transient
   * collision, and otherwise fail with a message that names the actual cause.
   */
  private def bindCollector(port: Int): HttpServer = {
    val address = new InetSocketAddress("127.0.0.1", port)
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    var lastFailure: BindException = null

    while (System.nanoTime() < deadline) {
      try return HttpServer.create(address, 0)
      catch {
        case failure: BindException =>
          lastFailure = failure
          Thread.sleep(250)
      }
    }

    fail(
      s"could not bind the stub OTLP collector on 127.0.0.1:$port — the build reserved this " +
        s"port but another process claimed it before the test JVM started ($lastFailure)",
    )
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

  /**
   * Scans the raw OTLP payload for each required string.
   *
   * <p>This is a substring match over protobuf decoded as ISO-8859-1, chosen to keep a protobuf
   * dependency out of the test. It proves each string appears somewhere in the export, not that
   * it appears as the value of its own key. `expectedVersion` is distinctive enough for that to
   * be a real assertion; `"driver"` is a short generic needle and only weakly implies that
   * `flare.role` holds it. Treat this as a smoke test that the SPI ran and the attributes were
   * exported — `FlareAutoConfigTest` is what pins the actual values.
   */
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
