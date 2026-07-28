package io.flare.spark.config

import io.opentelemetry.api.trace.Span
import munit.FunSuite
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.SparkSession

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Collections

class KafkaSparkTraceBridgeAgentTest extends FunSuite {

  // This brokerless fixture intentionally starts from ConsumerRecords rather than KafkaConsumer.poll.
  // It verifies the Spark-specific ListIterator process path and parent selection owned by this PR.
  // OpenTelemetry's Kafka tests own receive-span and propagated-link contents.
  test("Kafka ListIterator processing stays in the active Flare task trace") {
    val spark = SparkSession.builder()
      .appName("flare-kafka-trace-bridge-agent-test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.plugins", "io.flare.spark.plugin.FlareSparkPlugin")
      .getOrCreate()

    try {
      val observed = spark.sparkContext
        .parallelize(Seq(1), numSlices = 1)
        .mapPartitions(_ => KafkaSparkTraceBridgeProbe.observe())
        .collect()
        .head

      assert(observed.taskValid, "Flare must make a valid task span current")
      assert(observed.processValid, "Kafka instrumentation must start a process span")
      assertEquals(
        observed.processTraceId,
        observed.taskTraceId,
        "receive telemetry must keep Kafka processing in the Flare task trace",
      )
      assertNotEquals(
        observed.processSpanId,
        observed.taskSpanId,
        "the current span must be a Kafka process span, not the unchanged task span",
      )
      assertNotEquals(
        observed.processTraceId,
        KafkaSparkTraceBridgeProbe.UpstreamTraceId,
        "receive mode must not adopt the upstream producer context as the process parent",
      )
      assertEquals(
        observed.restoredSpanId,
        observed.taskSpanId,
        "closing the iterator's process span must restore the Flare task span",
      )
    } finally {
      spark.stop()
    }
  }
}

private object KafkaSparkTraceBridgeProbe {

  val UpstreamTraceId = "11111111111111111111111111111111"
  private val UpstreamSpanId = "2222222222222222"

  def observe(): Iterator[ObservedTraceContexts] = {
    val task = Span.current().getSpanContext
    val partition = new TopicPartition("input", 0)
    val record = new ConsumerRecord[String, String]("input", 0, 1L, "key", "value")
    record.headers().add(
      "traceparent",
      s"00-$UpstreamTraceId-$UpstreamSpanId-01".getBytes(UTF_8),
    )

    val recordsByPartition =
      Collections.singletonMap(partition, Collections.singletonList(record))
    val records = new ConsumerRecords[String, String](recordsByPartition)
    val iterator = records.records(partition).listIterator()

    iterator.next()
    val process = Span.current().getSpanContext
    iterator.hasNext()
    val restored = Span.current().getSpanContext

    Iterator.single(
      ObservedTraceContexts(
        taskValid = task.isValid,
        taskTraceId = task.getTraceId,
        taskSpanId = task.getSpanId,
        processValid = process.isValid,
        processTraceId = process.getTraceId,
        processSpanId = process.getSpanId,
        restoredSpanId = restored.getSpanId,
      ),
    )
  }
}

private final case class ObservedTraceContexts(
  taskValid: Boolean,
  taskTraceId: String,
  taskSpanId: String,
  processValid: Boolean,
  processTraceId: String,
  processSpanId: String,
  restoredSpanId: String,
)
