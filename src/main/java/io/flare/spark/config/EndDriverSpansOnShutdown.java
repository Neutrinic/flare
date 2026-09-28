package io.flare.spark.config;

import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import java.lang.management.ManagementFactory;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Ends Flare's open driver spans before the wrapped span processor shuts down (#122).
 *
 * <p>When a cluster manager stops the driver with SIGTERM, as a Databricks job cluster does, Spark
 * never stops its context. The agent's shutdown hook and Flare's run at the same time, and the
 * agent's usually wins: its batch processor stops, then Flare ends the root span and it is dropped.
 * Every run on a Databricks job cluster lost its root this way.
 *
 * <p>Wrapping the processor puts Flare inside the agent's own shutdown sequence. The tracer
 * provider shuts processors down one by one, and this wrapper asks Flare to end its spans first,
 * so they are queued while the batch processor still accepts them and go out in its final export.
 * Whichever hook fires first, the order is the same.
 *
 * <p>Flare's span state lives in Spark's classloader and this class in the agent's, so the call
 * goes through the platform MBean server. The object name is duplicated in {@code DriverSpans};
 * change them together.
 */
final class EndDriverSpansOnShutdown implements SpanProcessor {

  static final String MBEAN_NAME = "io.flare.spark:type=DriverSpans";

  private static final Logger logger = Logger.getLogger(EndDriverSpansOnShutdown.class.getName());

  private final SpanProcessor delegate;

  EndDriverSpansOnShutdown(SpanProcessor delegate) {
    this.delegate = delegate;
  }

  @Override
  public CompletableResultCode shutdown() {
    endDriverSpans();
    return delegate.shutdown();
  }

  /** Asks Flare to end its open driver spans. A no-op when Flare never registered, or already ended them. */
  static void endDriverSpans() {
    try {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      ObjectName name = new ObjectName(MBEAN_NAME);
      if (server.isRegistered(name)) {
        server.invoke(name, "endOpenSpans", new Object[0], new String[0]);
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, "[Flare] Could not end driver spans before shutdown", e);
    }
  }

  @Override
  public void onStart(Context parentContext, ReadWriteSpan span) {
    delegate.onStart(parentContext, span);
  }

  @Override
  public boolean isStartRequired() {
    return delegate.isStartRequired();
  }

  @Override
  public void onEnd(ReadableSpan span) {
    delegate.onEnd(span);
  }

  @Override
  public boolean isEndRequired() {
    return delegate.isEndRequired();
  }

  @Override
  public CompletableResultCode forceFlush() {
    return delegate.forceFlush();
  }

  @Override
  public String toString() {
    return "EndDriverSpansOnShutdown{" + delegate + "}";
  }
}
