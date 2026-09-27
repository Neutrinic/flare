package io.flare.spark.plugin

import java.lang.management.ManagementFactory
import java.util.logging.{Level, Logger}
import javax.management.{InstanceAlreadyExistsException, ObjectName, StandardMBean}

/** The JMX operation the agent side calls as its tracer provider shuts down (#122). */
trait DriverSpansMBean {
  def endOpenSpans(): Unit
}

/**
 * Lets the agent end Flare's driver spans before it stops exporting them (#122).
 *
 * The agent's span processor wrapper, `EndDriverSpansOnShutdown`, lives in the agent's extension
 * classloader. Flare's span state lives in Spark's classloader. Neither can see the other's
 * classes, so they meet through the platform MBean server, which is one per JVM. The object name
 * is duplicated in the Java class; change them together.
 */
private[spark] object DriverSpans {

  val Name = "io.flare.spark:type=DriverSpans"

  private val logger = Logger.getLogger(DriverSpans.getClass.getName)

  def register(): Unit =
    try {
      val server = ManagementFactory.getPlatformMBeanServer
      val name = new ObjectName(Name)
      if (!server.isRegistered(name)) {
        val bean = new DriverSpansMBean {
          override def endOpenSpans(): Unit = FlareDriverState.endOpenSpans()
        }
        server.registerMBean(new StandardMBean(bean, classOf[DriverSpansMBean]), name)
      }
    } catch {
      // Lost a race with another registration in this JVM. The registered one stands.
      case _: InstanceAlreadyExistsException => ()
      // Without it the root span is only protected by the JVM hook, as before. Not fatal.
      case e: Exception =>
        logger.log(Level.WARNING, s"[Flare] Could not register $Name: ${e.getMessage}", e)
    }
}
