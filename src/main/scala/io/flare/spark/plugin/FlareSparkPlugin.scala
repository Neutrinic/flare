package io.flare.spark.plugin

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}

/**
 * Top-level SparkPlugin entry point. Returns FlareDriverPlugin for the driver JVM
 * and FlareExecutorPlugin for each executor JVM.
 *
 * Register via spark.plugins=io.flare.spark.plugin.FlareSparkPlugin
 * or let the pre-wired spark-defaults.conf in the Docker image handle it.
 */
class FlareSparkPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new FlareDriverPlugin()
  override def executorPlugin(): ExecutorPlugin = new FlareExecutorPlugin()
}
