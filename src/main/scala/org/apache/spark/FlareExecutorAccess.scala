package org.apache.spark

/**
 * The executors a SparkContext has already registered, for a listener added after they were
 * announced (#224).
 *
 * The SparkContext advice registers Flare's listener as the constructor returns, and by then the
 * startup executors have been announced: local mode's one executor, and on YARN and Kubernetes
 * most of the cluster, since the constructor waits for `spark.scheduler.minRegisteredResourcesRatio`
 * of them. `SparkContext.getExecutorIds` is `private[spark]`, hence this object in Spark's package.
 *
 * The coarse-grained scheduler backend (standalone, YARN, Kubernetes) puts an executor in its map
 * before posting `SparkListenerExecutorAdded`, and `getExecutorIds` reads that map. So, read after
 * the listener is added, this misses no executor whose event the listener also missed. Local mode's
 * backend keeps no such list, and its one executor is always the driver.
 */
object FlareExecutorAccess {

  def registeredExecutorIds(sc: SparkContext): Seq[String] =
    if (sc.isLocal) Seq(SparkContext.DRIVER_IDENTIFIER)
    else sc.schedulerBackend match {
      case client: ExecutorAllocationClient => client.getExecutorIds()
      case _                                 => Nil
    }
}
