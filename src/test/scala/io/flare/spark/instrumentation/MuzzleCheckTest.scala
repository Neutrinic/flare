package io.flare.spark.instrumentation

import munit.FunSuite

/**
 * Muzzle-style checks: verify that all ByteBuddy instrumentation targets exist in the
 * current Spark version on the classpath.
 *
 * The OTEL muzzle framework is Gradle-only. Since Flare uses sbt, these reflection-based
 * tests serve the same purpose: they fail the build if a target class, method, or field
 * is missing or renamed in any Spark version.
 *
 * Runs automatically in the CI matrix (Spark 3.3–4.0 x Scala 2.12/2.13).
 *
 * Each test mirrors the exact target used in the corresponding TypeInstrumentation or
 * AdviceHelper, so a failure here means the instrumentation would fail at runtime.
 */
class MuzzleCheckTest extends FunSuite {

  // ── SparkContextInstrumentation ─────────────────────────────────────────────
  // Target: org.apache.spark.SparkContext constructor
  // Risk: LOW — public API, stable since Spark 1.x

  test("SparkContext class exists") {
    val clazz = Class.forName("org.apache.spark.SparkContext")
    assert(clazz != null, "SparkContext class must exist")
  }

  test("SparkContext has a constructor") {
    val clazz = Class.forName("org.apache.spark.SparkContext")
    val constructors = clazz.getDeclaredConstructors
    assert(constructors.nonEmpty, "SparkContext must have at least one constructor")
  }

  // ── TaskRunnerInstrumentation ───────────────────────────────────────────────
  // Target: org.apache.spark.executor.Executor$TaskRunner.run()
  // Risk: HIGH — private[spark] inner class
  // Used by: TaskRunnerInstrumentation.java (typeMatcher + transform)
  //          TaskRunnerAdvice.java (@Advice.FieldValue("taskDescription"))

  test("Executor$TaskRunner class exists") {
    val clazz = Class.forName("org.apache.spark.executor.Executor$TaskRunner")
    assert(clazz != null, "Executor$TaskRunner must exist")
  }

  test("Executor$TaskRunner has run() method with no arguments") {
    val clazz = Class.forName("org.apache.spark.executor.Executor$TaskRunner")
    val methods = clazz.getDeclaredMethods
    val runMethod = methods.find(m => m.getName == "run" && m.getParameterCount == 0)
    assert(runMethod.isDefined,
      s"TaskRunner must have run() with no args. Found methods: ${methods.map(_.getName).mkString(", ")}")
  }

  test("Executor$TaskRunner has taskDescription field") {
    // TaskRunnerAdvice uses @Advice.FieldValue("taskDescription") to read this field.
    // Scala compiler may mangle the name, so we check both exact and contains.
    val clazz = Class.forName("org.apache.spark.executor.Executor$TaskRunner")
    val fields = clazz.getDeclaredFields
    val found = fields.exists(f =>
      f.getName == "taskDescription" || f.getName.contains("taskDescription"))
    assert(found,
      s"TaskRunner must have taskDescription field. Found fields: ${fields.map(_.getName).mkString(", ")}")
  }

  // ── SubmitMissingTasksInstrumentation ───────────────────────────────────────
  // Target: org.apache.spark.scheduler.DAGScheduler.submitMissingTasks(stage, jobId)
  // Risk: HIGH — private[scheduler] method
  // Used by: SubmitMissingTasksInstrumentation.java (typeMatcher + transform)

  test("DAGScheduler class exists") {
    val clazz = Class.forName("org.apache.spark.scheduler.DAGScheduler")
    assert(clazz != null, "DAGScheduler must exist")
  }

  test("DAGScheduler has submitMissingTasks method") {
    val clazz = Class.forName("org.apache.spark.scheduler.DAGScheduler")
    val methods = clazz.getDeclaredMethods
    val found = methods.exists(_.getName == "submitMissingTasks")
    assert(found,
      s"DAGScheduler must have submitMissingTasks method. " +
      s"Found methods containing 'submit': ${methods.filter(_.getName.contains("submit")).map(_.getName).mkString(", ")}")
  }

  // ── SubmitMissingTasksAdviceHelper reflection targets ───────────────────────
  // These are accessed via reflection in SubmitMissingTasksAdviceHelper.ensureReflection()

  test("DAGScheduler has jobIdToActiveJob field") {
    // SubmitMissingTasksAdviceHelper accesses this private field via reflection.
    // Scala may mangle the name, so fuzzy matching is used (same as production code).
    val clazz = Class.forName("org.apache.spark.scheduler.DAGScheduler")
    val fields = clazz.getDeclaredFields
    val found = fields.exists(f =>
      f.getName == "jobIdToActiveJob" || f.getName.contains("jobIdToActiveJob"))
    assert(found,
      s"DAGScheduler must have jobIdToActiveJob field. " +
      s"Found fields containing 'job': ${fields.filter(_.getName.toLowerCase.contains("job")).map(_.getName).mkString(", ")}")
  }

  test("Stage class exists and has id method") {
    // SubmitMissingTasksAdviceHelper calls Stage.id() via reflection.
    val clazz = Class.forName("org.apache.spark.scheduler.Stage")
    val method = clazz.getMethod("id")
    assert(method != null, "Stage must have public id() method")
    assert(method.getReturnType == classOf[Int],
      s"Stage.id() must return Int, got ${method.getReturnType}")
  }

  test("ActiveJob class exists and has properties method") {
    // SubmitMissingTasksAdviceHelper calls ActiveJob.properties() via reflection
    // to get java.util.Properties for traceparent injection.
    val clazz = Class.forName("org.apache.spark.scheduler.ActiveJob")
    val method = clazz.getMethod("properties")
    assert(method != null, "ActiveJob must have public properties() method")
    assert(classOf[java.util.Properties].isAssignableFrom(method.getReturnType),
      s"ActiveJob.properties() must return java.util.Properties, got ${method.getReturnType}")
  }

  // ── TaskRunnerAdviceHelper reflection target ────────────────────────────────
  // TaskRunnerAdviceHelper.getProperties() accesses TaskDescription.properties()

  test("TaskDescription class exists and has properties method") {
    // TaskRunnerAdviceHelper calls taskDescription.properties() via reflection
    // to extract W3C traceparent from the serialized task properties.
    val clazz = Class.forName("org.apache.spark.scheduler.TaskDescription")
    val method = clazz.getMethod("properties")
    assert(method != null, "TaskDescription must have public properties() method")
    assert(classOf[java.util.Properties].isAssignableFrom(method.getReturnType),
      s"TaskDescription.properties() must return java.util.Properties, got ${method.getReturnType}")
  }

  // ── ExecutorPlugin API (used by FlareExecutorPlugin) ────────────────────────
  // Risk: LOW — public API since Spark 3.0

  test("ExecutorPlugin interface exists") {
    val clazz = Class.forName("org.apache.spark.api.plugin.ExecutorPlugin")
    assert(clazz != null, "ExecutorPlugin must exist")
    assert(clazz.isInterface, "ExecutorPlugin must be an interface")
  }

  test("SparkPlugin interface exists") {
    val clazz = Class.forName("org.apache.spark.api.plugin.SparkPlugin")
    assert(clazz != null, "SparkPlugin must exist")
    assert(clazz.isInterface, "SparkPlugin must be an interface")
  }

  // ── TaskContext API (used by FlareExecutorPlugin for context extraction) ────
  // Risk: LOW — public API

  test("TaskContext has getLocalProperty method") {
    val clazz = Class.forName("org.apache.spark.TaskContext")
    val method = clazz.getMethod("getLocalProperty", classOf[String])
    assert(method != null, "TaskContext must have getLocalProperty(String)")
    assert(method.getReturnType == classOf[String],
      s"TaskContext.getLocalProperty() must return String, got ${method.getReturnType}")
  }

  test("TaskContext has stageId method") {
    val clazz = Class.forName("org.apache.spark.TaskContext")
    val method = clazz.getMethod("stageId")
    assert(method != null, "TaskContext must have stageId()")
    assert(method.getReturnType == classOf[Int],
      s"TaskContext.stageId() must return Int, got ${method.getReturnType}")
  }

  test("TaskContext has partitionId method") {
    val clazz = Class.forName("org.apache.spark.TaskContext")
    val method = clazz.getMethod("partitionId")
    assert(method != null, "TaskContext must have partitionId()")
    assert(method.getReturnType == classOf[Int],
      s"TaskContext.partitionId() must return Int, got ${method.getReturnType}")
  }

  test("TaskContext has attemptNumber method") {
    val clazz = Class.forName("org.apache.spark.TaskContext")
    val method = clazz.getMethod("attemptNumber")
    assert(method != null, "TaskContext must have attemptNumber()")
    assert(method.getReturnType == classOf[Int],
      s"TaskContext.attemptNumber() must return Int, got ${method.getReturnType}")
  }
}
