"""One stage of N near-empty tasks, so any per-task cost Flare adds shows up in the wall time.

    spark-submit many_tasks.py --tasks 10000 --result result.json

Each task writes 100 rows to the `noop` sink: JVM only, no shuffle, no Python workers. A small
warm-up job runs first so JVM and code-generation start-up is not billed to the measured stage.
"""
import argparse
import json
import time

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--tasks", type=int, required=True)
parser.add_argument("--rows-per-task", type=int, default=100)
parser.add_argument("--result", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName(f"flare-bench many-tasks {args.tasks}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.range(0, 1000, 1, 10).write.format("noop").mode("overwrite").save()

started = time.time()
(spark.range(0, args.tasks * args.rows_per_task, 1, args.tasks)
    .write.format("noop").mode("overwrite").save())
seconds = time.time() - started

with open(args.result, "w") as fh:
    json.dump({"app_id": spark.sparkContext.applicationId, "tasks": args.tasks,
               "seconds": seconds}, fh)
spark.stop()
