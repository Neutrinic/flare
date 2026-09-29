"""Many small jobs in one application, so any per-job cost Flare adds on the driver shows up.

    spark-submit many_jobs.py --jobs 2000 --result result.json

Each job is a DataFrame count over a small cached table, the shape of a notebook or an ETL loop
firing one action after another. Every count is its own SQL execution with its own plan, so this is
also the heaviest case for the driver-side listener and SQL plan capture. A warm-up job runs first,
as in many_tasks.py.
"""
import argparse
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

parser = argparse.ArgumentParser()
parser.add_argument("--jobs", type=int, required=True)
parser.add_argument("--result", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName(f"flare-bench many-jobs {args.jobs}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.range(0, 10000, 1, 4).cache()
df.count()

started = time.time()
for i in range(args.jobs):
    df.filter(col("id") % 7 == i % 7).count()
seconds = time.time() - started

with open(args.result, "w") as fh:
    json.dump({"app_id": spark.sparkContext.applicationId, "jobs": args.jobs, "seconds": seconds}, fh)
spark.stop()
