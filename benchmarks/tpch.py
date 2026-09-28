"""One pass of the 22 TPC-H queries, as a realistic mix of scans, joins, shuffles and spills.

    spark-submit tpch.py --data /path/to/sf5 --queries /path/to/queries --result result.json

--data holds one Parquet directory per table; --queries holds q1.sql to q22.sql. Both come from
tpch_generate.py. Every executor reads --data from its local filesystem, so on a cluster without
shared storage the directory must exist at the same path on every node.
"""
import argparse
import json
import time
from pathlib import Path

from pyspark.sql import SparkSession

TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]

parser = argparse.ArgumentParser()
parser.add_argument("--data", type=Path, required=True)
parser.add_argument("--queries", type=Path, required=True)
parser.add_argument("--result", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName("flare-bench tpch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
for table in TABLES:
    spark.read.parquet(str(args.data / table)).createOrReplaceTempView(table)

per_query = {}
started = time.time()
for number in range(1, 23):
    t = time.time()
    rows = spark.sql((args.queries / f"q{number}.sql").read_text()).collect()
    per_query[number] = {"seconds": time.time() - t, "rows": len(rows)}
seconds = time.time() - started

with open(args.result, "w") as fh:
    json.dump({"app_id": spark.sparkContext.applicationId, "seconds": seconds,
               "queries": per_query}, fh)
spark.stop()
