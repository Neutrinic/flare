"""Generate TPC-H data as Parquet, and the 22 query texts, with DuckDB's tpch extension.

    pip install duckdb
    python tpch_generate.py --scale-factor 5 --out /path/to/tpch

Writes <out>/sf<N>/<table>/*.parquet and <out>/queries/q1.sql .. q22.sql. Copy the result to the
same path on every node that runs executors. This is not an audited TPC-H setup; it is a fixed,
reproducible query mix.
"""
import argparse
from pathlib import Path

import duckdb

TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]

parser = argparse.ArgumentParser()
parser.add_argument("--scale-factor", type=int, default=5)
parser.add_argument("--out", type=Path, required=True)
parser.add_argument("--memory-limit", default="2GB")
parser.add_argument("--copy-to", type=Path, help="copy the output here when done, e.g. a volume")
parser.add_argument("--database", default=":memory:", help="a file here lets large scale factors spill to disk")
args = parser.parse_args()

data = args.out / f"sf{args.scale_factor}"
queries = args.out / "queries"
data.mkdir(parents=True, exist_ok=True)
queries.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(args.database)
con.execute(f"SET memory_limit='{args.memory_limit}'")
con.execute("INSTALL tpch; LOAD tpch;")
con.execute(f"CALL dbgen(sf={args.scale_factor})")
for table in TABLES:
    con.execute(f"COPY {table} TO '{data / table}' "
                "(FORMAT PARQUET, COMPRESSION SNAPPY, PER_THREAD_OUTPUT true, ROW_GROUP_SIZE 122880)")
for number, text in con.execute("SELECT query_nr, query FROM tpch_queries()").fetchall():
    (queries / f"q{number}.sql").write_text(text)
print(f"wrote {data} and {queries}")
if args.copy_to:
    import shutil
    shutil.copytree(args.out / f"sf{args.scale_factor}", args.copy_to / f"sf{args.scale_factor}", dirs_exist_ok=True)
    shutil.copytree(queries, args.copy_to / "queries", dirs_exist_ok=True)
    print(f"copied to {args.copy_to}")
