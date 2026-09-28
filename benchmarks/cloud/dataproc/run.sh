#!/bin/bash
# The overhead matrix on a Dataproc master, detached. Generates TPC-H into the bucket if it is not
# there, runs run_matrix.py in client mode (the driver on this host), and copies progress and
# results to gs://$BUCKET/bench/out/ every minute and at the end.
#   run.sh <bucket> <scale factor> <minutes> <repeats> <configs>
set -uo pipefail
BUCKET=$1 SF=$2 MINUTES=$3 REPEATS=$4 CONFIGS=$5
export HOME=${HOME:-/root}  # unset when launched detached; DuckDB needs it to install extensions
PY=/opt/conda/default/bin/python
B=/opt/flare/bench W=/mnt/flare-bench OUT=gs://$BUCKET/bench/out
mkdir -p $W && cd $W && rm -f $W/results.jsonl
exec > $W/progress.log 2>&1

sync_out() { gsutil -q -m cp $W/progress.log $W/results.jsonl $W/sink.log "$OUT/" 2>/dev/null; }
( while true; do sleep 60; sync_out; done ) & SYNC=$!

if ! gsutil ls "gs://$BUCKET/tpch/sf$SF/lineitem/" > /dev/null 2>&1; then
  echo "$(date -u +%T) generating TPC-H SF$SF"
  $PY $B/tpch_generate.py --scale-factor $SF --out $W/tpch --database $W/tpch.duckdb --memory-limit 8GB \
    || { echo "generation failed"; kill $SYNC; sync_out; exit 1; }
  gsutil -q -m cp -r $W/tpch/sf$SF "gs://$BUCKET/tpch/"
  gsutil -q -m cp -r $W/tpch/queries "gs://$BUCKET/tpch/"
  rm -rf $W/tpch.duckdb $W/tpch/sf$SF
fi
mkdir -p $W/queries && gsutil -q -m cp "gs://$BUCKET/tpch/queries/*" $W/queries/
echo "$(date -u +%T) data ready"

setsid nohup $PY $B/otlp_sink.py --port 4318 > $W/sink.log 2>&1 < /dev/null &
sleep 2
MASTER_IP=$(hostname -I | awk '{print $1}')
$PY $B/run_matrix.py \
  --spark-submit "spark-submit --master yarn --deploy-mode client --conf spark.dynamicAllocation.enabled=false --num-executors 4 --executor-cores 2 --executor-memory 4g --driver-memory 4g" \
  --agent /opt/flare/opentelemetry-javaagent.jar --flare /opt/flare/flare-spark.jar \
  --sink "http://$MASTER_IP:4318" \
  --event-dir $W/events --out $W/results.jsonl \
  --workloads tpch --tpch-data "gs://$BUCKET/tpch/sf$SF" --tpch-queries $W/queries --tpch-minutes $MINUTES \
  --nodes local --no-warmup --repeats $REPEATS --configs "$CONFIGS"
echo "$(date -u +%T) matrix exit $?"
kill $SYNC; sync_out
echo done | gsutil -q cp - "$OUT/DONE"
