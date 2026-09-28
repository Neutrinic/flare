#!/bin/bash
# Dataproc initialization action for the overhead benchmark: the agent and Flare on every node at
# fixed paths; on the master, the harness and what it needs. BUCKET is the cluster metadata key
# flare-bench-bucket.
set -euo pipefail
BUCKET=$(/usr/share/google/get_metadata_value attributes/flare-bench-bucket)
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
AGENT_URL=https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/2.30.0/opentelemetry-javaagent-2.30.0.jar

mkdir -p /opt/flare
wget -q -O /opt/flare/opentelemetry-javaagent.jar "$AGENT_URL"
gsutil -q cp "gs://$BUCKET/bench/flare-spark.jar" /opt/flare/flare-spark.jar

if [ "$ROLE" = "Master" ]; then
  mkdir -p /opt/flare/bench
  gsutil -q cp "gs://$BUCKET/bench/scripts/*" /opt/flare/bench/
  chmod +x /opt/flare/bench/*.sh
  apt-get install -y -qq time > /dev/null
  /opt/conda/default/bin/pip install -q duckdb
fi
