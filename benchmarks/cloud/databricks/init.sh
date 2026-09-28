#!/bin/bash
# Databricks init script for the overhead benchmark. Every node: the agent and Flare at fixed paths,
# and an agent configuration file sending OTLP to a sink on the driver. The driver: that sink.
# BENCH is the volume directory holding flare-spark.jar and otlp_sink.py.
set -euo pipefail
BENCH=/Volumes/aws/default/flare/bench
AGENT_URL=https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/2.30.0/opentelemetry-javaagent-2.30.0.jar

mkdir -p /opt/flare
wget -q -O /opt/flare/opentelemetry-javaagent.jar "$AGENT_URL"
cp $BENCH/flare-spark.jar /opt/flare/flare-spark.jar
cp /opt/flare/flare-spark.jar /databricks/jars/zz-flare-spark.jar
{
  echo "otel.exporter.otlp.endpoint=http://${DB_DRIVER_IP}:4318"
  echo "otel.exporter.otlp.protocol=http/protobuf"
  echo "otel.exporter.otlp.compression=gzip"
  echo "otel.logs.exporter=none"
} > /opt/flare/otel.properties

if [ "${DB_IS_DRIVER:-}" = "TRUE" ]; then
  cp $BENCH/otlp_sink.py /opt/flare/otlp_sink.py
  PY=$(command -v /databricks/python3/bin/python3 || command -v python3)
  setsid nohup "$PY" /opt/flare/otlp_sink.py --port 4318 > /tmp/otlp_sink.log 2>&1 < /dev/null &
fi
echo "flare-bench-init: done on $(hostname), driver ${DB_DRIVER_IP}"
