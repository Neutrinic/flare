#!/bin/bash
set -e

SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"

# Resolve the container hostname reliably.
# The `hostname` command is not installed in the slim Spark base image,
# so fall back to $HOSTNAME (set by bash) or /etc/hostname.
_HOST="${HOSTNAME:-$(cat /etc/hostname 2>/dev/null || echo 0.0.0.0)}"

# Allow command-line argument to override SPARK_ROLE env var.
# This enables: docker compose run spark-master run skewed
if [[ -n "$1" ]]; then
  SPARK_ROLE="$1"
fi
SPARK_ROLE="${SPARK_ROLE:-master}"

# Allow FLARE_* env vars to override spark-defaults.conf values at runtime
# by appending them as system properties to extraJavaOptions.
# This avoids having to rebuild the image to change granularity or sampling.
_flare_jvm_opts=""
[[ -n "${FLARE_TRACE_GRANULARITY}" ]] && _flare_jvm_opts="$_flare_jvm_opts -DFLARE_TRACE_GRANULARITY=${FLARE_TRACE_GRANULARITY}"
[[ -n "${FLARE_SAMPLING_RATIO}" ]]    && _flare_jvm_opts="$_flare_jvm_opts -DFLARE_SAMPLING_RATIO=${FLARE_SAMPLING_RATIO}"
[[ -n "${FLARE_MAX_SPANS_PER_TRACE}" ]] && _flare_jvm_opts="$_flare_jvm_opts -DFLARE_MAX_SPANS_PER_TRACE=${FLARE_MAX_SPANS_PER_TRACE}"
[[ -n "${FLARE_ENABLED}" ]]           && _flare_jvm_opts="$_flare_jvm_opts -DFLARE_ENABLED=${FLARE_ENABLED}"

export SPARK_DAEMON_JAVA_OPTS="${SPARK_DAEMON_JAVA_OPTS} ${_flare_jvm_opts}"

case "${SPARK_ROLE}" in

  master)
    echo "[Flare] Starting Spark master on ${_HOST}"
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host "${_HOST}" \
      --port 7077 \
      --webui-port 8080
    ;;

  worker)
    echo "[Flare] Starting Spark worker → ${SPARK_MASTER_URL}"
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --webui-port 8081 \
      "${SPARK_MASTER_URL}"
    ;;

  history)
    echo "[Flare] Starting Spark history server"
    export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/opt/spark/spark-events"
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.history.HistoryServer
    ;;

  # Run a named example job and exit.
  # Usage: docker run flare-spark run <example>
  # Examples: simple, skewed, pipeline
  run)
    EXAMPLE="${2:-simple}"
    MAIN_CLASS=""
    case "${EXAMPLE}" in
      simple)   MAIN_CLASS="io.flare.examples.SimpleJob"   ;;
      skewed)   MAIN_CLASS="io.flare.examples.SkewedJob"   ;;
      pipeline) MAIN_CLASS="io.flare.examples.PipelineJob" ;;
      *)
        echo "Unknown example: ${EXAMPLE}. Available: simple, skewed, pipeline"
        exit 1
        ;;
    esac

    echo "[Flare] Running example: ${EXAMPLE} (${MAIN_CLASS})"
    exec "${SPARK_HOME}/bin/spark-submit" \
      --master "${SPARK_MASTER_URL}" \
      --class "${MAIN_CLASS}" \
      /opt/flare/flare-examples.jar
    ;;

  *)
    # Pass through arbitrary spark-submit or spark-class invocations
    echo "[Flare] Executing: $*"
    exec "$@"
    ;;

esac
