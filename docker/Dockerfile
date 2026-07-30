FROM registry.access.redhat.com/ubi8/ubi-minimal:8.10
LABEL maintainer="Flare"

ARG SPARK_VERSION=4.0.0
ARG SCALA_VERSION=2.13
ARG JDK_VERSION=21
ARG OTEL_AGENT_VERSION=2.30.0

# ── System dependencies ────────────────────────────────────────────────────────
# No Python, no Jupyter, no Almond — Flare is JVM-only
RUN microdnf update -y && \
    microdnf install -y \
      java-${JDK_VERSION}-openjdk-devel \
      tar gzip curl procps && \
    microdnf clean all

# ── Environment ────────────────────────────────────────────────────────────────
ENV JAVA_HOME=/usr/lib/jvm/java-${JDK_VERSION}-openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Java 21 module access required by Spark + Arrow
ENV JAVA_OPTS="\
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED"
ENV SPARK_DAEMON_JAVA_OPTS="$JAVA_OPTS"
ENV SPARK_SUBMIT_OPTS="$JAVA_OPTS"

# ── Spark ──────────────────────────────────────────────────────────────────────
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | \
    tar -xz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

RUN mkdir -p \
    ${SPARK_HOME}/logs \
    ${SPARK_HOME}/work \
    ${SPARK_HOME}/spark-events \
    /opt/flare

# ── OTEL agent (pinned to match Dependencies.scala) ───────────────────────────
# Both JARs land in /opt/flare so spark-defaults.conf can reference a stable path
RUN curl -L \
    https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${OTEL_AGENT_VERSION}/opentelemetry-javaagent.jar \
    -o /opt/flare/opentelemetry-javaagent.jar

# ── Flare extension JAR ────────────────────────────────────────────────────────
# Stage the assembly JAR into docker/ before building:
#   sbt assembly
#   cp target/scala-2.13/flare-spark.jar docker/
COPY flare-spark.jar /opt/flare/flare-spark.jar

# ── Spark config ───────────────────────────────────────────────────────────────
# spark-defaults.conf pre-wires both driver and executor extraJavaOptions.
# Users get traces with zero additional configuration.
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# ── Examples JAR (optional, built separately) ──────────────────────────────────
# Stage into docker/ before building:
#   cp examples/target/scala-2.13/flare-examples.jar docker/
COPY flare-examples.jar /opt/flare/flare-examples.jar

# ── Entrypoint ─────────────────────────────────────────────────────────────────
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 8080: Master WebUI
# 7077: Master port
# 8081: Worker WebUI
# 18080: History Server
# 4040: Application UI (driver)
EXPOSE 8080 7077 8081 18080 4040

VOLUME ["/opt/spark/logs", "/opt/spark/work", "/opt/spark/spark-events"]

ENTRYPOINT ["/entrypoint.sh"]
