#!/usr/bin/env bash
# Build one assembly JAR per Scala version for a Spark version, named like its Maven coordinate.
#
#   build-assemblies.sh <spark-version> <label> <out-dir>
#
# Shared by release.yml (label = the version) and dev.yml (label = dev). release.yml only runs on
# a version tag, so on its own this logic would go almost untested; dev.yml runs it on every push
# to main, which exercises exactly the path a release depends on.
set -euo pipefail

SPARK="$1"
LABEL="$2"
OUT="$3"

MM="${SPARK%.*}"        # 3.5.1 -> 3.5
COORD="${MM//./-}"      # 3.5   -> 3-5, matching the Central coordinate
mkdir -p "$OUT"

# Mirrors crossScalaVersions in build.sbt: Spark 4.x is Scala 2.13 only. Kept in step with that
# definition by the count check below rather than by memory.
case "$MM" in
  4.*) SCALAS="2.13.16";         EXPECTED=1 ;;
  *)   SCALAS="2.12.18 2.13.16"; EXPECTED=2 ;;
esac

for SC in $SCALAS; do
  BIN="${SC%.*}"        # 2.13.16 -> 2.13
  sbt -DsparkVersion="$SPARK" "++$SC" assembly
  cp "target/scala-$BIN/flare-spark-$MM.jar" "$OUT/flare-spark-${COORD}_${BIN}-${LABEL}.jar"
done

ACTUAL=$(find "$OUT" -maxdepth 1 -name '*.jar' | wc -l)
if [ "$ACTUAL" -ne "$EXPECTED" ]; then
  echo "expected $EXPECTED assembly JARs for Spark $SPARK, got $ACTUAL" >&2
  exit 1
fi
ls -l "$OUT"
