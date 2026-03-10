#!/usr/bin/env bash
# Compiles (if needed) and runs JMH benchmarks, passing all arguments through.
#
# Usage:
#   ./lucene/benchmark-jmh/run-benchmark.sh ScoreDocSortBenchmark -rf json -rff results.json
#
# This ensures you never accidentally run stale bytecode.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

: "${JAVA_HOME:=/usr/lib/jvm/java-25-openjdk}"
export JAVA_HOME
export JAVA25_HOME="$JAVA_HOME"
export RUNTIME_JAVA_HOME="$JAVA_HOME"

echo "=== Compiling benchmarks ===" >&2
JAVA_HOME="$JAVA_HOME" "$ROOT_DIR/gradlew" -p "$ROOT_DIR" :lucene:benchmark-jmh:assemble --quiet

echo "=== Running JMH ===" >&2
exec "$JAVA_HOME/bin/java" \
  --module-path "$ROOT_DIR/lucene/benchmark-jmh/build/benchmarks" \
  --module org.apache.lucene.benchmark.jmh \
  "$@"
