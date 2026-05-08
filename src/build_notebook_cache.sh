#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/home/phd/bigdata-spark-query-optimization}"
PYTHON_BIN="${PYTHON_BIN:-$PROJECT_ROOT/venv/bin/python}"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="${PYTHON_BIN_FALLBACK:-python3.12}"
fi
if [ -z "${JAVA_HOME:-}" ] && [ -x "$HOME/.local/share/jdks/temurin-21/bin/java" ]; then
  export JAVA_HOME="$HOME/.local/share/jdks/temurin-21"
  export PATH="$JAVA_HOME/bin:$PATH"
fi

export PYTHONPATH="$PROJECT_ROOT/src:${PYTHONPATH:-}"

"$PYTHON_BIN" "$PROJECT_ROOT/src/build_notebook_cache.py" \
  --bucket "${MINIO_BUCKET:-taxi-data}" \
  --endpoint "${MINIO_ENDPOINT:-http://100.127.42.127:9100}" \
  --master "${SPARK_MASTER:-spark://100.127.42.127:7077}" \
  --driver-host "${SPARK_DRIVER_HOST:-100.127.42.127}" \
  --prefixes ${CACHE_PREFIXES:-2025}
