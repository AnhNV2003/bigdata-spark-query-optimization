#!/usr/bin/env bash
set -euo pipefail

. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

"$PYTHON_BIN" "$PROJECT_ROOT/src/build_notebook_cache.py" \
  --bucket "${MINIO_BUCKET:-taxi-data}" \
  --endpoint "${MINIO_ENDPOINT:-http://${NODE1_IP:-127.0.0.1}:${MINIO_API_PORT:-9000}}" \
  --master "${SPARK_MASTER:-spark://${NODE1_IP:-127.0.0.1}:${SPARK_MASTER_PORT:-7077}}" \
  --driver-host "${SPARK_DRIVER_HOST:-${NODE1_IP:-127.0.0.1}}" \
  --prefixes ${CACHE_PREFIXES:-2025}
