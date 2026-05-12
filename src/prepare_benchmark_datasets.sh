#!/usr/bin/env bash
set -euo pipefail

. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

"$PYTHON_BIN" "$PROJECT_ROOT/src/prepare_benchmark_datasets.py" \
  --source-bucket "${MINIO_BUCKET:-taxi-data}" \
  --source-prefix "${SOURCE_PREFIX:-}" \
  --source-format "${SOURCE_FORMAT:-parquet}" \
  --target-bucket "${MINIO_BUCKET:-taxi-data}" \
  --target-prefix "${TARGET_PREFIX:-bench}" \
  --endpoint "${MINIO_ENDPOINT:-http://${NODE1_IP:-127.0.0.1}:${MINIO_API_PORT:-9000}}" \
  --master "${SPARK_MASTER:-spark://${NODE1_IP:-127.0.0.1}:${SPARK_MASTER_PORT:-7077}}" \
  --driver-host "${SPARK_DRIVER_HOST:-${NODE1_IP:-127.0.0.1}}" \
  --zone-lookup-path "${ZONE_LOOKUP_PATH:-$PROJECT_ROOT/dataset/reference/taxi_zone_lookup.csv}" \
  --window-start "${WINDOW_START:-2025-01-01 00:00:00}" \
  --window-end "${WINDOW_END:-2026-01-01 00:00:00}" \
  --merge-schema
