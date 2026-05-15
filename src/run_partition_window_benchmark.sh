#!/usr/bin/env bash
set -euo pipefail

. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

RESULTS_ROOT="${RESULTS_ROOT:-$PROJECT_ROOT/results}"
WINDOW_START="${PARTITION_WINDOW_START:-2025-01-01 00:00:00}"
WINDOW_END="${PARTITION_WINDOW_END:-2025-02-01 00:00:00}"
MINIO_BUCKET="${MINIO_BUCKET:-taxi-data}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://${NODE1_IP:-127.0.0.1}:${MINIO_API_PORT:-9000}}"
SPARK_MASTER="${SPARK_MASTER:-spark://${NODE1_IP:-127.0.0.1}:${SPARK_MASTER_PORT:-7077}}"
SPARK_DRIVER_HOST="${SPARK_DRIVER_HOST:-${NODE1_IP:-127.0.0.1}}"
ZONE_LOOKUP_PATH="${ZONE_LOOKUP_PATH:-$PROJECT_ROOT/dataset/reference/taxi_zone_lookup.csv}"

mkdir -p "$RESULTS_ROOT/partition_month"

run_partition_benchmark() {
  "$PYTHON_BIN" "$PROJECT_ROOT/src/run_benchmark.py" \
    --bucket "$MINIO_BUCKET" \
    --endpoint "$MINIO_ENDPOINT" \
    --master "$SPARK_MASTER" \
    --driver-host "$SPARK_DRIVER_HOST" \
    --window-start "$WINDOW_START" \
    --window-end "$WINDOW_END" \
    --zone-lookup-path "$ZONE_LOOKUP_PATH" \
    --source-format parquet \
    --query-group partition \
    --benchmark-group partition_month \
    --storage-format parquet \
    "$@"
}

run_partition_benchmark \
  --prefix bench/parquet/raw \
  --layout raw \
  --dataset-variant trajectory_parquet_raw_partition_month_compare \
  --output "$RESULTS_ROOT/partition_month/trajectory_partition_month_raw_benchmark.csv"

run_partition_benchmark \
  --prefix bench/parquet/partitioned_year_month \
  --layout partitioned_year_month \
  --dataset-variant trajectory_parquet_partitioned_year_month_month_window \
  --output "$RESULTS_ROOT/partition_month/trajectory_partition_month_benchmark.csv"
