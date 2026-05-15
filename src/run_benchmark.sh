#!/usr/bin/env bash
set -euo pipefail

. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

RESULTS_ROOT="${RESULTS_ROOT:-$PROJECT_ROOT/results}"
WINDOW_START="${WINDOW_START:-2025-01-01 00:00:00}"
WINDOW_END="${WINDOW_END:-2026-01-01 00:00:00}"
MINIO_BUCKET="${MINIO_BUCKET:-taxi-data}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://${NODE1_IP:-127.0.0.1}:${MINIO_API_PORT:-9000}}"
SPARK_MASTER="${SPARK_MASTER:-spark://${NODE1_IP:-127.0.0.1}:${SPARK_MASTER_PORT:-7077}}"
SPARK_DRIVER_HOST="${SPARK_DRIVER_HOST:-${NODE1_IP:-127.0.0.1}}"
ZONE_LOOKUP_PATH="${ZONE_LOOKUP_PATH:-$PROJECT_ROOT/dataset/reference/taxi_zone_lookup.csv}"

mkdir -p \
  "$RESULTS_ROOT/format" \
  "$RESULTS_ROOT/partition" \
  "$RESULTS_ROOT/bucketing" \
  "$RESULTS_ROOT/join_skew"

run_benchmark() {
  "$PYTHON_BIN" "$PROJECT_ROOT/src/run_benchmark.py" \
    --bucket "$MINIO_BUCKET" \
    --endpoint "$MINIO_ENDPOINT" \
    --master "$SPARK_MASTER" \
    --driver-host "$SPARK_DRIVER_HOST" \
    --window-start "$WINDOW_START" \
    --window-end "$WINDOW_END" \
    --zone-lookup-path "$ZONE_LOOKUP_PATH" \
    "$@"
}

for FORMAT in parquet orc avro; do
  run_benchmark \
    --prefix "bench/${FORMAT}/raw" \
    --source-format "$FORMAT" \
    --query-group format \
    --benchmark-group format \
    --storage-format "$FORMAT" \
    --layout raw \
    --dataset-variant "trajectory_${FORMAT}_raw" \
    --output "$RESULTS_ROOT/format/trajectory_${FORMAT}_format_benchmark.csv"
done

run_benchmark \
  --prefix bench/parquet/raw \
  --source-format parquet \
  --query-group partition \
  --benchmark-group partition \
  --storage-format parquet \
  --layout raw \
  --dataset-variant trajectory_parquet_raw_partition_compare \
  --output "$RESULTS_ROOT/partition/trajectory_partition_raw_benchmark.csv"

run_benchmark \
  --prefix bench/parquet/partitioned_year_month \
  --source-format parquet \
  --query-group partition \
  --benchmark-group partition \
  --storage-format parquet \
  --layout partitioned_year_month \
  --dataset-variant trajectory_parquet_partitioned_year_month \
  --output "$RESULTS_ROOT/partition/trajectory_partition_benchmark.csv"

run_benchmark \
  --prefix bench/parquet/raw \
  --source-format parquet \
  --query-group bucket_layout \
  --benchmark-group bucketing \
  --storage-format parquet \
  --layout raw \
  --dataset-variant trajectory_parquet_raw_bucket_compare \
  --output "$RESULTS_ROOT/bucketing/trajectory_bucket_layout_raw_benchmark.csv"

run_benchmark \
  --prefix bench/parquet/bucketed_origin_zone_hash \
  --source-format parquet \
  --query-group bucket_layout \
  --benchmark-group bucketing \
  --storage-format parquet \
  --layout bucketed_origin_zone_hash \
  --dataset-variant trajectory_parquet_bucketed_origin_zone_hash \
  --output "$RESULTS_ROOT/bucketing/trajectory_bucket_layout_bucketed_benchmark.csv"

run_benchmark \
  --prefix bench/parquet/raw \
  --source-format parquet \
  --query-group join_skew \
  --benchmark-group join_skew \
  --storage-format parquet \
  --layout raw \
  --dataset-variant trajectory_parquet_join_baseline \
  --output "$RESULTS_ROOT/join_skew/trajectory_join_skew_benchmark.csv"

"$PYTHON_BIN" "$PROJECT_ROOT/src/generate_evidence.py" \
  --results-root "$RESULTS_ROOT" \
  --update-docs
