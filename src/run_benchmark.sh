#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/home/vanh/data/projects/bigdata"
COMPOSE_FILE="/home/vanh/data/projects/bigdata/docker-compose.yaml"
RESULTS_ROOT="$PROJECT_ROOT/results"

mkdir -p \
  "$RESULTS_ROOT/format" \
  "$RESULTS_ROOT/partition" \
  "$RESULTS_ROOT/bucketing" \
  "$RESULTS_ROOT/join_skew"

# Some result files may have been created by a container user that the host
# user cannot chmod later. Permission fixes are best-effort only; missing
# chmod rights should not block the benchmark runs themselves.
chmod -R 777 "$RESULTS_ROOT" || true

SPARK_SUBMIT=(
  docker compose -f "$COMPOSE_FILE" exec spark-master
  /opt/spark/bin/spark-submit
  --master spark://spark-master:7077
  --conf spark.jars.ivy=/tmp/ivy
  --conf spark.ui.showConsoleProgress=false
  --packages org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.24.6,org.apache.spark:spark-avro_2.13:4.0.1
  /workspace/src/run_benchmark.py
)

WINDOW_START="${WINDOW_START:-2024-01-01 00:00:00}"
WINDOW_END="${WINDOW_END:-2024-02-01 00:00:00}"

for FORMAT in parquet orc avro; do
  "${SPARK_SUBMIT[@]}" \
    --bucket taxi-data \
    --prefix "bench/${FORMAT}/raw" \
    --source-format "$FORMAT" \
    --query-group format \
    --benchmark-group format \
    --storage-format "$FORMAT" \
    --layout raw \
    --dataset-variant "trajectory_${FORMAT}_raw" \
    --window-start "$WINDOW_START" \
    --window-end "$WINDOW_END" \
    --zone-lookup-path /workspace/reference/taxi_zone_lookup.csv \
    --output "/workspace/results/format/trajectory_${FORMAT}_format_benchmark.csv"
done

"${SPARK_SUBMIT[@]}" \
  --bucket taxi-data \
  --prefix bench/parquet/partitioned_year_month \
  --source-format parquet \
  --query-group partition \
  --benchmark-group partition \
  --storage-format parquet \
  --layout partitioned_year_month \
  --dataset-variant trajectory_parquet_partitioned_year_month \
  --window-start "$WINDOW_START" \
  --window-end "$WINDOW_END" \
  --zone-lookup-path /workspace/reference/taxi_zone_lookup.csv \
  --output /workspace/results/partition/trajectory_partition_benchmark.csv

"${SPARK_SUBMIT[@]}" \
  --bucket taxi-data \
  --prefix bench/parquet/raw \
  --source-format parquet \
  --query-group bucket_layout \
  --benchmark-group bucketing \
  --storage-format parquet \
  --layout raw \
  --dataset-variant trajectory_parquet_raw_bucket_compare \
  --window-start "$WINDOW_START" \
  --window-end "$WINDOW_END" \
  --zone-lookup-path /workspace/reference/taxi_zone_lookup.csv \
  --output /workspace/results/bucketing/trajectory_bucket_layout_raw_benchmark.csv

"${SPARK_SUBMIT[@]}" \
  --bucket taxi-data \
  --prefix bench/parquet/bucketed_origin_zone_hash \
  --source-format parquet \
  --query-group bucket_layout \
  --benchmark-group bucketing \
  --storage-format parquet \
  --layout bucketed_origin_zone_hash \
  --dataset-variant trajectory_parquet_bucketed_origin_zone_hash \
  --window-start "$WINDOW_START" \
  --window-end "$WINDOW_END" \
  --zone-lookup-path /workspace/reference/taxi_zone_lookup.csv \
  --output /workspace/results/bucketing/trajectory_bucket_layout_bucketed_benchmark.csv

"${SPARK_SUBMIT[@]}" \
  --bucket taxi-data \
  --prefix bench/parquet/raw \
  --source-format parquet \
  --query-group join_skew \
  --benchmark-group join_skew \
  --storage-format parquet \
  --layout raw \
  --dataset-variant trajectory_parquet_join_baseline \
  --window-start "$WINDOW_START" \
  --window-end "$WINDOW_END" \
  --zone-lookup-path /workspace/reference/taxi_zone_lookup.csv \
  --output /workspace/results/join_skew/trajectory_join_skew_benchmark.csv
