#!/usr/bin/env bash
set -euo pipefail

WINDOW_START="${WINDOW_START:-2024-01-01 00:00:00}"
WINDOW_END="${WINDOW_END:-2024-02-01 00:00:00}"

docker compose -f /home/vanh/data/projects/bigdata/docker-compose.yaml exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.ui.showConsoleProgress=false \
  --packages org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.24.6,org.apache.spark:spark-avro_2.13:4.0.1 \
  /workspace/src/prepare_benchmark_datasets.py \
  --source-bucket taxi-data \
  --source-prefix '' \
  --source-format parquet \
  --target-bucket taxi-data \
  --target-prefix bench \
  --zone-lookup-path /workspace/reference/taxi_zone_lookup.csv \
  --window-start "$WINDOW_START" \
  --window-end "$WINDOW_END" \
  --merge-schema
