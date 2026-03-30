#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/home/vanh/data/projects/bigdata"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yaml"

docker compose -f "$COMPOSE_FILE" exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.ui.showConsoleProgress=false \
  --packages org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.24.6,org.apache.spark:spark-avro_2.13:4.0.1 \
  /workspace/src/build_notebook_cache.py \
  --bucket taxi-data \
  --prefixes 2021 2022 2023 2024 2025
