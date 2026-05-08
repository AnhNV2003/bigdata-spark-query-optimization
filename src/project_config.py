from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://100.127.42.127:7077")
DEFAULT_SPARK_DRIVER_HOST = os.environ.get("SPARK_DRIVER_HOST", "100.127.42.127")
DEFAULT_MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://100.127.42.127:9000")
DEFAULT_MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "taxi-data")
DEFAULT_MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
DEFAULT_MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

DEFAULT_ZONE_LOOKUP_PATH = str(PROJECT_ROOT / "dataset/reference/taxi_zone_lookup.csv")
DEFAULT_RESULTS_ROOT = PROJECT_ROOT / "results"

DEFAULT_SPARK_PACKAGES = ",".join(
    [
        "org.apache.hadoop:hadoop-aws:3.4.1",
        "software.amazon.awssdk:bundle:2.24.6",
        "org.apache.spark:spark-avro_2.13:4.0.1",
    ]
)
DEFAULT_SPARK_IVY_DIR = str(Path.home() / ".ivy2")
