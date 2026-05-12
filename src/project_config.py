from __future__ import annotations

import os
from pathlib import Path


DETECTED_PROJECT_ROOT = Path(__file__).resolve().parents[1]

def load_env_file(env_path: Path) -> bool:
    if not env_path.exists():
        return False

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]

        os.environ[key] = value

    return True


load_env_file(DETECTED_PROJECT_ROOT / ".env")

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", str(DETECTED_PROJECT_ROOT))).expanduser()

DEFAULT_NODE1_IP = os.environ.get("NODE1_IP", "127.0.0.1")
DEFAULT_SPARK_MASTER_PORT = os.environ.get("SPARK_MASTER_PORT", "7077")
DEFAULT_MINIO_API_PORT = os.environ.get("MINIO_API_PORT", "9000")

DEFAULT_SPARK_MASTER = os.environ.get(
    "SPARK_MASTER",
    f"spark://{DEFAULT_NODE1_IP}:{DEFAULT_SPARK_MASTER_PORT}",
)
DEFAULT_SPARK_DRIVER_HOST = os.environ.get("SPARK_DRIVER_HOST", DEFAULT_NODE1_IP)
DEFAULT_MINIO_ENDPOINT = os.environ.get(
    "MINIO_ENDPOINT",
    f"http://{DEFAULT_NODE1_IP}:{DEFAULT_MINIO_API_PORT}",
)
DEFAULT_MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "taxi-data")
DEFAULT_MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
DEFAULT_MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

DEFAULT_ZONE_LOOKUP_PATH = os.environ.get(
    "ZONE_LOOKUP_PATH",
    str(PROJECT_ROOT / "dataset/reference/taxi_zone_lookup.csv"),
)
DEFAULT_RESULTS_ROOT = Path(os.environ.get("RESULTS_ROOT", str(PROJECT_ROOT / "results")))

DEFAULT_SPARK_PACKAGES = ",".join(
    [
        "org.apache.hadoop:hadoop-aws:3.4.1",
        "software.amazon.awssdk:bundle:2.24.6",
        "org.apache.spark:spark-avro_2.13:4.0.1",
    ]
)
DEFAULT_SPARK_IVY_DIR = os.environ.get("SPARK_IVY_DIR", str(Path.home() / ".ivy2"))
