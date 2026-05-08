from __future__ import annotations

import argparse

from pyspark.sql.functions import col, count, when

from project_config import (
    DEFAULT_MINIO_ACCESS_KEY,
    DEFAULT_MINIO_BUCKET,
    DEFAULT_MINIO_ENDPOINT,
    DEFAULT_MINIO_SECRET_KEY,
    DEFAULT_SPARK_DRIVER_HOST,
    DEFAULT_SPARK_MASTER,
)
from test_read_minio_parquet import build_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute full-dataset null ratios for selected columns in parquet stored on MinIO."
    )
    parser.add_argument("--bucket", default=DEFAULT_MINIO_BUCKET, help="MinIO bucket name")
    parser.add_argument(
        "--prefix",
        default="",
        help="Object prefix inside the bucket. Use trailing slash for a folder-like prefix.",
    )
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_MINIO_ENDPOINT,
        help="MinIO S3 endpoint visible from Spark",
    )
    parser.add_argument("--access-key", default=DEFAULT_MINIO_ACCESS_KEY, help="MinIO access key")
    parser.add_argument("--secret-key", default=DEFAULT_MINIO_SECRET_KEY, help="MinIO secret key")
    parser.add_argument("--master", default=DEFAULT_SPARK_MASTER, help="Spark master URL")
    parser.add_argument(
        "--driver-host",
        default=DEFAULT_SPARK_DRIVER_HOST,
        help="Driver host/IP reachable from Spark workers",
    )
    parser.add_argument(
        "--columns",
        nargs="+",
        default=["Rate_Code", "mta_tax", "store_and_forward"],
        help="Columns to profile for null ratios",
    )
    args = parser.parse_args()

    spark = build_spark_session(
        app_name="check-null-ratios",
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        master=args.master,
        driver_host=args.driver_host,
    )
    spark.sparkContext.setLogLevel("ERROR")

    path = f"s3a://{args.bucket}/{args.prefix}"
    print(f"Reading parquet from: {path}")

    df = spark.read.option("recursiveFileLookup", "true").parquet(path)

    missing_columns = [
        column_name for column_name in args.columns if column_name not in df.columns
    ]
    if missing_columns:
        raise ValueError(f"Columns not found in dataset: {', '.join(missing_columns)}")

    agg_exprs = [
        count("*").alias("__total_rows__"),
    ] + [
        count(when(col(column_name).isNull(), 1)).alias(column_name)
        for column_name in args.columns
    ]
    stats = df.agg(*agg_exprs).collect()[0].asDict()
    total_rows = int(stats.pop("__total_rows__"))
    print(f"\nTotal rows: {total_rows}")

    print("\n=== Null ratio on full dataset ===")
    for column_name in args.columns:
        null_count = int(stats[column_name])
        null_ratio = (null_count / total_rows) if total_rows else 0.0
        print(
            f"{column_name}: null_count={null_count}, total_rows={total_rows}, null_ratio={null_ratio:.6f}"
        )

    spark.stop()


if __name__ == "__main__":
    main()
