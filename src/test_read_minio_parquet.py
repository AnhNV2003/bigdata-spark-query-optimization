from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

from project_config import (
    DEFAULT_MINIO_ACCESS_KEY,
    DEFAULT_MINIO_BUCKET,
    DEFAULT_MINIO_ENDPOINT,
    DEFAULT_MINIO_SECRET_KEY,
    DEFAULT_SPARK_DRIVER_HOST,
    DEFAULT_SPARK_IVY_DIR,
    DEFAULT_SPARK_MASTER,
    DEFAULT_SPARK_PACKAGES,
)


def build_spark_session(
    app_name: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    master: str | None = None,
    driver_host: str | None = DEFAULT_SPARK_DRIVER_HOST,
    packages: str | None = DEFAULT_SPARK_PACKAGES,
    extra_configs: dict[str, str] | None = None,
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.jars.ivy", DEFAULT_SPARK_IVY_DIR)
        .config("spark.shuffle.readHostLocalDisk", "false")
    )
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    if driver_host:
        builder = builder.config("spark.driver.host", driver_host)
    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)
    builder = builder.master(master or DEFAULT_SPARK_MASTER)
    return builder.getOrCreate()


def describe_dataframe(df, sample_rows: int) -> None:
    print("\n=== Schema ===")
    df.printSchema()

    print(f"\n=== Sample rows ({sample_rows}) ===")
    df.show(sample_rows, truncate=False)

    print("\n=== Row count ===")
    print(df.count())

    print("\n=== Column count ===")
    print(len(df.columns))

    print("\n=== Null counts ===")
    null_exprs = [count(col(c)).alias(c) for c in df.columns]
    non_null_counts = df.agg(*null_exprs).collect()[0].asDict()
    total_rows = df.count()
    for column_name in df.columns:
        null_count = total_rows - non_null_counts[column_name]
        print(f"{column_name}: {null_count}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read parquet from MinIO with Spark and inspect the contents."
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
    parser.add_argument("--sample-rows", type=int, default=5, help="Number of sample rows to print")
    args = parser.parse_args()

    spark = build_spark_session(
        app_name="test-read-minio-parquet",
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
    describe_dataframe(df, args.sample_rows)

    spark.stop()


if __name__ == "__main__":
    main()
