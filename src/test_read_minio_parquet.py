from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def build_spark_session(
    app_name: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    master: str | None = None,
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    if master:
        builder = builder.master(master)
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
    parser.add_argument("--bucket", default="taxi-data", help="MinIO bucket name")
    parser.add_argument(
        "--prefix",
        default="",
        help="Object prefix inside the bucket. Use trailing slash for a folder-like prefix.",
    )
    parser.add_argument(
        "--endpoint",
        default="http://minio1:9000",
        help="MinIO S3 endpoint visible from Spark",
    )
    parser.add_argument("--access-key", default="minioadmin", help="MinIO access key")
    parser.add_argument("--secret-key", default="minioadmin", help="MinIO secret key")
    parser.add_argument("--sample-rows", type=int, default=5, help="Number of sample rows to print")
    args = parser.parse_args()

    spark = build_spark_session(
        app_name="test-read-minio-parquet",
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
    )
    spark.sparkContext.setLogLevel("ERROR")

    path = f"s3a://{args.bucket}/{args.prefix}"
    print(f"Reading parquet from: {path}")

    df = spark.read.option("recursiveFileLookup", "true").parquet(path)
    describe_dataframe(df, args.sample_rows)

    spark.stop()


if __name__ == "__main__":
    main()
