from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import functions as F

from test_read_minio_parquet import build_spark_session
from trajectory_utils import (
    build_s3_path,
    normalize_trajectory_df,
    read_source_df,
    register_taxi_zone_dim_view,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare trajectory benchmark datasets in multiple formats and layouts."
    )
    parser.add_argument("--source-bucket", default="taxi-data", help="Source MinIO bucket")
    parser.add_argument("--source-prefix", default="", help="Source object prefix")
    parser.add_argument(
        "--source-format",
        default="parquet",
        choices=["parquet", "orc", "avro"],
        help="Source dataset format",
    )
    parser.add_argument("--target-bucket", default="taxi-data", help="Target MinIO bucket")
    parser.add_argument(
        "--target-prefix",
        default="bench",
        help="Target benchmark prefix inside the bucket",
    )
    parser.add_argument(
        "--endpoint",
        default="http://minio1:9000",
        help="MinIO S3 endpoint visible from Spark",
    )
    parser.add_argument("--access-key", default="minioadmin", help="MinIO access key")
    parser.add_argument("--secret-key", default="minioadmin", help="MinIO secret key")
    parser.add_argument(
        "--zone-lookup-path",
        default="/workspace/reference/taxi_zone_lookup.csv",
        help="Path to the taxi zone lookup CSV inside the Spark container",
    )
    parser.add_argument(
        "--bucket-count",
        type=int,
        default=16,
        help="Number of hash buckets to create for origin-zone bucketing",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        default=["parquet", "orc", "avro"],
        choices=["parquet", "orc", "avro"],
        help="Formats to generate for the raw benchmark dataset",
    )
    parser.add_argument(
        "--merge-schema",
        action="store_true",
        help="Enable schema merging when reading mixed-schema source data",
    )
    parser.add_argument(
        "--window-start",
        default="2024-01-01 00:00:00",
        help="Only prepare source year/month folders that overlap this window start",
    )
    parser.add_argument(
        "--window-end",
        default="2024-02-01 00:00:00",
        help="Only prepare source year/month folders that overlap this window end",
    )
    return parser.parse_args()


def write_raw_formats(
    trajectory_df,
    base_output: str,
    formats: list[str],
    mode: str,
) -> None:
    for output_format in formats:
        output_path = f"{base_output}/{output_format}/raw"
        print(f"Writing raw {output_format} dataset to: {output_path} (mode={mode})")
        writer = trajectory_df.write.mode(mode).format(output_format)
        if output_format == "avro":
            trajectory_df.sparkSession.sql(
                "SET spark.sql.legacy.replaceDatabricksSparkAvro.enabled=true"
            )
        writer.save(output_path)


def write_partitioned_dataset(trajectory_df, base_output: str, mode: str) -> None:
    output_path = f"{base_output}/parquet/partitioned_year_month"
    print(f"Writing partitioned parquet dataset to: {output_path} (mode={mode})")
    (
        trajectory_df.write.mode(mode)
        .format("parquet")
        .partitionBy("trip_year", "trip_month")
        .save(output_path)
    )


def write_bucketed_dataset(
    trajectory_df,
    base_output: str,
    bucket_count: int,
    mode: str,
) -> None:
    output_path = f"{base_output}/parquet/bucketed_origin_zone_hash"
    print(f"Writing hash-bucketed origin-zone parquet dataset to: {output_path} (mode={mode})")
    bucketed_df = trajectory_df.withColumn(
        "origin_zone_bucket",
        F.when(
            F.col("origin_zone_id").isNotNull(),
            F.pmod(F.hash("origin_zone_id"), F.lit(bucket_count)),
        ).otherwise(F.lit(-1)),
    )
    (
        bucketed_df.write.mode(mode)
        .format("parquet")
        .partitionBy("origin_zone_bucket")
        .save(output_path)
    )


def write_reference_dimension(spark, base_output: str) -> None:
    output_path = f"{base_output}/reference/taxi_zone_dim"
    print(f"Writing taxi zone dimension parquet to: {output_path}")
    spark.table("taxi_zone_dim").write.mode("overwrite").parquet(output_path)


def list_year_month_leaf_paths(spark, source_path: str) -> list[str]:
    root_path = spark._jvm.org.apache.hadoop.fs.Path(source_path)
    filesystem = root_path.getFileSystem(spark._jsc.hadoopConfiguration())
    year_month_paths: list[str] = []

    for year_status in filesystem.listStatus(root_path):
        year_path = year_status.getPath()
        year_name = year_path.getName()
        if not year_status.isDirectory() or not year_name.isdigit() or len(year_name) != 4:
            continue

        for month_status in filesystem.listStatus(year_path):
            month_path = month_status.getPath()
            month_name = month_path.getName()
            if month_status.isDirectory() and month_name.isdigit():
                year_month_paths.append(month_path.toString())

    return sorted(year_month_paths)


def filter_leaf_paths_by_window(
    leaf_paths: list[str],
    window_start: str,
    window_end: str,
) -> list[str]:
    start_dt = datetime.strptime(window_start, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(window_end, "%Y-%m-%d %H:%M:%S")
    filtered_paths: list[str] = []

    for leaf_path in leaf_paths:
        parts = leaf_path.rstrip("/").split("/")
        year = int(parts[-2])
        month = int(parts[-1])

        month_start = datetime(year, month, 1)
        if month == 12:
            month_end = datetime(year + 1, 1, 1)
        else:
            month_end = datetime(year, month + 1, 1)

        if month_end > start_dt and month_start < end_dt:
            filtered_paths.append(leaf_path)

    return filtered_paths


def main() -> None:
    args = parse_args()
    spark = build_spark_session(
        app_name="prepare-trajectory-benchmark-datasets",
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
    )
    spark.sparkContext.setLogLevel("ERROR")

    source_path = build_s3_path(args.source_bucket, args.source_prefix)
    target_base = build_s3_path(args.target_bucket, args.target_prefix).rstrip("/")

    print(f"Reading source dataset from: {source_path}")
    register_taxi_zone_dim_view(spark, args.zone_lookup_path)
    leaf_paths = filter_leaf_paths_by_window(
        list_year_month_leaf_paths(spark, source_path),
        args.window_start,
        args.window_end,
    )
    print(f"Found {len(leaf_paths)} raw year/month leaf paths to process.")
    write_mode = "overwrite"

    for index, leaf_path in enumerate(leaf_paths, start=1):
        print(f"\nProcessing leaf path {index}/{len(leaf_paths)}: {leaf_path}")
        try:
            source_df = read_source_df(
                spark=spark,
                source_format=args.source_format,
                source_path=leaf_path,
                layout="leaf",
                merge_schema=False,
            )
        except Exception as exc:
            if "UNABLE_TO_INFER_SCHEMA" in str(exc):
                print(f"Skipping empty or unreadable path: {leaf_path}")
                continue
            raise

        trajectory_df = normalize_trajectory_df(source_df).cache()
        print(f"Normalized rows for {leaf_path}: {trajectory_df.count()}")

        write_raw_formats(trajectory_df, target_base, args.formats, write_mode)
        write_partitioned_dataset(trajectory_df, target_base, write_mode)
        write_bucketed_dataset(trajectory_df, target_base, args.bucket_count, write_mode)
        trajectory_df.unpersist()
        write_mode = "append"

    write_reference_dimension(spark, target_base)

    print("\nFinished preparing benchmark datasets.")
    spark.stop()


if __name__ == "__main__":
    main()
