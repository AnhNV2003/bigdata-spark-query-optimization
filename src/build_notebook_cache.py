from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession

from test_read_minio_parquet import build_spark_session
from trajectory_utils import load_trajectory_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a reusable normalized trips_clean cache for notebooks."
    )
    parser.add_argument("--bucket", default="taxi-data", help="MinIO bucket name")
    parser.add_argument(
        "--prefixes",
        nargs="+",
        default=["2021", "2022", "2023", "2024", "2025"],
        help="Raw source prefixes to normalize",
    )
    parser.add_argument(
        "--cache-prefix",
        default="notebook_cache/trips_clean_2021_2025",
        help="Destination prefix for the normalized cache",
    )
    parser.add_argument(
        "--source-format",
        default="parquet",
        choices=["parquet", "orc", "avro"],
        help="Input source format",
    )
    parser.add_argument(
        "--endpoint",
        default="http://minio1:9000",
        help="MinIO endpoint visible from Spark",
    )
    parser.add_argument(
        "--master",
        default="spark://spark-master:7077",
        help="Spark master URL",
    )
    parser.add_argument("--access-key", default="minioadmin", help="MinIO access key")
    parser.add_argument("--secret-key", default="minioadmin", help="MinIO secret key")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Overwrite an existing notebook cache",
    )
    return parser.parse_args()


def s3_path_exists(spark: SparkSession, path_str: str) -> bool:
    path = spark._jvm.org.apache.hadoop.fs.Path(path_str)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    return filesystem.exists(path)


def union_by_name(dataframes: list[DataFrame]) -> DataFrame:
    if not dataframes:
        raise ValueError("No source DataFrames were loaded.")

    merged = dataframes[0]
    for dataframe in dataframes[1:]:
        merged = merged.unionByName(dataframe, allowMissingColumns=True)
    return merged


def main() -> None:
    args = parse_args()

    spark = build_spark_session(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        app_name="build-notebook-cache",
        master=args.master,
    )
    spark.sparkContext.setLogLevel("ERROR")

    cache_path = f"s3a://{args.bucket}/{args.cache_prefix.strip('/')}"

    if s3_path_exists(spark, cache_path) and not args.refresh:
        print(f"Notebook cache already exists: {cache_path}")
        spark.stop()
        return

    yearly_dfs: list[DataFrame] = []
    for prefix in args.prefixes:
        source_path = f"s3a://{args.bucket}/{prefix.strip('/')}"
        print(f"Normalizing raw trajectory data from: {source_path}")
        yearly_dfs.append(
            load_trajectory_df(
                spark,
                args.source_format,
                source_path,
                layout="raw",
                merge_schema=True,
            )
        )

    trajectory_df = union_by_name(yearly_dfs).repartition("trip_year", "trip_month")

    print(f"Writing normalized notebook cache to: {cache_path}")
    (
        trajectory_df.write.mode("overwrite")
        .partitionBy("trip_year", "trip_month")
        .parquet(cache_path)
    )

    print(f"Notebook cache ready: {cache_path}")
    spark.stop()


if __name__ == "__main__":
    main()
