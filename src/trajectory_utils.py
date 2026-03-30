from __future__ import annotations

import csv
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PAYMENT_DIM_ROWS = [
    ("CASH", "Cash payment"),
    ("CREDIT", "Card payment"),
    ("NO CHARGE", "No charge"),
    ("DISPUTE", "Dispute"),
    ("UNKNOWN", "Unknown"),
    ("VOIDED TRIP", "Voided trip"),
]

DEFAULT_BUCKET_COUNT = 16


def build_s3_path(bucket: str, prefix: str) -> str:
    clean_prefix = prefix.strip("/")
    return f"s3a://{bucket}/{clean_prefix}" if clean_prefix else f"s3a://{bucket}/"


def read_source_df(
    spark: SparkSession,
    source_format: str,
    source_path: str,
    layout: str,
    merge_schema: bool = False,
) -> DataFrame:
    reader = spark.read.format(source_format)

    if source_format == "avro":
        spark.sql("SET spark.sql.legacy.replaceDatabricksSparkAvro.enabled=true")

    if layout == "raw":
        reader = reader.option("recursiveFileLookup", "true")

    # Large multi-year NYC taxi dumps often disagree on minor numeric types
    # (for example INT vs BIGINT). Let the reader pick one footer schema for
    # raw source folders, then normalize downstream, instead of forcing
    # Spark's schema merge to fail before we can cast into the trajectory view.
    if merge_schema and layout != "raw" and source_format in {"parquet", "orc"}:
        reader = reader.option("mergeSchema", "true")

    return reader.load(source_path)


def list_leaf_data_paths(spark: SparkSession, source_path: str) -> list[str]:
    path = spark._jvm.org.apache.hadoop.fs.Path(source_path)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    leaves: list[str] = []

    def _walk(current_path) -> None:
        statuses = filesystem.listStatus(current_path)
        child_dirs = []
        has_data_file = False

        for status in statuses:
            child_path = status.getPath()
            name = child_path.getName()
            if status.isDirectory():
                child_dirs.append(child_path)
            elif not name.startswith("_") and not name.startswith("."):
                has_data_file = True

        if has_data_file or not child_dirs:
            leaves.append(current_path.toString())
            return

        for child_dir in child_dirs:
            _walk(child_dir)

    _walk(path)
    return sorted(set(leaves))


def _existing_columns(df: DataFrame) -> set[str]:
    return set(df.columns)


def _coalesce_columns(*columns):
    valid_columns = [column for column in columns if column is not None]
    if not valid_columns:
        return F.lit(None)
    if len(valid_columns) == 1:
        return valid_columns[0]
    return F.coalesce(*valid_columns)


def _string_column(df: DataFrame, name: str):
    if name not in _existing_columns(df):
        return None
    return F.col(name).cast("string")


def _double_column(df: DataFrame, name: str):
    if name not in _existing_columns(df):
        return None
    return F.col(name).cast("double")


def _long_column(df: DataFrame, name: str):
    if name not in _existing_columns(df):
        return None
    return F.col(name).cast("long")


def _int_column(df: DataFrame, name: str):
    if name not in _existing_columns(df):
        return None
    return F.col(name).cast("int")


def _timestamp_column(df: DataFrame, name: str):
    if name not in _existing_columns(df):
        return None
    return F.to_timestamp(F.col(name))


def _legacy_vendor(df: DataFrame):
    column = _string_column(df, "vendor_name")
    if column is None:
        return None
    return F.upper(F.trim(column))


def _modern_vendor(df: DataFrame):
    vendor_id = _int_column(df, "VendorID")
    if vendor_id is None:
        return None
    return (
        F.when(vendor_id == 1, F.lit("CMT"))
        .when(vendor_id == 2, F.lit("VTS"))
        .otherwise(vendor_id.cast("string"))
    )


def _legacy_payment_type(df: DataFrame):
    column = _string_column(df, "Payment_Type")
    if column is None:
        return None
    return F.upper(F.trim(column))


def _modern_payment_type(df: DataFrame):
    payment_type = _int_column(df, "payment_type")
    if payment_type is None:
        return None
    return (
        F.when(payment_type == 1, F.lit("CREDIT"))
        .when(payment_type == 2, F.lit("CASH"))
        .when(payment_type == 3, F.lit("NO CHARGE"))
        .when(payment_type == 4, F.lit("DISPUTE"))
        .when(payment_type == 5, F.lit("UNKNOWN"))
        .when(payment_type == 6, F.lit("VOIDED TRIP"))
        .otherwise(payment_type.cast("string"))
    )


def normalize_trajectory_df(df: DataFrame) -> DataFrame:
    columns = _existing_columns(df)

    if {
        "pickup_ts",
        "dropoff_ts",
        "trip_date",
        "trip_year",
        "trip_month",
        "origin_zone_id",
        "destination_zone_id",
        "trip_distance",
        "fare_amt",
        "total_amt",
    }.issubset(columns):
        return df

    pickup_ts = _coalesce_columns(
        _timestamp_column(df, "Trip_Pickup_DateTime"),
        _timestamp_column(df, "tpep_pickup_datetime"),
    )
    dropoff_ts = _coalesce_columns(
        _timestamp_column(df, "Trip_Dropoff_DateTime"),
        _timestamp_column(df, "tpep_dropoff_datetime"),
    )

    origin_zone_id = _int_column(df, "PULocationID")
    destination_zone_id = _int_column(df, "DOLocationID")

    start_lon = _double_column(df, "Start_Lon")
    start_lat = _double_column(df, "Start_Lat")
    end_lon = _double_column(df, "End_Lon")
    end_lat = _double_column(df, "End_Lat")

    has_gps_coordinates = (
        F.coalesce(start_lon, start_lat, end_lon, end_lat).isNotNull()
        if any(column is not None for column in [start_lon, start_lat, end_lon, end_lat])
        else F.lit(False)
    )

    trajectory_df = (
        df.select(
            _coalesce_columns(_legacy_vendor(df), _modern_vendor(df)).alias("vendor_name_norm"),
            _coalesce_columns(_legacy_payment_type(df), _modern_payment_type(df)).alias(
                "payment_type_norm"
            ),
            pickup_ts.alias("pickup_ts"),
            dropoff_ts.alias("dropoff_ts"),
            F.to_date(pickup_ts).alias("trip_date"),
            F.year(pickup_ts).alias("trip_year"),
            F.month(pickup_ts).alias("trip_month"),
            origin_zone_id.alias("origin_zone_id"),
            destination_zone_id.alias("destination_zone_id"),
            _coalesce_columns(
                _long_column(df, "Passenger_Count"),
                _long_column(df, "passenger_count"),
            ).alias("passenger_count"),
            _coalesce_columns(
                _double_column(df, "Trip_Distance"),
                _double_column(df, "trip_distance"),
            ).alias("trip_distance"),
            _coalesce_columns(
                _double_column(df, "Fare_Amt"),
                _double_column(df, "fare_amount"),
            ).alias("fare_amt"),
            _coalesce_columns(
                _double_column(df, "surcharge"),
                _double_column(df, "extra"),
            ).alias("surcharge"),
            _coalesce_columns(
                _double_column(df, "Tip_Amt"),
                _double_column(df, "tip_amount"),
            ).alias("tip_amt"),
            _coalesce_columns(
                _double_column(df, "Tolls_Amt"),
                _double_column(df, "tolls_amount"),
            ).alias("tolls_amt"),
            _coalesce_columns(
                _double_column(df, "Total_Amt"),
                _double_column(df, "total_amount"),
            ).alias("total_amt"),
            (start_lon if start_lon is not None else F.lit(None).cast("double")).alias("start_lon"),
            (start_lat if start_lat is not None else F.lit(None).cast("double")).alias("start_lat"),
            (end_lon if end_lon is not None else F.lit(None).cast("double")).alias("end_lon"),
            (end_lat if end_lat is not None else F.lit(None).cast("double")).alias("end_lat"),
            has_gps_coordinates.alias("has_gps_coordinates"),
        )
        .where(pickup_ts.isNotNull())
        .withColumn(
            "trajectory_mode",
            F.when(F.col("origin_zone_id").isNotNull(), F.lit("zone_based")).otherwise(
                F.when(F.col("has_gps_coordinates"), F.lit("coordinate_based")).otherwise(
                    F.lit("unknown")
                )
            ),
        )
        .withColumn(
            "route_key",
            F.when(
                F.col("origin_zone_id").isNotNull() & F.col("destination_zone_id").isNotNull(),
                F.concat_ws(
                    "->",
                    F.col("origin_zone_id").cast("string"),
                    F.col("destination_zone_id").cast("string"),
                ),
            ),
        )
        .withColumn(
            "schema_family",
            F.when(F.col("origin_zone_id").isNotNull(), F.lit("modern_zone")).otherwise(
                F.when(F.col("has_gps_coordinates"), F.lit("legacy_gps")).otherwise(
                    F.lit("mixed_unknown")
                )
            ),
        )
    )

    return trajectory_df


def add_bucket_layout_columns(df: DataFrame, bucket_count: int = DEFAULT_BUCKET_COUNT) -> DataFrame:
    if "origin_zone_bucket" in df.columns:
        return df

    return df.withColumn(
        "origin_zone_bucket",
        F.when(
            F.col("origin_zone_id").isNotNull(),
            F.pmod(F.hash("origin_zone_id"), F.lit(bucket_count)),
        ).otherwise(F.lit(-1)),
    )


def load_trajectory_df(
    spark: SparkSession,
    source_format: str,
    source_path: str,
    layout: str,
    merge_schema: bool = False,
) -> DataFrame:
    if layout == "raw" and merge_schema and source_format in {"parquet", "orc"}:
        trajectory_df = None
        for leaf_path in list_leaf_data_paths(spark, source_path):
            try:
                leaf_df = read_source_df(
                    spark=spark,
                    source_format=source_format,
                    source_path=leaf_path,
                    layout="leaf",
                    merge_schema=False,
                )
            except Exception as exc:
                if "UNABLE_TO_INFER_SCHEMA" in str(exc):
                    continue
                raise
            normalized_leaf_df = normalize_trajectory_df(leaf_df)
            if trajectory_df is None:
                trajectory_df = normalized_leaf_df
            else:
                trajectory_df = trajectory_df.unionByName(
                    normalized_leaf_df,
                    allowMissingColumns=True,
                )

        if trajectory_df is None:
            raise ValueError(f"No readable source data found under {source_path}")
        return trajectory_df

    return normalize_trajectory_df(
        read_source_df(
            spark=spark,
            source_format=source_format,
            source_path=source_path,
            layout=layout,
            merge_schema=merge_schema,
        )
    )


def register_trajectory_view(
    spark: SparkSession,
    source_df: DataFrame,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
) -> DataFrame:
    trajectory_df = add_bucket_layout_columns(
        normalize_trajectory_df(source_df),
        bucket_count=bucket_count,
    )
    trajectory_df.createOrReplaceTempView("trips_clean")
    return trajectory_df


def register_payment_dim_view(spark: SparkSession) -> None:
    payment_dim_df = spark.createDataFrame(
        PAYMENT_DIM_ROWS,
        ["payment_type_norm", "payment_description"],
    )
    payment_dim_df.createOrReplaceTempView("payment_type_dim")


def register_taxi_zone_dim_view(spark: SparkSession, zone_lookup_path: str) -> None:
    path = Path(zone_lookup_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Taxi zone lookup file not found: {zone_lookup_path}. "
            "Mount the reference data or provide a valid path."
        )

    # Load the small reference CSV on the driver first, then create a Spark
    # DataFrame. This avoids executor-side file path mismatches between the
    # Jupyter container and the Spark worker containers.
    with path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = [
            (
                int(row["LocationID"]),
                row["Borough"].strip(),
                row["Zone"].strip(),
                row["service_zone"].strip(),
            )
            for row in reader
        ]

    zone_dim_df = spark.createDataFrame(
        rows,
        ["location_id", "borough", "zone", "service_zone"],
    )
    zone_dim_df.createOrReplaceTempView("taxi_zone_dim")


def register_supporting_views(spark: SparkSession, zone_lookup_path: str) -> None:
    register_payment_dim_view(spark)
    register_taxi_zone_dim_view(spark, zone_lookup_path)
