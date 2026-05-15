from __future__ import annotations

import argparse
import csv
import shutil
import statistics
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from project_config import DEFAULT_RESULTS_ROOT


BENCHMARK_FIELDS = [
    "query_id",
    "query_name",
    "experiment_type",
    "benchmark_group",
    "dataset_variant",
    "storage_format",
    "layout",
    "run_id",
    "run_type",
    "runtime_sec",
    "rows_returned",
    "stages",
    "tasks",
    "median_runtime_sec",
    "best_runtime_sec",
    "shuffle_read_mb",
    "shuffle_write_mb",
    "spill_mb",
    "partitions_scanned",
    "uses_broadcast_join",
    "uses_partition_pruning",
    "window_start",
    "window_end",
    "bucket_zone_id",
    "bucket_value",
    "explain_path",
    "notes",
]

SUPPLEMENTAL_FIELDS = [
    "query_id",
    "query_name",
    "experiment_type",
    "benchmark_group",
    "strategy",
    "run_id",
    "run_type",
    "runtime_sec",
    "rows_returned",
    "median_runtime_sec",
    "best_runtime_sec",
    "adaptive_enabled",
    "auto_broadcast_threshold",
    "shuffle_partitions",
    "left_rows",
    "right_rows",
    "hot_key_left_rows",
    "hot_key_right_rows",
    "salt_count",
    "left_skew_ratio_before",
    "left_skew_ratio_after",
    "uses_adaptive_plan",
    "uses_broadcast_join",
    "uses_sort_merge_join",
    "explain_path",
    "notes",
]


PARTITION_QUERIES = [
    (
        "QP01",
        "monthly_count_pruning",
        "Count one month of trajectory records.",
        """
        SELECT COUNT(*) AS trip_count
        FROM trips_clean
        WHERE trip_year = 2025
          AND trip_month = 1
          AND pickup_ts >= TIMESTAMP '2025-01-01 00:00:00'
          AND pickup_ts < TIMESTAMP '2025-02-01 00:00:00'
        """,
    ),
    (
        "QP02",
        "monthly_zone_flow",
        "Aggregate one month of trips by day and origin zone.",
        """
        SELECT
            trip_date,
            origin_zone_id,
            COUNT(*) AS trip_count,
            SUM(total_amt) AS total_revenue,
            AVG(trip_distance) AS avg_trip_distance
        FROM trips_clean
        WHERE trip_year = 2025
          AND trip_month = 1
          AND pickup_ts >= TIMESTAMP '2025-01-01 00:00:00'
          AND pickup_ts < TIMESTAMP '2025-02-01 00:00:00'
          AND origin_zone_id IS NOT NULL
        GROUP BY trip_date, origin_zone_id
        ORDER BY trip_date, trip_count DESC
        LIMIT 500
        """,
    ),
    (
        "QP03",
        "monthly_od_volume",
        "Aggregate one month of origin-destination volume.",
        """
        SELECT
            origin_zone_id,
            destination_zone_id,
            COUNT(*) AS trip_count,
            AVG(total_amt) AS avg_total_amt,
            AVG(trip_distance) AS avg_trip_distance
        FROM trips_clean
        WHERE trip_year = 2025
          AND trip_month = 1
          AND pickup_ts >= TIMESTAMP '2025-01-01 00:00:00'
          AND pickup_ts < TIMESTAMP '2025-02-01 00:00:00'
          AND origin_zone_id IS NOT NULL
          AND destination_zone_id IS NOT NULL
        GROUP BY origin_zone_id, destination_zone_id
        ORDER BY trip_count DESC
        LIMIT 100
        """,
    ),
]


BASELINE_JOIN_SQL = """
SELECT /*+ MERGE(l, r) */
    l.join_key,
    COUNT(*) AS joined_rows,
    SUM(l.amount + r.weight) AS total_score
FROM left_fact l
JOIN right_fact r
  ON l.join_key = r.join_key
GROUP BY l.join_key
ORDER BY joined_rows DESC
LIMIT 20
"""

SALTED_JOIN_SQL = """
WITH left_salted AS (
    SELECT
        *,
        PMOD(CAST(left_id AS BIGINT), {salt_count}) AS salt_key
    FROM left_fact
),
right_salted AS (
    SELECT
        r.*,
        salts.salt_key
    FROM right_fact r
    CROSS JOIN RANGE(0, {salt_count}) AS salts(salt_key)
)
SELECT /*+ MERGE(l, r) */
    l.join_key,
    COUNT(*) AS joined_rows,
    SUM(l.amount + r.weight) AS total_score
FROM left_salted l
JOIN right_salted r
  ON l.join_key = r.join_key
 AND l.salt_key = r.salt_key
GROUP BY l.join_key
ORDER BY joined_rows DESC
LIMIT 20
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local supplemental Topic 5 benchmarks for partition and skew gaps."
    )
    parser.add_argument("--results-root", default=str(DEFAULT_RESULTS_ROOT))
    parser.add_argument("--work-dir", default="/tmp/dqo_supplemental_bench")
    parser.add_argument("--partition-rows", type=int, default=600_000)
    parser.add_argument("--left-rows", type=int, default=180_000)
    parser.add_argument("--right-rows", type=int, default=12_000)
    parser.add_argument("--hot-left-rows", type=int, default=60_000)
    parser.add_argument("--hot-right-rows", type=int, default=80)
    parser.add_argument("--key-count", type=int, default=2_000)
    parser.add_argument("--salt-count", type=int, default=8)
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--measured-runs", type=int, default=2)
    parser.add_argument("--keep-work-dir", action="store_true")
    return parser.parse_args()


def build_spark(app_name: str, shuffle_partitions: int) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def ensure_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    ensure_parent(path)
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_text(path: Path, content: str) -> str:
    ensure_parent(path)
    path.write_text(content, encoding="utf-8")
    return str(path)


def explain_formatted(spark: SparkSession, sql_text: str) -> str:
    rows = spark.sql(f"EXPLAIN FORMATTED {sql_text}").collect()
    return "\n".join(row[0] for row in rows)


def plan_flags(explain_text: str) -> dict[str, bool]:
    lowered = explain_text.lower()
    return {
        "uses_broadcast_join": "broadcasthashjoin" in lowered or "broadcastnestedloopjoin" in lowered,
        "uses_partition_pruning": "partitionfilters" in lowered,
        "uses_adaptive_plan": "adaptivesparkplan" in lowered,
        "uses_sort_merge_join": "sortmergejoin" in lowered,
    }


def time_sql(spark: SparkSession, sql_text: str, measured_runs: int) -> list[dict[str, object]]:
    spark.sql(sql_text).count()
    rows: list[dict[str, object]] = []
    for run_id in range(1, measured_runs + 1):
        started = time.perf_counter()
        rows_returned = spark.sql(sql_text).count()
        runtime_sec = time.perf_counter() - started
        rows.append(
            {
                "run_id": run_id,
                "runtime_sec": round(runtime_sec, 6),
                "rows_returned": rows_returned,
            }
        )
    return rows


def add_runtime_summaries(rows: list[dict[str, object]]) -> None:
    runtimes = [float(row["runtime_sec"]) for row in rows]
    median_runtime = round(statistics.median(runtimes), 6)
    best_runtime = round(min(runtimes), 6)
    for row in rows:
        row["median_runtime_sec"] = median_runtime
        row["best_runtime_sec"] = best_runtime


def build_synthetic_trajectory(spark: SparkSession, row_count: int) -> DataFrame:
    base = spark.range(row_count).withColumn("trip_month", (F.pmod(F.col("id"), F.lit(12)) + 1).cast("int"))
    return (
        base.withColumn("trip_day", (F.pmod(F.col("id"), F.lit(28)) + 1).cast("int"))
        .withColumn("trip_hour", F.pmod(F.col("id"), F.lit(24)).cast("int"))
        .withColumn(
            "pickup_ts",
            F.to_timestamp(
                F.format_string(
                    "2025-%02d-%02d %02d:00:00",
                    F.col("trip_month"),
                    F.col("trip_day"),
                    F.col("trip_hour"),
                )
            ),
        )
        .withColumn("trip_date", F.to_date("pickup_ts"))
        .withColumn("trip_year", F.lit(2025).cast("int"))
        .withColumn(
            "origin_zone_id",
            F.when(F.pmod(F.col("id"), F.lit(100)) < 45, F.lit(237)).otherwise(
                (F.pmod(F.col("id"), F.lit(261)) + 1).cast("int")
            ),
        )
        .withColumn("destination_zone_id", (F.pmod(F.col("id") * 7, F.lit(263)) + 1).cast("int"))
        .withColumn("trip_distance", (F.lit(1.0) + F.pmod(F.col("id"), F.lit(30)).cast("double") / 10.0))
        .withColumn("total_amt", (F.lit(8.0) + F.pmod(F.col("id"), F.lit(120)).cast("double") / 2.0))
        .withColumn(
            "route_key",
            F.concat_ws("->", F.col("origin_zone_id").cast("string"), F.col("destination_zone_id").cast("string")),
        )
        .select(
            "pickup_ts",
            "trip_date",
            "trip_year",
            "trip_month",
            "origin_zone_id",
            "destination_zone_id",
            "trip_distance",
            "total_amt",
            "route_key",
        )
    )


def count_month_partitions(partitioned_path: Path) -> int:
    return len(list(partitioned_path.glob("trip_year=*/trip_month=*")))


def run_partition_month_benchmark(
    spark: SparkSession,
    results_root: Path,
    work_dir: Path,
    partition_rows: int,
    measured_runs: int,
) -> None:
    output_dir = results_root / "partition_month"
    plans_dir = output_dir / "plans"
    output_dir.mkdir(parents=True, exist_ok=True)
    plans_dir.mkdir(parents=True, exist_ok=True)

    data_dir = work_dir / "partition_month_data"
    raw_path = data_dir / "raw"
    partitioned_path = data_dir / "partitioned_year_month"
    ensure_clean_dir(data_dir)

    trajectory_df = build_synthetic_trajectory(spark, partition_rows).cache()
    trajectory_df.count()
    trajectory_df.repartition(24).write.mode("overwrite").parquet(str(raw_path))
    (
        trajectory_df.repartition(24, "trip_month")
        .write.mode("overwrite")
        .partitionBy("trip_year", "trip_month")
        .parquet(str(partitioned_path))
    )
    trajectory_df.unpersist()

    total_partitions = count_month_partitions(partitioned_path)
    variants = [
        (
            "raw",
            raw_path,
            output_dir / "trajectory_partition_month_raw_benchmark.csv",
            "trajectory_synthetic_raw_partition_month_compare",
            "12/12",
        ),
        (
            "partitioned_year_month",
            partitioned_path,
            output_dir / "trajectory_partition_month_benchmark.csv",
            "trajectory_synthetic_partitioned_year_month_month_window",
            f"1/{total_partitions}",
        ),
    ]

    for layout, source_path, output_path, dataset_variant, partitions_scanned in variants:
        rows_to_write: list[dict[str, object]] = []
        reader = spark.read.parquet(str(source_path))
        reader.createOrReplaceTempView("trips_clean")

        for query_id, query_name, notes, sql_text in PARTITION_QUERIES:
            explain_text = explain_formatted(spark, sql_text)
            explain_path = write_text(
                plans_dir / f"{output_path.stem}_{query_id}.txt",
                explain_text,
            )
            flags = plan_flags(explain_text)
            run_rows = time_sql(spark, sql_text, measured_runs)
            add_runtime_summaries(run_rows)
            for run in run_rows:
                rows_to_write.append(
                    {
                        "query_id": query_id,
                        "query_name": query_name,
                        "experiment_type": "partition_month",
                        "benchmark_group": "partition_month",
                        "dataset_variant": dataset_variant,
                        "storage_format": "parquet",
                        "layout": layout,
                        "run_id": run["run_id"],
                        "run_type": "measured",
                        "runtime_sec": run["runtime_sec"],
                        "rows_returned": run["rows_returned"],
                        "stages": "",
                        "tasks": "",
                        "median_runtime_sec": run["median_runtime_sec"],
                        "best_runtime_sec": run["best_runtime_sec"],
                        "shuffle_read_mb": "",
                        "shuffle_write_mb": "",
                        "spill_mb": "",
                        "partitions_scanned": partitions_scanned,
                        "uses_broadcast_join": flags["uses_broadcast_join"],
                        "uses_partition_pruning": flags["uses_partition_pruning"],
                        "window_start": "2025-01-01 00:00:00",
                        "window_end": "2025-02-01 00:00:00",
                        "bucket_zone_id": 237,
                        "bucket_value": "",
                        "explain_path": explain_path,
                        "notes": notes,
                    }
                )

        write_csv(output_path, BENCHMARK_FIELDS, rows_to_write)


def build_skew_facts(
    spark: SparkSession,
    left_rows: int,
    right_rows: int,
    hot_left_rows: int,
    hot_right_rows: int,
    key_count: int,
) -> tuple[DataFrame, DataFrame]:
    left = (
        spark.range(left_rows)
        .withColumnRenamed("id", "left_id")
        .withColumn(
            "join_key",
            F.when(F.col("left_id") < hot_left_rows, F.lit(0)).otherwise(
                (F.pmod(F.col("left_id"), F.lit(key_count)) + 1).cast("int")
            ),
        )
        .withColumn("amount", (F.lit(1.0) + F.pmod(F.col("left_id"), F.lit(100)).cast("double") / 10.0))
    )
    right = (
        spark.range(right_rows)
        .withColumnRenamed("id", "right_id")
        .withColumn(
            "join_key",
            F.when(F.col("right_id") < hot_right_rows, F.lit(0)).otherwise(
                (F.pmod(F.col("right_id"), F.lit(key_count)) + 1).cast("int")
            ),
        )
        .withColumn("weight", (F.lit(0.5) + F.pmod(F.col("right_id"), F.lit(50)).cast("double") / 20.0))
    )
    return left, right


def skew_ratio_for_partitioning(df: DataFrame, shuffle_partitions: int, columns: list[str]) -> float:
    counts = (
        df.repartition(shuffle_partitions, *[F.col(column) for column in columns])
        .withColumn("partition_id", F.spark_partition_id())
        .groupBy("partition_id")
        .count()
        .collect()
    )
    values = [row["count"] for row in counts if row["count"] > 0]
    if not values:
        return 0.0
    median = statistics.median(values)
    return round(max(values) / median, 3) if median else 0.0


def run_supplemental_join_benchmarks(
    spark: SparkSession,
    results_root: Path,
    measured_runs: int,
    left_rows: int,
    right_rows: int,
    hot_left_rows: int,
    hot_right_rows: int,
    key_count: int,
    salt_count: int,
    shuffle_partitions: int,
) -> None:
    left, right = build_skew_facts(
        spark,
        left_rows=left_rows,
        right_rows=right_rows,
        hot_left_rows=hot_left_rows,
        hot_right_rows=hot_right_rows,
        key_count=key_count,
    )
    left.cache().count()
    right.cache().count()
    left.createOrReplaceTempView("left_fact")
    right.createOrReplaceTempView("right_fact")

    salted_left = left.withColumn("salt_key", F.pmod(F.col("left_id"), F.lit(salt_count)))
    skew_before = skew_ratio_for_partitioning(left, shuffle_partitions, ["join_key"])
    skew_after = skew_ratio_for_partitioning(salted_left, shuffle_partitions, ["join_key", "salt_key"])

    run_aqe_benchmark(
        spark,
        results_root,
        measured_runs,
        left_rows,
        right_rows,
        hot_left_rows,
        hot_right_rows,
        salt_count,
        shuffle_partitions,
        skew_before,
        skew_after,
    )
    run_large_skew_benchmark(
        spark,
        results_root,
        measured_runs,
        left_rows,
        right_rows,
        hot_left_rows,
        hot_right_rows,
        salt_count,
        shuffle_partitions,
        skew_before,
        skew_after,
    )

    left.unpersist()
    right.unpersist()


def supplemental_rows_for_query(
    spark: SparkSession,
    output_dir: Path,
    output_stem: str,
    query_id: str,
    query_name: str,
    experiment_type: str,
    benchmark_group: str,
    strategy: str,
    sql_text: str,
    measured_runs: int,
    adaptive_enabled: bool,
    left_rows: int,
    right_rows: int,
    hot_left_rows: int,
    hot_right_rows: int,
    salt_count: int,
    shuffle_partitions: int,
    skew_before: float,
    skew_after: float,
    notes: str,
) -> list[dict[str, object]]:
    spark.conf.set("spark.sql.adaptive.enabled", str(adaptive_enabled).lower())
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    explain_text = explain_formatted(spark, sql_text)
    explain_path = write_text(output_dir / "plans" / f"{output_stem}_{query_id}.txt", explain_text)
    flags = plan_flags(explain_text)
    run_rows = time_sql(spark, sql_text, measured_runs)
    add_runtime_summaries(run_rows)

    rows: list[dict[str, object]] = []
    for run in run_rows:
        rows.append(
            {
                "query_id": query_id,
                "query_name": query_name,
                "experiment_type": experiment_type,
                "benchmark_group": benchmark_group,
                "strategy": strategy,
                "run_id": run["run_id"],
                "run_type": "measured",
                "runtime_sec": run["runtime_sec"],
                "rows_returned": run["rows_returned"],
                "median_runtime_sec": run["median_runtime_sec"],
                "best_runtime_sec": run["best_runtime_sec"],
                "adaptive_enabled": adaptive_enabled,
                "auto_broadcast_threshold": -1,
                "shuffle_partitions": shuffle_partitions,
                "left_rows": left_rows,
                "right_rows": right_rows,
                "hot_key_left_rows": hot_left_rows,
                "hot_key_right_rows": hot_right_rows,
                "salt_count": salt_count,
                "left_skew_ratio_before": skew_before,
                "left_skew_ratio_after": skew_after,
                "uses_adaptive_plan": flags["uses_adaptive_plan"],
                "uses_broadcast_join": flags["uses_broadcast_join"],
                "uses_sort_merge_join": flags["uses_sort_merge_join"],
                "explain_path": explain_path,
                "notes": notes,
            }
        )
    return rows


def run_aqe_benchmark(
    spark: SparkSession,
    results_root: Path,
    measured_runs: int,
    left_rows: int,
    right_rows: int,
    hot_left_rows: int,
    hot_right_rows: int,
    salt_count: int,
    shuffle_partitions: int,
    skew_before: float,
    skew_after: float,
) -> None:
    output_dir = results_root / "join_skew_aqe"
    output_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    rows.extend(
        supplemental_rows_for_query(
            spark,
            output_dir,
            "trajectory_join_skew_aqe_benchmark",
            "AQE_OFF",
            "sortmerge_skew_join_aqe_off",
            "join_skew_aqe",
            "join_skew_aqe",
            "SortMerge AQE off",
            BASELINE_JOIN_SQL,
            measured_runs,
            False,
            left_rows,
            right_rows,
            hot_left_rows,
            hot_right_rows,
            salt_count,
            shuffle_partitions,
            skew_before,
            skew_after,
            "Large synthetic fact-to-fact skew join with AQE disabled and broadcast disabled.",
        )
    )
    rows.extend(
        supplemental_rows_for_query(
            spark,
            output_dir,
            "trajectory_join_skew_aqe_benchmark",
            "AQE_ON",
            "sortmerge_skew_join_aqe_on",
            "join_skew_aqe",
            "join_skew_aqe",
            "SortMerge AQE on",
            BASELINE_JOIN_SQL,
            measured_runs,
            True,
            left_rows,
            right_rows,
            hot_left_rows,
            hot_right_rows,
            salt_count,
            shuffle_partitions,
            skew_before,
            skew_after,
            "Large synthetic fact-to-fact skew join with AQE enabled and broadcast disabled.",
        )
    )
    write_csv(output_dir / "trajectory_join_skew_aqe_benchmark.csv", SUPPLEMENTAL_FIELDS, rows)


def run_large_skew_benchmark(
    spark: SparkSession,
    results_root: Path,
    measured_runs: int,
    left_rows: int,
    right_rows: int,
    hot_left_rows: int,
    hot_right_rows: int,
    salt_count: int,
    shuffle_partitions: int,
    skew_before: float,
    skew_after: float,
) -> None:
    output_dir = results_root / "join_skew_large"
    output_dir.mkdir(parents=True, exist_ok=True)
    salted_sql = SALTED_JOIN_SQL.format(salt_count=salt_count)
    rows: list[dict[str, object]] = []
    rows.extend(
        supplemental_rows_for_query(
            spark,
            output_dir,
            "trajectory_large_skew_join_benchmark",
            "JL01",
            "large_sortmerge_skew_join",
            "join_skew_large",
            "join_skew_large",
            "SortMerge non-broadcast",
            BASELINE_JOIN_SQL,
            measured_runs,
            True,
            left_rows,
            right_rows,
            hot_left_rows,
            hot_right_rows,
            salt_count,
            shuffle_partitions,
            skew_before,
            skew_after,
            "Large synthetic fact-to-fact skew join with broadcast disabled.",
        )
    )
    rows.extend(
        supplemental_rows_for_query(
            spark,
            output_dir,
            "trajectory_large_skew_join_benchmark",
            "JL02",
            "large_salted_skew_join",
            "join_skew_large",
            "join_skew_large",
            "Salted non-broadcast",
            salted_sql,
            measured_runs,
            True,
            left_rows,
            right_rows,
            hot_left_rows,
            hot_right_rows,
            salt_count,
            shuffle_partitions,
            skew_before,
            skew_after,
            "Large synthetic fact-to-fact salted join with broadcast disabled; right side is replicated by salt.",
        )
    )
    write_csv(output_dir / "trajectory_large_skew_join_benchmark.csv", SUPPLEMENTAL_FIELDS, rows)


def write_readme(results_root: Path) -> None:
    path = results_root / "supplemental_README.md"
    path.write_text(
        "\n".join(
            [
                "# Supplemental Topic 5 Benchmarks",
                "",
                "These local-mode Spark benchmarks close the evidence gaps that were not runnable through the current MinIO compose setup.",
                "",
                "Generated groups:",
                "",
                "- `partition_month/`: one-month partition pruning with explicit `partitions_scanned` metadata.",
                "- `join_skew_aqe/`: AQE off/on comparison for a non-broadcast skew join.",
                "- `join_skew_large/`: large synthetic fact-to-fact SortMerge vs salted skew join with broadcast disabled.",
                "",
                "The synthetic rows preserve the project's trajectory model: time window, origin zone, destination zone, route key, distance, and amount.",
            ]
        ),
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    results_root = Path(args.results_root)
    work_dir = Path(args.work_dir)
    results_root.mkdir(parents=True, exist_ok=True)
    ensure_clean_dir(work_dir)

    spark = build_spark("supplemental-topic5-benchmarks", args.shuffle_partitions)
    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_partition_month_benchmark(
            spark=spark,
            results_root=results_root,
            work_dir=work_dir,
            partition_rows=args.partition_rows,
            measured_runs=args.measured_runs,
        )
        run_supplemental_join_benchmarks(
            spark=spark,
            results_root=results_root,
            measured_runs=args.measured_runs,
            left_rows=args.left_rows,
            right_rows=args.right_rows,
            hot_left_rows=args.hot_left_rows,
            hot_right_rows=args.hot_right_rows,
            key_count=args.key_count,
            salt_count=args.salt_count,
            shuffle_partitions=args.shuffle_partitions,
        )
        write_readme(results_root)
    finally:
        spark.stop()
        if not args.keep_work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)

    print(f"Supplemental benchmarks written under: {results_root}")


if __name__ == "__main__":
    main()
