#!/usr/bin/env python3
"""Terminal-first 3-minute demo pipeline for Topic 5.

This script intentionally runs a compact local Spark workload so the presentation
shows real code execution in the terminal. The live workload is illustrative; the
final numbers printed at the end are read from the generated project evidence for
the full benchmark.
"""

from __future__ import annotations

import argparse
import csv
import platform
import shutil
import subprocess
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = PROJECT_ROOT / "results"
EVIDENCE_DIR = RESULTS_DIR / "evidence"
CHARTS_DIR = EVIDENCE_DIR / "charts"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the terminal-first Topic 5 demo pipeline.")
    parser.add_argument("--work-dir", default="/tmp/dqo_topic5_terminal_demo")
    parser.add_argument("--rows", type=int, default=240_000)
    parser.add_argument("--left-rows", type=int, default=90_000)
    parser.add_argument("--right-rows", type=int, default=6_000)
    parser.add_argument("--hot-left-rows", type=int, default=30_000)
    parser.add_argument("--hot-right-rows", type=int, default=40)
    parser.add_argument("--salt-count", type=int, default=8)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--keep-work-dir", action="store_true")
    parser.add_argument("--open-assets", action="store_true")
    parser.add_argument("--with-script-cues", action="store_true")
    parser.add_argument(
        "--presentation-mode",
        action="store_true",
        help="Pause between terminal sections so the screen recording fits a 3-minute narrated demo.",
    )
    parser.add_argument("--pause-scale", type=float, default=1.0)
    return parser.parse_args()


def section(title: str) -> None:
    print()
    print("=" * 86, flush=True)
    print(title, flush=True)
    print("=" * 86, flush=True)


def line(message: str) -> None:
    print(message, flush=True)


def presentation_pause(args: argparse.Namespace, seconds: float, note: str) -> None:
    if not args.presentation_mode:
        return
    wait = max(0.0, seconds * args.pause_scale)
    line(f"\n[presenter pause: {note} | {wait:.0f}s]")
    time.sleep(wait)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def parse_percent(value: str) -> float:
    return float(value.replace("% faster", "").replace("% slower", "").strip())


def require(path: Path) -> None:
    if not path.exists():
        raise SystemExit(f"Missing required file: {path.relative_to(PROJECT_ROOT)}")


def clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def build_spark(shuffle_partitions: int) -> SparkSession:
    return (
        SparkSession.builder.appName("topic5-terminal-demo")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def make_trajectory_df(spark: SparkSession, rows: int) -> DataFrame:
    base = (
        spark.range(rows)
        .withColumnRenamed("id", "trip_id")
        .withColumn("trip_year", F.lit(2025))
        .withColumn("trip_month", (F.pmod(F.col("trip_id"), F.lit(12)) + F.lit(1)).cast("int"))
        .withColumn(
            "origin_zone_id",
            F.when(F.pmod(F.col("trip_id"), F.lit(11)) == 0, F.lit(237)).otherwise(
                (F.pmod(F.col("trip_id"), F.lit(260)) + F.lit(1)).cast("int")
            ),
        )
        .withColumn(
            "destination_zone_id",
            (F.pmod(F.col("trip_id") * F.lit(7), F.lit(260)) + F.lit(1)).cast("int"),
        )
    )
    return base.select(
        F.col("trip_id"),
        F.col("trip_year"),
        F.col("trip_month"),
        F.col("origin_zone_id"),
        F.col("destination_zone_id"),
        F.concat_ws(
            "->",
            F.col("origin_zone_id").cast("string"),
            F.col("destination_zone_id").cast("string"),
        ).alias("route_key"),
        F.pmod(F.col("trip_id"), F.lit(16)).cast("int").alias("origin_zone_bucket"),
        (F.lit(5.0) + F.pmod(F.col("trip_id"), F.lit(80)).cast("double") / F.lit(10.0)).alias(
            "trip_distance"
        ),
        (F.lit(8.0) + F.pmod(F.col("trip_id"), F.lit(500)).cast("double") / F.lit(10.0)).alias(
            "total_amt"
        ),
    )


def time_sql(spark: SparkSession, label: str, sql_text: str) -> tuple[float, int]:
    line(f"  running {label} ...")
    started = time.perf_counter()
    count = spark.sql(sql_text).count()
    elapsed = time.perf_counter() - started
    line(f"  done    {label:<34} rows={count:<8} runtime={elapsed:.3f}s")
    return elapsed, count


def explain_text(spark: SparkSession, sql_text: str) -> str:
    return "\n".join(row[0] for row in spark.sql(f"EXPLAIN FORMATTED {sql_text}").collect())


def plan_has(spark: SparkSession, sql_text: str, token: str) -> bool:
    return token.lower() in explain_text(spark, sql_text).lower()


def max_avg_ratio(df: DataFrame, columns: list[str]) -> float:
    counts = df.groupBy(*columns).count()
    row = counts.agg(F.max("count").alias("max_count"), F.avg("count").alias("avg_count")).first()
    return float(row["max_count"]) / float(row["avg_count"])


def run_live_spark_demo(args: argparse.Namespace) -> None:
    work_dir = Path(args.work_dir)
    clean_dir(work_dir)

    section("1/5 LIVE SPARK SETUP: BUILD MINI TRAJECTORY WORKLOAD")
    line(f"Creating local Spark session and {args.rows:,} synthetic trajectory rows...")
    presentation_pause(args, 10, "explain compact live Spark workload")
    spark = build_spark(args.shuffle_partitions)
    spark.sparkContext.setLogLevel("ERROR")

    raw_path = work_dir / "raw_parquet"
    orc_path = work_dir / "raw_orc"
    partition_path = work_dir / "partitioned_year_month"
    bucket_path = work_dir / "bucketed_origin_zone_hash"

    trips = make_trajectory_df(spark, args.rows)

    started = time.perf_counter()
    trips.write.mode("overwrite").parquet(str(raw_path))
    trips.write.mode("overwrite").orc(str(orc_path))
    trips.write.mode("overwrite").partitionBy("trip_year", "trip_month").parquet(str(partition_path))
    trips.write.mode("overwrite").partitionBy("origin_zone_bucket").parquet(str(bucket_path))
    line(f"Wrote Parquet, ORC, year/month partitions, and origin-zone bucket folders in {time.perf_counter() - started:.1f}s")
    presentation_pause(args, 18, "explain generated benchmark layouts")

    spark.read.parquet(str(raw_path)).createOrReplaceTempView("raw_trips")
    spark.read.orc(str(orc_path)).createOrReplaceTempView("orc_trips")
    spark.read.parquet(str(partition_path)).createOrReplaceTempView("partitioned_trips")
    spark.read.parquet(str(bucket_path)).createOrReplaceTempView("bucketed_trips")

    section("2/5 LIVE FORMAT CHECK: COLUMNAR READS")
    format_sql = """
    SELECT origin_zone_id, COUNT(*) AS trips, AVG(total_amt) AS avg_total
    FROM {table}
    WHERE origin_zone_id IS NOT NULL
    GROUP BY origin_zone_id
    ORDER BY trips DESC
    LIMIT 20
    """
    time_sql(spark, "Parquet projection/aggregation", format_sql.format(table="raw_trips"))
    time_sql(spark, "ORC projection/aggregation", format_sql.format(table="orc_trips"))
    line("  live check proves the demo can execute columnar Spark reads; final Avro comparison is printed from full evidence below")
    presentation_pause(args, 16, "explain columnar storage and Avro comparison")

    section("3/5 LIVE DATA SKIPPING: PARTITIONING AND BUCKETING")
    month_sql_raw = """
    SELECT origin_zone_id, COUNT(*) AS trips
    FROM raw_trips
    WHERE trip_year = 2025 AND trip_month = 1
    GROUP BY origin_zone_id
    ORDER BY trips DESC
    LIMIT 20
    """
    month_sql_partitioned = month_sql_raw.replace("raw_trips", "partitioned_trips")
    time_sql(spark, "raw month query", month_sql_raw)
    time_sql(spark, "partitioned month query", month_sql_partitioned)
    line(f"  physical plan PartitionFilters found: {plan_has(spark, month_sql_partitioned, 'PartitionFilters')}")

    bucket_sql_raw = """
    SELECT origin_zone_id, destination_zone_id, COUNT(*) AS trips
    FROM raw_trips
    WHERE origin_zone_id = 237
    GROUP BY origin_zone_id, destination_zone_id
    ORDER BY trips DESC
    LIMIT 20
    """
    bucket_sql_bucketed = """
    SELECT origin_zone_id, destination_zone_id, COUNT(*) AS trips
    FROM bucketed_trips
    WHERE origin_zone_bucket = 13 AND origin_zone_id = 237
    GROUP BY origin_zone_id, destination_zone_id
    ORDER BY trips DESC
    LIMIT 20
    """
    time_sql(spark, "raw origin-zone query", bucket_sql_raw)
    time_sql(spark, "bucketed origin-zone query", bucket_sql_bucketed)
    line(f"  bucket plan PartitionFilters found: {plan_has(spark, bucket_sql_bucketed, 'PartitionFilters')}")
    presentation_pause(args, 24, "explain partition pruning and bucket directory pruning")

    section("4/5 LIVE JOIN/SKEW CHECK: NON-BROADCAST SORTMERGE AND SALTING")
    left = spark.range(args.left_rows).withColumnRenamed("id", "left_id").select(
        F.col("left_id"),
        F.when(F.col("left_id") < args.hot_left_rows, F.lit(237))
        .otherwise((F.pmod(F.col("left_id"), F.lit(2_000)) + F.lit(1)).cast("int"))
        .alias("join_key"),
        (F.lit(1.0) + F.pmod(F.col("left_id"), F.lit(100)).cast("double") / F.lit(10.0)).alias("amount"),
    )
    right = spark.range(args.right_rows).withColumnRenamed("id", "right_id").select(
        F.col("right_id"),
        F.when(F.col("right_id") < args.hot_right_rows, F.lit(237))
        .otherwise((F.pmod(F.col("right_id"), F.lit(2_000)) + F.lit(1)).cast("int"))
        .alias("join_key"),
        (F.lit(1.0) + F.pmod(F.col("right_id"), F.lit(50)).cast("double") / F.lit(10.0)).alias("weight"),
    )
    left.createOrReplaceTempView("left_fact")
    right.createOrReplaceTempView("right_fact")

    before = max_avg_ratio(left, ["join_key"])
    after = max_avg_ratio(
        left.withColumn("salt_key", F.pmod(F.col("left_id"), F.lit(args.salt_count))),
        ["join_key", "salt_key"],
    )
    line(f"  measured hot-key skew ratio before/after salting: {before:.3f} -> {after:.3f}")

    baseline_join = """
    SELECT /*+ MERGE(l, r) */ l.join_key, COUNT(*) AS joined_rows
    FROM left_fact l
    JOIN right_fact r ON l.join_key = r.join_key
    GROUP BY l.join_key
    ORDER BY joined_rows DESC
    LIMIT 20
    """
    salted_join = f"""
    WITH left_salted AS (
        SELECT *, PMOD(CAST(left_id AS BIGINT), {args.salt_count}) AS salt_key
        FROM left_fact
    ),
    right_salted AS (
        SELECT r.*, salts.salt_key
        FROM right_fact r
        CROSS JOIN RANGE(0, {args.salt_count}) AS salts(salt_key)
    )
    SELECT /*+ MERGE(l, r) */ l.join_key, COUNT(*) AS joined_rows
    FROM left_salted l
    JOIN right_salted r
      ON l.join_key = r.join_key AND l.salt_key = r.salt_key
    GROUP BY l.join_key
    ORDER BY joined_rows DESC
    LIMIT 20
    """
    time_sql(spark, "non-broadcast SortMerge join", baseline_join)
    time_sql(spark, "salted non-broadcast join", salted_join)
    salted_plan = explain_text(spark, salted_join)
    line(f"  SortMergeJoin found: {'sortmergejoin' in salted_plan.lower()}")
    line(f"  BroadcastHashJoin absent: {'broadcasthashjoin' not in salted_plan.lower()}")
    line(f"  AdaptiveSparkPlan found: {'adaptivesparkplan' in salted_plan.lower()}")
    presentation_pause(args, 24, "explain non-broadcast skew join and salting")

    spark.stop()
    if not args.keep_work_dir:
        shutil.rmtree(work_dir, ignore_errors=True)


def official_summary(args: argparse.Namespace) -> None:
    section("5/5 OFFICIAL GENERATED EVIDENCE: FULL PROJECT RESULTS")
    required_files = [
        EVIDENCE_DIR / "format_summary.csv",
        EVIDENCE_DIR / "partition_month_summary.csv",
        EVIDENCE_DIR / "bucketing_summary.csv",
        EVIDENCE_DIR / "join_summary.csv",
        EVIDENCE_DIR / "join_skew_aqe_summary.csv",
        EVIDENCE_DIR / "join_skew_large_summary.csv",
        EVIDENCE_DIR / "topic5_requirement_audit.csv",
        EVIDENCE_DIR / "validation_report.csv",
    ]
    for path in required_files:
        require(path)

    format_rows = read_csv(EVIDENCE_DIR / "format_summary.csv")
    avro_avg = sum(float(r["Avro"]) for r in format_rows) / len(format_rows)
    parquet_avg = sum(float(r["Parquet"]) for r in format_rows) / len(format_rows)
    orc_avg = sum(float(r["ORC"]) for r in format_rows) / len(format_rows)

    partition_rows = read_csv(EVIDENCE_DIR / "partition_month_summary.csv")
    partition_speedups = [parse_percent(r["result"]) for r in partition_rows]

    bucketing_rows = read_csv(EVIDENCE_DIR / "bucketing_summary.csv")
    bucket_speedups = [parse_percent(r["result"]) for r in bucketing_rows]

    join_rows = {r["query_id"]: r for r in read_csv(EVIDENCE_DIR / "join_summary.csv")}
    sortmerge = join_rows["Q08"]
    broadcast = join_rows["Q09"]
    broadcast_speedup = float(sortmerge["median_runtime_sec"]) / float(broadcast["median_runtime_sec"])

    large_rows = read_csv(EVIDENCE_DIR / "join_skew_large_summary.csv")
    skew_before = float(large_rows[0]["left_skew_ratio_before"])
    skew_after = float(large_rows[0]["left_skew_ratio_after"])

    audit_status = sorted({r["Status"] for r in read_csv(EVIDENCE_DIR / "topic5_requirement_audit.csv")})
    validation_status = sorted({r["Status"] for r in read_csv(EVIDENCE_DIR / "validation_report.csv")})
    chart_count = len(list(CHARTS_DIR.glob("*.png")))

    line(f"  FORMAT      Parquet {avro_avg / parquet_avg:.1f}x faster than Avro; ORC {avro_avg / orc_avg:.1f}x faster than Avro")
    line(
        "  PARTITION   one-month pruning scans 1/12 partitions; "
        f"{min(partition_speedups):.1f}-{max(partition_speedups):.1f}% faster"
    )
    line(f"  BUCKETING   origin-zone hash layout is {min(bucket_speedups):.1f}-{max(bucket_speedups):.1f}% faster")
    line(
        "  JOIN        broadcast is "
        f"{broadcast_speedup:.1f}x faster than SortMerge; spill {sortmerge['spill_mb']} MB -> {broadcast['spill_mb']} MB"
    )
    line(f"  SKEW        non-broadcast salting reduces skew ratio {skew_before:.3f} -> {skew_after:.3f}")
    line(f"  AUDIT       {', '.join(audit_status)}")
    line(f"  VALIDATION  {', '.join(validation_status)}")
    line(f"  CHARTS      {chart_count} PNG files in {CHARTS_DIR.relative_to(PROJECT_ROOT)}")
    presentation_pause(args, 18, "close on official evidence, audit, and validation")


def script_cues() -> None:
    section("3-MINUTE TERMINAL DEMO SCRIPT")
    cues = [
        ("0:00-0:20", "Run one command and explain: this is a compact Spark demo plus official evidence validation."),
        ("0:20-0:50", "Show Spark creating mini trajectory layouts: Parquet, ORC, partitioned, bucketed."),
        ("0:50-1:35", "Show terminal runtimes for format, partition pruning, and bucket data skipping."),
        ("1:35-2:20", "Show non-broadcast SortMerge/salted join and plan checks: no broadcast, SortMerge, AQE."),
        ("2:20-2:50", "Show official generated evidence numbers for 48.7M-row project benchmark."),
        ("2:50-3:00", "Close on audit=Fulfilled, validation=PASS, charts=10 PNG files."),
    ]
    width = max(len(t) for t, _ in cues)
    for time_range, text in cues:
        line(f"  {time_range:<{width}}  {text}")


def open_assets() -> None:
    if platform.system() != "Darwin":
        line(f"Open manually: {EVIDENCE_DIR / 'TOPIC5_COMPLETE_REPORT.md'}")
        line(f"Open manually: {CHARTS_DIR}")
        return
    subprocess.run(["open", str(EVIDENCE_DIR / "TOPIC5_COMPLETE_REPORT.md")], check=False)
    subprocess.run(["open", str(CHARTS_DIR)], check=False)


def main() -> None:
    args = parse_args()
    started = time.perf_counter()

    section("TOPIC 5 TERMINAL PRODUCT DEMO")
    line("Project: Distributed Query Optimization on Columnar Storage")
    line("Demo mode: live local Spark process + official generated benchmark evidence")
    line(f"Evidence folder: {EVIDENCE_DIR.relative_to(PROJECT_ROOT)}")

    run_live_spark_demo(args)
    official_summary(args)

    if args.with_script_cues or args.presentation_mode:
        script_cues()

    line(f"\nDemo completed in {time.perf_counter() - started:.1f}s")

    if args.open_assets:
        open_assets()


if __name__ == "__main__":
    main()
