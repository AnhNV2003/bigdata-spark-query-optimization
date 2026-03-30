from __future__ import annotations

import argparse
import csv
import os
import statistics
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

from benchmark_definitions import ALL_QUERY_IDS, QUERY_GROUPS, QUERY_MAP
from test_read_minio_parquet import build_spark_session
from trajectory_utils import (
    DEFAULT_BUCKET_COUNT,
    build_s3_path,
    load_trajectory_df,
    register_supporting_views,
    register_trajectory_view,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run trajectory benchmark queries and export results to CSV."
    )
    parser.add_argument("--bucket", default="taxi-data", help="MinIO bucket name")
    parser.add_argument("--prefix", default="", help="Object prefix inside the bucket")
    parser.add_argument(
        "--source-format",
        default="parquet",
        choices=["parquet", "orc", "avro"],
        help="Input format",
    )
    parser.add_argument(
        "--endpoint",
        default="http://minio1:9000",
        help="MinIO S3 endpoint visible from Spark",
    )
    parser.add_argument("--access-key", default="minioadmin", help="MinIO access key")
    parser.add_argument("--secret-key", default="minioadmin", help="MinIO secret key")
    parser.add_argument(
        "--query-ids",
        nargs="+",
        help="Explicit benchmark query IDs to run, or ALL to run the full set",
    )
    parser.add_argument(
        "--query-group",
        choices=sorted(QUERY_GROUPS),
        help="Run a predefined benchmark group such as format, partition, join_skew, or bucket_layout",
    )
    parser.add_argument(
        "--layout",
        default="raw",
        choices=["raw", "partitioned_year_month", "bucketed_origin_zone_hash"],
        help="Dataset layout label and read mode",
    )
    parser.add_argument(
        "--window-start",
        default="2024-01-01 00:00:00",
        help="Benchmark time window start timestamp",
    )
    parser.add_argument(
        "--window-end",
        default="2024-02-01 00:00:00",
        help="Benchmark time window end timestamp",
    )
    parser.add_argument(
        "--zone-lookup-path",
        default="/workspace/reference/taxi_zone_lookup.csv",
        help="Path to the taxi zone lookup CSV inside the Spark container",
    )
    parser.add_argument(
        "--bucket-count",
        type=int,
        default=DEFAULT_BUCKET_COUNT,
        help="Hash bucket count used by the bucketed origin-zone layout",
    )
    parser.add_argument(
        "--bucket-zone-id",
        type=int,
        help="Optional fixed origin zone ID for bucket-layout benchmark queries",
    )
    parser.add_argument(
        "--warmup-runs",
        type=int,
        default=1,
        help="Number of warm-up runs per query",
    )
    parser.add_argument(
        "--measured-runs",
        type=int,
        default=3,
        help="Number of measured runs per query",
    )
    parser.add_argument(
        "--benchmark-group",
        default="ad_hoc",
        help="Batch label for the output file, e.g. format / partition / join_skew",
    )
    parser.add_argument("--storage-format", default="parquet", help="Reported storage format label")
    parser.add_argument(
        "--dataset-variant",
        default="trips_trajectory_raw",
        help="Dataset variant label",
    )
    parser.add_argument(
        "--output",
        default="/workspace/results/benchmark_runs.csv",
        help="Output CSV path visible inside the Spark container",
    )
    parser.add_argument(
        "--merge-schema",
        action="store_true",
        help="Enable schema merging when reading mixed-schema raw parquet or ORC inputs",
    )
    return parser.parse_args()


def resolve_query_ids(args: argparse.Namespace) -> list[str]:
    if args.query_ids:
        if args.query_ids == ["ALL"]:
            return ALL_QUERY_IDS
        return args.query_ids
    if args.query_group:
        return QUERY_GROUPS[args.query_group]
    raise ValueError("Provide either --query-ids or --query-group.")


def render_sql(sql_template: str, time_window: dict[str, str]) -> str:
    return sql_template.format(**time_window)


def explain_formatted_text(spark: SparkSession, sql_text: str) -> str:
    rows = spark.sql(f"EXPLAIN FORMATTED {sql_text}").collect()
    return "\n".join(row[0] for row in rows)


def extract_plan_hints(explain_text: str) -> dict[str, object]:
    text = explain_text.lower()
    return {
        "uses_broadcast_join": "broadcasthashjoin" in text or "broadcastnestedloopjoin" in text,
        "uses_partition_pruning": "partitionfilters" in text or "dynamicpruning" in text,
    }


def stage_task_summary(spark: SparkSession, job_group: str) -> tuple[int, int]:
    tracker = spark.sparkContext.statusTracker()
    job_ids = tracker.getJobIdsForGroup(job_group)
    stage_ids: set[int] = set()
    total_tasks = 0

    for job_id in job_ids:
        job_info = tracker.getJobInfo(job_id)
        if not job_info:
            continue
        for stage_id in job_info.stageIds:
            stage_ids.add(stage_id)
            stage_info = tracker.getStageInfo(stage_id)
            if stage_info:
                total_tasks += stage_info.numTasks

    return len(stage_ids), total_tasks


def run_query_once(
    spark: SparkSession,
    query_id: str,
    sql_text: str,
    run_label: str,
) -> dict[str, object]:
    job_group = f"{query_id}-{run_label}-{int(time.time() * 1000)}"
    spark.sparkContext.setJobGroup(job_group, f"benchmark {query_id} {run_label}")

    started = time.perf_counter()
    rows_returned = spark.sql(sql_text).count()
    runtime_sec = time.perf_counter() - started

    stages, tasks = stage_task_summary(spark, job_group)
    return {
        "runtime_sec": round(runtime_sec, 6),
        "rows_returned": rows_returned,
        "stages": stages,
        "tasks": tasks,
    }


def ensure_output_parent(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)


def ensure_output_writable(output_path: Path) -> None:
    probe_path = output_path.parent / f".{output_path.name}.write_probe"
    with probe_path.open("w", encoding="utf-8"):
        pass
    probe_path.unlink(missing_ok=True)


def atomic_write_text(target_path: Path, content: str) -> None:
    temp_path = target_path.parent / f".{target_path.name}.{os.getpid()}.tmp"
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(target_path)


def atomic_write_csv(
    target_path: Path,
    fieldnames: list[str],
    rows: list[dict[str, object]],
) -> None:
    temp_path = target_path.parent / f".{target_path.name}.{os.getpid()}.tmp"
    with temp_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    temp_path.replace(target_path)


def resolve_output_path(raw_output: str) -> Path:
    requested = Path(raw_output)
    try:
        ensure_output_parent(requested)
        ensure_output_writable(requested)
        return requested
    except PermissionError:
        fallback = Path("/tmp/benchmark_outputs") / requested.name
        ensure_output_parent(fallback)
        ensure_output_writable(fallback)
        print(
            "Output path is not writable inside the current container. "
            f"Falling back to: {fallback}"
        )
        return fallback


def write_explain_text(output_path: Path, query_id: str, explain_text: str) -> str:
    explain_dir = output_path.parent / "plans"
    explain_path = explain_dir / f"{output_path.stem}_{query_id}.txt"
    explain_dir.mkdir(parents=True, exist_ok=True)

    try:
        ensure_output_writable(explain_path)
    except PermissionError:
        explain_dir = output_path.parent / "plans_runtime"
        explain_dir.mkdir(parents=True, exist_ok=True)
        explain_path = explain_dir / f"{output_path.stem}_{query_id}.txt"
        ensure_output_writable(explain_path)

    atomic_write_text(explain_path, explain_text)
    return str(explain_path)


def resolve_bucket_target(
    spark: SparkSession,
    window_start: str,
    window_end: str,
    bucket_count: int,
    bucket_zone_id: int | None,
) -> tuple[int, int]:
    zone_id = bucket_zone_id

    if zone_id is None:
        row = spark.sql(
            f"""
            SELECT origin_zone_id
            FROM trips_clean
            WHERE pickup_ts >= TIMESTAMP '{window_start}'
              AND pickup_ts < TIMESTAMP '{window_end}'
              AND origin_zone_id IS NOT NULL
            GROUP BY origin_zone_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
            """
        ).collect()
        if not row:
            raise ValueError("Could not resolve a benchmark origin zone inside the selected window.")
        zone_id = int(row[0][0])

    bucket_value = spark.sql(
        f"SELECT PMOD(HASH(CAST({zone_id} AS INT)), {bucket_count}) AS bucket_value"
    ).collect()[0]["bucket_value"]
    return zone_id, int(bucket_value)


def main() -> None:
    args = parse_args()
    requested_query_ids = resolve_query_ids(args)
    missing = [query_id for query_id in requested_query_ids if query_id not in QUERY_MAP]
    if missing:
        raise ValueError(f"Unknown query IDs: {', '.join(missing)}")

    spark = build_spark_session(
        app_name="run-trajectory-benchmark",
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
    )
    spark.sparkContext.setLogLevel("ERROR")

    source_path = build_s3_path(args.bucket, args.prefix)
    print(f"Loading source data from: {source_path} ({args.source_format}, layout={args.layout})")

    trajectory_df = load_trajectory_df(
        spark=spark,
        source_format=args.source_format,
        source_path=source_path,
        layout=args.layout,
        merge_schema=args.merge_schema,
    )
    register_trajectory_view(spark, trajectory_df, bucket_count=args.bucket_count)
    register_supporting_views(spark, args.zone_lookup_path)

    bucket_zone_id, bucket_value = resolve_bucket_target(
        spark=spark,
        window_start=args.window_start,
        window_end=args.window_end,
        bucket_count=args.bucket_count,
        bucket_zone_id=args.bucket_zone_id,
    )

    time_window = {
        "window_start": args.window_start,
        "window_end": args.window_end,
        "window_year": datetime.strptime(args.window_start, "%Y-%m-%d %H:%M:%S").year,
        "window_month": datetime.strptime(args.window_start, "%Y-%m-%d %H:%M:%S").month,
        "bucket_zone_id": bucket_zone_id,
        "bucket_value": bucket_value,
    }
    output_path = resolve_output_path(args.output)

    fieldnames = [
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
    rows_to_write: list[dict[str, object]] = []

    for query_id in requested_query_ids:
        query = QUERY_MAP[query_id]
        sql_text = render_sql(query.sql, time_window)
        explain_text = explain_formatted_text(spark, sql_text)
        plan_hints = extract_plan_hints(explain_text)
        explain_path = write_explain_text(output_path, query_id, explain_text)

        print(f"\n=== Running {query.query_id}: {query.name} ===")
        print(query.description)
        print("\n--- EXPLAIN FORMATTED ---")
        print(explain_text)

        for warmup_index in range(1, args.warmup_runs + 1):
            _ = run_query_once(spark, query.query_id, sql_text, f"warmup-{warmup_index}")
            print(f"Warm-up {warmup_index}/{args.warmup_runs} completed.")

        measured_runs: list[dict[str, object]] = []
        runtimes: list[float] = []

        for measured_index in range(1, args.measured_runs + 1):
            result = run_query_once(
                spark,
                query.query_id,
                sql_text,
                f"measured-{measured_index}",
            )
            measured_runs.append(result)
            runtimes.append(float(result["runtime_sec"]))
            print(
                f"Measured run {measured_index}/{args.measured_runs}: "
                f"{result['runtime_sec']} sec, rows={result['rows_returned']}, "
                f"stages={result['stages']}, tasks={result['tasks']}"
            )

        median_runtime = round(statistics.median(runtimes), 6) if runtimes else 0.0
        best_runtime = round(min(runtimes), 6) if runtimes else 0.0

        for run_index, run_result in enumerate(measured_runs, start=1):
            rows_to_write.append(
                {
                    "query_id": query.query_id,
                    "query_name": query.name,
                    "experiment_type": query.experiment_type,
                    "benchmark_group": args.benchmark_group,
                    "dataset_variant": args.dataset_variant,
                    "storage_format": args.storage_format,
                    "layout": args.layout,
                    "run_id": run_index,
                    "run_type": "measured",
                    "runtime_sec": run_result["runtime_sec"],
                    "rows_returned": run_result["rows_returned"],
                    "stages": run_result["stages"],
                    "tasks": run_result["tasks"],
                    "median_runtime_sec": median_runtime,
                    "best_runtime_sec": best_runtime,
                    "shuffle_read_mb": "",
                    "shuffle_write_mb": "",
                    "spill_mb": "",
                    "partitions_scanned": "",
                    "uses_broadcast_join": plan_hints["uses_broadcast_join"],
                    "uses_partition_pruning": plan_hints["uses_partition_pruning"],
                    "window_start": args.window_start,
                    "window_end": args.window_end,
                    "bucket_zone_id": bucket_zone_id,
                    "bucket_value": bucket_value,
                    "explain_path": explain_path,
                    "notes": query.description,
                }
            )

    atomic_write_csv(output_path, fieldnames, rows_to_write)

    print(f"\nBenchmark results saved to: {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
