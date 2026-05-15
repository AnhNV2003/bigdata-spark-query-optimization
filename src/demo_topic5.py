#!/usr/bin/env python3
"""Presentation demo for Topic 5 evidence.

The full Spark/MinIO benchmark is too slow and environment-sensitive for a
3-minute presentation demo. This script provides a reliable live demo by reading
the generated benchmark CSVs, physical plans, validation files, and chart folder.
"""

from __future__ import annotations

import argparse
import csv
import platform
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = PROJECT_ROOT / "results"
EVIDENCE_DIR = RESULTS_DIR / "evidence"
CHARTS_DIR = EVIDENCE_DIR / "charts"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def parse_percent(value: str) -> float:
    return float(value.replace("% faster", "").replace("% slower", "").strip())


def parse_speedup(value: str) -> float:
    return float(value.replace("x", "").strip())


def fmt(value: float, digits: int = 1) -> str:
    return f"{value:.{digits}f}"


def require_file(path: Path) -> None:
    if not path.exists():
        raise SystemExit(f"Missing required demo artifact: {path.relative_to(PROJECT_ROOT)}")


def find_signal(path: Path, signal: str) -> bool:
    if not path.exists():
        return False
    return signal in path.read_text(encoding="utf-8", errors="replace")


def print_header(title: str) -> None:
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def print_rows(rows: list[tuple[str, str]]) -> None:
    width = max(len(label) for label, _ in rows)
    for label, value in rows:
        print(f"{label:<{width}} : {value}")


def open_asset(path: Path) -> None:
    if platform.system() == "Darwin":
        subprocess.run(["open", str(path)], check=False)
    else:
        print(f"Open manually: {path}")


def build_demo_summary() -> dict[str, str]:
    require_file(EVIDENCE_DIR / "format_summary.csv")
    require_file(EVIDENCE_DIR / "partition_month_summary.csv")
    require_file(EVIDENCE_DIR / "bucketing_summary.csv")
    require_file(EVIDENCE_DIR / "join_summary.csv")
    require_file(EVIDENCE_DIR / "join_skew_aqe_summary.csv")
    require_file(EVIDENCE_DIR / "join_skew_large_summary.csv")
    require_file(EVIDENCE_DIR / "topic5_requirement_audit.csv")
    require_file(EVIDENCE_DIR / "validation_report.csv")

    format_rows = read_csv(EVIDENCE_DIR / "format_summary.csv")
    partition_rows = read_csv(EVIDENCE_DIR / "partition_month_summary.csv")
    bucket_rows = read_csv(EVIDENCE_DIR / "bucketing_summary.csv")
    join_rows = read_csv(EVIDENCE_DIR / "join_summary.csv")
    aqe_rows = read_csv(EVIDENCE_DIR / "join_skew_aqe_summary.csv")
    large_rows = read_csv(EVIDENCE_DIR / "join_skew_large_summary.csv")
    audit_rows = read_csv(EVIDENCE_DIR / "topic5_requirement_audit.csv")
    validation_rows = read_csv(EVIDENCE_DIR / "validation_report.csv")

    avro_avg_runtime = sum(float(r["Avro"]) for r in format_rows) / len(format_rows)
    parquet_avg_runtime = sum(float(r["Parquet"]) for r in format_rows) / len(format_rows)
    orc_avg_runtime = sum(float(r["ORC"]) for r in format_rows) / len(format_rows)
    parquet_avg = avro_avg_runtime / parquet_avg_runtime
    orc_avg = avro_avg_runtime / orc_avg_runtime

    partition_speedups = [parse_percent(r["result"]) for r in partition_rows]
    partition_scan = sorted({r["partitions_scanned"] for r in partition_rows})[0]

    bucket_speedups = [parse_percent(r["result"]) for r in bucket_rows]

    join_by_id = {r["query_id"]: r for r in join_rows}
    sortmerge = join_by_id["Q08"]
    broadcast = join_by_id["Q09"]
    broadcast_speedup = float(sortmerge["median_runtime_sec"]) / float(broadcast["median_runtime_sec"])

    aqe_modes = ", ".join(
        f"{r['query_id']} adaptive={r['adaptive_enabled']}" for r in aqe_rows
    )

    large_first = large_rows[0]
    skew_before = float(large_first["left_skew_ratio_before"])
    skew_after = float(large_first["left_skew_ratio_after"])

    audit_status = sorted({r["Status"] for r in audit_rows})
    validation_status = sorted({r["Status"] for r in validation_rows})
    chart_files = sorted(CHARTS_DIR.glob("*.png"))

    return {
        "format": f"Parquet {fmt(parquet_avg)}x faster than Avro; ORC {fmt(orc_avg)}x faster than Avro",
        "partition": f"one-month partition pruning scans {partition_scan}; {fmt(min(partition_speedups))}-{fmt(max(partition_speedups))}% faster",
        "bucketing": f"origin-zone hash bucket layout is {fmt(min(bucket_speedups))}-{fmt(max(bucket_speedups))}% faster",
        "join": f"broadcast join is {fmt(broadcast_speedup)}x faster than SortMerge; spill {sortmerge['spill_mb']} MB -> {broadcast['spill_mb']} MB",
        "aqe": f"AQE evidence present: {aqe_modes}",
        "skew": f"non-broadcast salting reduces hot-key skew ratio {fmt(skew_before, 3)} -> {fmt(skew_after, 3)}",
        "audit": ", ".join(audit_status),
        "validation": ", ".join(validation_status),
        "charts": f"{len(chart_files)} PNG charts in {CHARTS_DIR.relative_to(PROJECT_ROOT)}",
    }


def print_plan_signals() -> None:
    signals = [
        (
            "Partition pruning",
            RESULTS_DIR / "partition_month" / "plans" / "trajectory_partition_month_benchmark_QP01.txt",
            "PartitionFilters",
            True,
        ),
        (
            "Broadcast join",
            RESULTS_DIR / "join_skew" / "plans" / "trajectory_join_skew_benchmark_Q09.txt",
            "BroadcastHashJoin",
            True,
        ),
        (
            "AQE enabled plan",
            RESULTS_DIR / "join_skew_aqe" / "plans" / "trajectory_join_skew_aqe_benchmark_AQE_ON.txt",
            "AdaptiveSparkPlan",
            True,
        ),
        (
            "Non-broadcast skew join",
            RESULTS_DIR / "join_skew_large" / "plans" / "trajectory_large_skew_join_benchmark_JL02.txt",
            "BroadcastHashJoin",
            False,
        ),
        (
            "SortMerge distributed join",
            RESULTS_DIR / "join_skew_large" / "plans" / "trajectory_large_skew_join_benchmark_JL02.txt",
            "SortMergeJoin",
            True,
        ),
    ]

    rows: list[tuple[str, str]] = []
    for label, path, signal, expected_present in signals:
        present = find_signal(path, signal)
        ok = present is expected_present
        expectation = "found" if expected_present else "absent"
        rows.append((label, f"{'PASS' if ok else 'FAIL'} ({signal} {expectation})"))
    print_rows(rows)


def print_script_cues() -> None:
    print_header("3-MINUTE NARRATION CUES")
    cues = [
        ("0:00-0:20", "Problem: optimize Spark SQL over columnar storage for NYC taxi trajectory analytics."),
        ("0:20-0:45", "Architecture: NYC taxi data -> MinIO/S3A -> Spark SQL -> benchmark layouts -> evidence."),
        ("0:45-1:20", "Run this demo command and show PASS/Fulfilled statuses from generated CSV evidence."),
        ("1:20-2:15", "Show the four optimizations: format, partition pruning, bucketing, join/skew."),
        ("2:15-2:45", "Open charts/report folder and point to visual proof and physical-plan signals."),
        ("2:45-3:00", "Close: all Topic 5 requirements are fulfilled and reproducible from results/evidence/."),
    ]
    print_rows(cues)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Topic 5 presentation demo.")
    parser.add_argument(
        "--open-assets",
        action="store_true",
        help="Open the complete report and chart folder after printing the demo summary.",
    )
    parser.add_argument(
        "--with-script-cues",
        action="store_true",
        help="Print a 3-minute narration timeline.",
    )
    args = parser.parse_args()

    summary = build_demo_summary()

    print_header("TOPIC 5 PRODUCT DEMO")
    print("Distributed Query Optimization on Columnar Storage")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Evidence root: {EVIDENCE_DIR.relative_to(PROJECT_ROOT)}")

    print_header("REQUIREMENT ALIGNMENT")
    print_rows(
        [
            ("Format comparison", summary["format"]),
            ("Partition data skipping", summary["partition"]),
            ("Bucketing data skipping", summary["bucketing"]),
            ("Join strategy", summary["join"]),
            ("AQE and skew", f"{summary['aqe']}; {summary['skew']}"),
        ]
    )

    print_header("PHYSICAL PLAN PROOF")
    print_plan_signals()

    print_header("EVALUATION AND TESTING")
    print_rows(
        [
            ("Requirement audit", summary["audit"]),
            ("Validation checks", summary["validation"]),
            ("Visual evidence", summary["charts"]),
            ("Complete report", str((EVIDENCE_DIR / "TOPIC5_COMPLETE_REPORT.md").relative_to(PROJECT_ROOT))),
        ]
    )

    if args.with_script_cues:
        print_script_cues()

    if args.open_assets:
        open_asset(EVIDENCE_DIR / "TOPIC5_COMPLETE_REPORT.md")
        open_asset(CHARTS_DIR)


if __name__ == "__main__":
    main()
