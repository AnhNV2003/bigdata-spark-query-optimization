from __future__ import annotations

import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: pandas. Install with `venv/bin/pip install pandas pyarrow`."
    ) from exc


SOURCE_ROOT = Path("/home/vanh/data/projects/bigdata/dataset/parquet")
TARGET_ROOT = Path("/home/vanh/data/projects/bigdata/dataset/csv")


def convert_file(parquet_path: Path) -> bool:
    relative_path = parquet_path.relative_to(SOURCE_ROOT)
    csv_path = TARGET_ROOT / relative_path.with_suffix(".csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    if csv_path.exists():
        print(f"Skip: {csv_path}")
        return False

    print(f"Convert: {parquet_path} -> {csv_path}")
    df = pd.read_parquet(parquet_path)
    df.to_csv(csv_path, index=False)
    return True


def main() -> int:
    if not SOURCE_ROOT.exists():
        print(f"Source folder not found: {SOURCE_ROOT}", file=sys.stderr)
        return 1

    TARGET_ROOT.mkdir(parents=True, exist_ok=True)
    parquet_files = sorted(SOURCE_ROOT.rglob("*.parquet"))

    if not parquet_files:
        print(f"No parquet files found under: {SOURCE_ROOT}")
        return 0

    converted = 0
    skipped = 0
    failed = 0

    for parquet_path in parquet_files:
        try:
            if convert_file(parquet_path):
                converted += 1
            else:
                skipped += 1
        except Exception as exc:
            failed += 1
            print(f"Failed: {parquet_path} ({exc})", file=sys.stderr)

    print(
        f"Done. total={len(parquet_files)} converted={converted} skipped={skipped} failed={failed}"
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
