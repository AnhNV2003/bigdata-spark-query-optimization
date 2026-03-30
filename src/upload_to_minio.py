from __future__ import annotations

import argparse
from pathlib import Path

from minio import Minio
from minio.error import S3Error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload local dataset files to MinIO.")
    parser.add_argument(
        "--source-dir",
        default="/home/vanh/data/projects/bigdata/dataset/parquet",
        help="Local directory containing files to upload",
    )
    parser.add_argument("--bucket", default="taxi-data", help="Target MinIO bucket")
    parser.add_argument(
        "--prefix",
        default="",
        help="Optional object prefix inside the bucket, e.g. parquet/",
    )
    parser.add_argument("--endpoint", default="localhost:9000", help="MinIO endpoint from the host machine")
    parser.add_argument("--access-key", default="minioadmin", help="MinIO access key")
    parser.add_argument("--secret-key", default="minioadmin", help="MinIO secret key")
    parser.add_argument(
        "--pattern",
        default="*.parquet",
        help="Glob pattern to choose which files to upload",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively upload files matching the pattern",
    )
    parser.add_argument("--from-year", type=int, help="Start year to upload from")
    parser.add_argument("--from-month", type=int, help="Start month to upload from (1-12)")
    parser.add_argument("--to-year", type=int, help="End year to upload to")
    parser.add_argument("--to-month", type=int, help="End month to upload to (1-12)")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip upload if the object already exists in MinIO. Default behavior is overwrite.",
    )
    return parser.parse_args()


def object_exists(client: Minio, bucket: str, object_name: str) -> bool:
    try:
        client.stat_object(bucket, object_name)
        return True
    except S3Error as exc:
        if exc.code == "NoSuchKey":
            return False
        raise


def validate_range_args(args: argparse.Namespace) -> None:
    provided = [args.from_year, args.from_month, args.to_year, args.to_month]
    if any(value is not None for value in provided) and not all(
        value is not None for value in provided
    ):
        raise ValueError(
            "You must provide all of --from-year, --from-month, "
            "--to-year, --to-month together."
        )

    for label, month in (("from-month", args.from_month), ("to-month", args.to_month)):
        if month is not None and not 1 <= month <= 12:
            raise ValueError(f"{label} must be between 1 and 12.")

    if args.from_year is not None:
        start = (args.from_year, args.from_month)
        end = (args.to_year, args.to_month)
        if start > end:
            raise ValueError(
                "The from year/month must be earlier than or equal to the to year/month."
            )


def extract_year_month(relative_path: Path) -> tuple[int, int] | None:
    parts = relative_path.parts
    if len(parts) < 3:
        return None

    try:
        year = int(parts[0])
        month = int(parts[1])
    except ValueError:
        return None

    if not 1 <= month <= 12:
        return None

    return year, month


def is_in_selected_range(relative_path: Path, args: argparse.Namespace) -> bool:
    if args.from_year is None:
        return True

    year_month = extract_year_month(relative_path)
    if year_month is None:
        return False

    start = (args.from_year, args.from_month)
    end = (args.to_year, args.to_month)
    return start <= year_month <= end


def main() -> None:
    args = parse_args()
    validate_range_args(args)

    source_dir = Path(args.source_dir).expanduser().resolve()
    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")

    client = Minio(
        args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=False,
    )

    if not client.bucket_exists(args.bucket):
        client.make_bucket(args.bucket)
        print(f"Created bucket: {args.bucket}")

    files = source_dir.rglob(args.pattern) if args.recursive else source_dir.glob(args.pattern)

    uploaded = 0
    skipped = 0
    filtered_out = 0

    prefix = args.prefix.strip("/")

    for file_path in sorted(files):
        if not file_path.is_file():
            continue

        relative_path = file_path.relative_to(source_dir)
        if not is_in_selected_range(relative_path, args):
            filtered_out += 1
            continue

        relative_path_str = relative_path.as_posix()
        object_name = f"{prefix}/{relative_path_str}" if prefix else relative_path_str

        if args.skip_existing and object_exists(client, args.bucket, object_name):
            print(f"Skip existing: {object_name}")
            skipped += 1
            continue

        client.fput_object(args.bucket, object_name, str(file_path))
        print(f"Uploaded: {file_path} -> s3://{args.bucket}/{object_name}")
        uploaded += 1

    print(
        f"\nDone. Uploaded={uploaded}, Skipped={skipped}, "
        f"FilteredOut={filtered_out}, Bucket={args.bucket}"
    )


if __name__ == "__main__":
    main()
