from __future__ import annotations

import argparse
from pathlib import Path

from minio import Minio
from minio.error import S3Error

from project_config import (
    DEFAULT_MINIO_ACCESS_KEY,
    DEFAULT_MINIO_BUCKET,
    DEFAULT_MINIO_ENDPOINT,
    DEFAULT_MINIO_SECRET_KEY,
    PROJECT_ROOT,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload local parquet files to MinIO.")
    parser.add_argument("--source-dir", default=str(PROJECT_ROOT / "dataset/parquet"))
    parser.add_argument("--bucket",     default=DEFAULT_MINIO_BUCKET)
    parser.add_argument("--endpoint",   default=DEFAULT_MINIO_ENDPOINT)
    parser.add_argument("--access-key", default=DEFAULT_MINIO_ACCESS_KEY)
    parser.add_argument("--secret-key", default=DEFAULT_MINIO_SECRET_KEY)
    parser.add_argument("--from-year",  type=int, default=2020)
    parser.add_argument("--from-month", type=int, default=1)
    parser.add_argument("--to-year",    type=int, default=2025)
    parser.add_argument("--to-month",   type=int, default=12)
    parser.add_argument("--skip-existing", action="store_true")
    return parser.parse_args()


def in_range(file_path: Path, source_dir: Path, from_ym: tuple, to_ym: tuple) -> bool:
    parts = file_path.relative_to(source_dir).parts
    if len(parts) < 3:
        return False
    try:
        ym = (int(parts[0]), int(parts[1]))
        return from_ym <= ym <= to_ym
    except ValueError:
        return False


def object_exists(client: Minio, bucket: str, name: str) -> bool:
    try:
        client.stat_object(bucket, name)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        raise


def main() -> None:
    args = parse_args()

    source_dir = Path(args.source_dir).resolve()
    from_ym = (args.from_year, args.from_month)
    to_ym   = (args.to_year,   args.to_month)

    endpoint = args.endpoint.removeprefix("http://").removeprefix("https://")
    client = Minio(endpoint, 
                   access_key=args.access_key, 
                   secret_key=args.secret_key, 
                   secure=False)

    if not client.bucket_exists(args.bucket):
        client.make_bucket(args.bucket)
        print(f"Created bucket: {args.bucket}")

    uploaded = skipped = filtered = 0

    for file_path in sorted(source_dir.rglob("*.parquet")):
        if not file_path.is_file():
            continue

        if not in_range(file_path, source_dir, from_ym, to_ym):
            filtered += 1
            continue

        object_name = file_path.relative_to(source_dir).as_posix()

        if args.skip_existing and object_exists(client, args.bucket, object_name):
            skipped += 1
            continue

        client.fput_object(args.bucket, object_name, str(file_path))
        print(f"Uploaded: {object_name}")
        uploaded += 1

    print(f"\nDone \nUploaded={uploaded}, \nSkipped={skipped}, \nFilteredOut={filtered}, \nBucket={args.bucket}")


if __name__ == "__main__":
    main()
