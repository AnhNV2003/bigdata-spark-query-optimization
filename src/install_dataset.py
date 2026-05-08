import argparse
import time
from pathlib import Path

import requests

from project_config import PROJECT_ROOT

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download NYC taxi parquet data.")
    parser.add_argument("--dataset-type", default="yellow", choices=["yellow", "green", "fhv", "hvfhv"])
    parser.add_argument("--from-year", type=int, default=2025)
    parser.add_argument("--to-year", type=int, default=2025)
    parser.add_argument("--from-month", type=int, default=1)
    parser.add_argument("--to-month", type=int, default=12)
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "dataset/parquet"))
    return parser.parse_args()


def download_file(url: str, path: Path, retries: int = 3) -> bool:
    for attempt in range(retries):
        try:
            with requests.get(url, stream=True, timeout=30) as r:
                if r.status_code != 200:
                    raise Exception(f"HTTP {r.status_code}")

                with path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

            return True
        except Exception as exc:
            print(f"Retry {attempt + 1}/{retries} failed: {exc}")
            time.sleep(2)

    return False


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    for year in range(args.from_year, args.to_year + 1):
        start_month = args.from_month if year == args.from_year else 1
        end_month = args.to_month if year == args.to_year else 12

        for month in range(start_month, end_month + 1):
            mm = f"{month:02d}"
            filename = f"{args.dataset_type}_tripdata_{year}-{mm}.parquet"
            url = f"{BASE_URL}/{filename}"
            month_dir = output_dir / str(year) / mm
            month_dir.mkdir(parents=True, exist_ok=True)
            filepath = month_dir / filename

            if filepath.exists():
                print(f"Skip (exists): {filename}")
                continue

            print(f"Downloading: {filename}")
            success = download_file(url, filepath)

            if success:
                print(f"Done: {filename}")
            else:
                print(f"Failed: {filename}")


if __name__ == "__main__":
    main()
