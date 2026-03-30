import time
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DATASET_TYPE = "yellow"  # đổi thành green / fhv / hvfhv nếu muốn
YEARS = range(2009, 2026)
MONTHS = range(1, 13)

OUTPUT_DIR = Path("/home/vanh/data/projects/bigdata/dataset/parquet")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


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


for year in YEARS:
    for month in MONTHS:
        mm = f"{month:02d}"
        filename = f"{DATASET_TYPE}_tripdata_{year}-{mm}.parquet"
        url = f"{BASE_URL}/{filename}"
        month_dir = OUTPUT_DIR / str(year) / mm
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
