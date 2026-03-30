from __future__ import annotations

from pathlib import Path

import requests


TAXI_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
TARGET_PATH = Path("/home/vanh/data/projects/bigdata/dataset/reference/taxi_zone_lookup.csv")


def main() -> None:
    TARGET_PATH.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(TAXI_ZONE_LOOKUP_URL, timeout=30)
    response.raise_for_status()
    TARGET_PATH.write_text(response.text, encoding="utf-8")
    print(f"Saved taxi zone lookup to: {TARGET_PATH}")


if __name__ == "__main__":
    main()
