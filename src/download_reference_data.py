from __future__ import annotations

import argparse
from pathlib import Path

import requests

from project_config import DEFAULT_ZONE_LOOKUP_PATH


TAXI_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def main() -> None:
    parser = argparse.ArgumentParser(description="Download NYC taxi zone reference data.")
    parser.add_argument("--output", default=DEFAULT_ZONE_LOOKUP_PATH, help="Output CSV path")
    args = parser.parse_args()

    target_path = Path(args.output).expanduser().resolve()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(TAXI_ZONE_LOOKUP_URL, timeout=30)
    response.raise_for_status()
    target_path.write_text(response.text, encoding="utf-8")
    print(f"Saved taxi zone lookup to: {target_path}")


if __name__ == "__main__":
    main()
