from __future__ import annotations

import subprocess
import sys
from datetime import date, timedelta
from typing import Optional

from scripts.utils import load_yaml, today_in_tz


def call(cmd: list[str]) -> None:
    """
    Execute a subprocess and stop pipeline on failure.
    """
    print(f"\n>>> Running: {' '.join(cmd)}\n", flush=True)

    result = subprocess.run(cmd)

    if result.returncode != 0:
        print(f"\nERROR: command failed with exit code {result.returncode}")
        raise SystemExit(result.returncode)


def run(start: date, end: date) -> None:
    """
    Execute full pipeline sequence.
    """

    start_str = start.isoformat()
    end_str = end.isoformat()

    call([
        sys.executable,
        "scripts/ingest.py",
        "--start", start_str,
        "--end", end_str
    ])

    call([
        sys.executable,
        "scripts/transform_silver.py",
        "--start", start_str,
        "--end", end_str
    ])

    call([
        sys.executable,
        "scripts/build_gold.py"
    ])

    call([
        sys.executable,
        "scripts/build_site.py"
    ])

    print("\nPipeline completed successfully.\n")


def main(start: Optional[str], end: Optional[str]) -> None:
    """
    Entry point. Supports daily run or backfill.
    """

    cfg = load_yaml("config/pipeline.yml")

    tz_name = cfg["project"]["timezone"]

    if start:
        d0 = date.fromisoformat(start)
        d1 = date.fromisoformat(end) if end else d0
    else:
        today = today_in_tz(tz_name)

        # ingest yesterday (avoid partial day)
        d0 = today - timedelta(days=1)
        d1 = d0

    print(f"\nPipeline date range: {d0} → {d1}\n")

    run(d0, d1)


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start",
        required=False,
        help="YYYY-MM-DD"
    )

    parser.add_argument(
        "--end",
        required=False,
        help="YYYY-MM-DD"
    )

    args = parser.parse_args()

    main(args.start, args.end)
