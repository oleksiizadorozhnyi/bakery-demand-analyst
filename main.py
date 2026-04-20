#!/usr/bin/env python3
"""Bakery Demand Analyst — CLI entry point.

Usage
-----
    python main.py --date 2026-01-10

The API server must be running before executing this script.
Start it with:
    python scripts/run_api.py
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date

from bakery_analyst.config import settings
from bakery_analyst.pipeline.runner import run

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the bakery demand analytics pipeline for a given date.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Target date in YYYY-MM-DD format (must exist in forecast_history).",
    )
    parser.add_argument(
        "--api-url",
        default=f"http://{settings.api_host}:{settings.api_port}",
        help="Base URL of the demand API (default: http://127.0.0.1:8000).",
    )
    parser.add_argument(
        "--analysis-out",
        default=None,
        help="Output path for the analysis CSV (default: out/analysis_YYYY-MM-DD.csv).",
    )
    parser.add_argument(
        "--report-out",
        default=None,
        help="Output path for the markdown report (default: out/report_YYYY-MM-DD.md).",
    )
    return parser.parse_args()


def _validate_date(date_str: str) -> None:
    if not _DATE_RE.match(date_str):
        print(f"[error] --date must be YYYY-MM-DD, got: {date_str!r}")
        sys.exit(1)
    try:
        date.fromisoformat(date_str)
    except ValueError:
        print(f"[error] Invalid calendar date: {date_str!r}")
        sys.exit(1)


def main() -> None:
    args = _parse_args()
    _validate_date(args.date)

    analysis_path = args.analysis_out or f"out/analysis_{args.date}.csv"
    report_path = args.report_out or f"out/report_{args.date}.md"

    exit_code = run(
        target_date=args.date,
        api_base_url=args.api_url,
        analysis_path=analysis_path,
        report_path=report_path,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
