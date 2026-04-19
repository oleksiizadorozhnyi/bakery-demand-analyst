#!/usr/bin/env python3
"""Seed the bakery SQLite database.

Usage
-----
    python scripts/seed_db.py                          # fully synthetic (default)
    python scripts/seed_db.py --mode semi_synthetic    # real bakery + weather data
    python scripts/seed_db.py --mode synthetic --force # wipe and regenerate
"""

from __future__ import annotations

import argparse
import sys
from typing import Literal

sys.path.insert(0, ".")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed the bakery analytics database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete all existing rows and regenerate data from scratch.",
    )
    parser.add_argument(
        "--mode",
        choices=["synthetic", "semi_synthetic"],
        default=None,
        help=(
            "Data generation mode. "
            "'synthetic' uses parameterised demand; "
            "'semi_synthetic' uses real French Bakery + Open-Meteo weather data. "
            "Defaults to the SEED_MODE env var, or 'synthetic' if unset."
        ),
    )
    args = parser.parse_args()

    # CLI flag takes precedence over env/config
    if args.mode == "semi_synthetic":
        from bakery_analyst.db.seed_semi import seed_database
    else:
        # also covers args.mode == "synthetic" and args.mode is None (default)
        from bakery_analyst.db.seed import seed_database  # type: ignore[assignment]

    seed_database(force=args.force)


if __name__ == "__main__":
    main()
