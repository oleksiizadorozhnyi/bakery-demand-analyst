#!/usr/bin/env python3
"""Download the French Bakery Daily Sales dataset via kagglehub.

Usage
-----
    python scripts/download_data.py

kagglehub handles authentication automatically — on first run it opens a
browser to log in to Kaggle; subsequent runs use the cached token.

The CSV is copied to data/raw/bakery_sales.csv, which is the path
expected by the semi-synthetic seeder.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

DEST = Path("data/raw/bakery_sales.csv")
DATASET = "matthieugimbert/french-bakery-daily-sales"


def download() -> None:
    try:
        import kagglehub
    except ImportError:
        print("[error] kagglehub is not installed. Run: pip install kagglehub", file=sys.stderr)
        sys.exit(1)

    if DEST.exists():
        print(f"[download_data] Already exists: {DEST} — skipping download.")
        print("  Delete the file and re-run to force a fresh download.")
        return

    print(f"[download_data] Downloading '{DATASET}' via kagglehub …")
    dataset_dir = Path(kagglehub.dataset_download(DATASET))
    print(f"[download_data] Downloaded to cache: {dataset_dir}")

    csv_files = list(dataset_dir.rglob("*.csv"))
    if not csv_files:
        print(f"[error] No CSV files found in {dataset_dir}", file=sys.stderr)
        sys.exit(1)

    source = csv_files[0]
    DEST.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, DEST)

    print(f"[download_data] Copied: {source.name} → {DEST.resolve()}")
    print("[download_data] Done. You can now run: make seed-semi")


if __name__ == "__main__":
    download()
