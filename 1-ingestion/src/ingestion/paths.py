"""Shared filesystem paths for the ingestion layer.

The parsing layer reads raw CSV files from ``/data/raw/csv`` in Docker. Keep
that path stable: it is the hand-off contract between layer 1 and layer 2.
"""

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _data_root() -> Path:
    configured = os.getenv("INGESTION_DATA_DIR")
    if configured:
        return Path(configured)
    if Path("/data").exists():
        return Path("/data")
    return PROJECT_ROOT / "data"


DATA_DIR = _data_root()
RAW_DIR = DATA_DIR / "raw"
RAW_ZIP_DIR = RAW_DIR / "zip"
RAW_CSV_DIR = RAW_DIR / "csv"
STATE_DIR = DATA_DIR / "state"
STATE_FILE = STATE_DIR / "last_seen.json"


def ensure_ingestion_dirs() -> None:
    RAW_ZIP_DIR.mkdir(parents=True, exist_ok=True)
    RAW_CSV_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
