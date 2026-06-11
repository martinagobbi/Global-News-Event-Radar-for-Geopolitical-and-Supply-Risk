"""
3-validation_and_storage/status.py
----------------------

The pipeline's GLOBAL error status.

Because every container mounts the same Docker volume (shared_data -> /data),
a single JSON file on that volume is a truly global signal: validation writes
it, and processing and serving read the exact same file. It is persistent
(survives a container restart) and needs no extra infrastructure.

File: /data/status/pipeline_status.json

    {
      "state": "ERROR" | "OK",
      "source": "3-validation_and_storage",
      "reason": "too_many_files" | "stale_latest_files",
      "triggered_at": "20260611091500",
      "snapshot_files": ["<file A>", "<file B>"]
    }

Only the validation layer writes this file. Writes are atomic (temp file +
os.replace) so a reader never sees a half-written document.
"""

import json
import logging
import os
import time
from pathlib import Path

logger = logging.getLogger("validation.status")

STATUS_DIR = Path(os.getenv("STATUS_DIR", "/data/status"))
STATUS_FILE = STATUS_DIR / "pipeline_status.json"


def _write(doc: dict) -> None:
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATUS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    os.replace(tmp, STATUS_FILE)


def read_status() -> dict:
    """Return the current status doc, or an OK default if none exists."""
    if not STATUS_FILE.exists():
        return {"state": "OK"}
    try:
        return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"state": "OK"}


def is_error() -> bool:
    return read_status().get("state") == "ERROR"


def set_error(reason: str, snapshot_files) -> None:
    """
    Raise the global ERROR status. snapshot_files records exactly what was in
    latest_files at the moment the error triggered; the error only clears once
    latest_files holds two files that are none of these.
    """
    if is_error():
        return  # keep the original trigger + snapshot
    doc = {
        "state": "ERROR",
        "source": "3-validation_and_storage",
        "reason": reason,
        "triggered_at": time.strftime("%Y%m%d%H%M%S"),
        "snapshot_files": sorted(snapshot_files),
    }
    _write(doc)
    logger.error("GLOBAL ERROR raised (%s); snapshot=%s",
                 reason, doc["snapshot_files"])


def clear_error() -> None:
    """Clear the global ERROR status (back to OK)."""
    if not is_error():
        return
    _write({"state": "OK", "source": "3-validation_and_storage",
            "cleared_at": time.strftime("%Y%m%d%H%M%S")})
    logger.info("GLOBAL ERROR cleared — pipeline OK")
