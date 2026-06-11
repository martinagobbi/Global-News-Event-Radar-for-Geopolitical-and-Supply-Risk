#!/usr/bin/env python
"""
3-validation_and_storage/main.py
--------------------

Validation layer. Watches a `latest_files` directory (on the shared Docker
volume) into which the upstream layer drops the two GDELT files (events +
mentions) every 15 minutes. Each cycle it:

  1. Enforces a staleness health check and raises/clears the global error
     status: latest_files must receive new files at least every 35 minutes.
     (The number of files present is NOT an error condition.)
  2. When a fresh, valid pair is present: validates GLOBALEVENTID integrity,
     appends both tables to the wide-column store, deduplicates events, then
     deletes the two files (so latest_files holds at most two at a time).

The validation layer is the sole owner of the long-term store: it creates the
tables (ON CLUSTER) and runs the events dedup itself. The processing layer is
a pure reader and has no storage responsibilities.

Environment variables
---------------------
    LATEST_FILES_DIR    directory watched for incoming files (default /data/latest_files)
    WATCH_INTERVAL      seconds between scans               (default 30)
    STALE_LIMIT_SECONDS staleness threshold                (default 2100 = 35 min)
    CLICKHOUSE_HOST / PORT / DATABASE / USER / PASSWORD / CLUSTER
    STATUS_DIR          shared status dir                   (default /data/status)
"""

import logging
import os
import time
from pathlib import Path

from gdelt import is_valid_pair
from storage import Storage
from validator import validate_pair
import status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("validation")

LATEST_FILES_DIR = Path(os.getenv("LATEST_FILES_DIR", "/data/latest_files"))
WATCH_INTERVAL = int(os.getenv("WATCH_INTERVAL", "30"))
STALE_LIMIT_SECONDS = int(os.getenv("STALE_LIMIT_SECONDS", str(35 * 60)))
STARTUP_RETRY_DELAY = 5


def list_files() -> list[Path]:
    """Return the data files currently in latest_files (ignores temp/hidden)."""
    if not LATEST_FILES_DIR.exists():
        return []
    return sorted(
        p for p in LATEST_FILES_DIR.iterdir()
        if p.is_file() and not p.name.startswith(".")
    )


def deduplicate_events(storage: Storage) -> None:
    """
    Deduplicate gdelt_events after this append — the most-recent DATEADDED per
    GLOBALEVENTID wins, across both shards. Validation owns this directly.
    Best effort: ReplacingMergeTree also converges on its own at the next merge.
    """
    try:
        storage.optimize_events()
    except Exception as exc:  # noqa: BLE001 — dedup is best-effort here
        logger.warning("Events dedup failed (will converge at next merge): %s", exc)


def process_pair(paths, storage: Storage) -> None:
    """Validate + ingest one pair, then delete the two files."""
    summary = validate_pair(paths, storage)
    deduplicate_events(storage)
    for p in paths:
        try:
            Path(p).unlink()
        except OSError as exc:
            logger.warning("Could not delete %s: %s", p, exc)
    logger.info("Cycle complete: %s", summary)


def main() -> None:
    LATEST_FILES_DIR.mkdir(parents=True, exist_ok=True)

    # Connect to ClickHouse and ensure the wide-column tables exist, retrying
    # to tolerate docker-compose bring-up order.
    storage = Storage(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        database=os.getenv("CLICKHOUSE_DATABASE", "default"),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )
    while True:
        try:
            storage.ensure_tables()
            break
        except Exception as exc:  # noqa: BLE001
            logger.warning("ClickHouse not ready (%s) — retry in %ds…",
                           exc, STARTUP_RETRY_DELAY)
            time.sleep(STARTUP_RETRY_DELAY)

    last_new_file_time = time.monotonic()
    prev_names: set[str] = {p.name for p in list_files()}

    logger.info("Validation watcher started on %s (interval=%ds, stale_limit=%ds)",
                LATEST_FILES_DIR, WATCH_INTERVAL, STALE_LIMIT_SECONDS)

    while True:
        files = list_files()
        names = {p.name for p in files}

        # Track arrival of genuinely new files for the staleness check.
        if names - prev_names:
            last_new_file_time = time.monotonic()
        prev_names = names

        st = status.read_status()
        err_active = st.get("state") == "ERROR"
        snapshot = set(st.get("snapshot_files", []))

        # ── Clear: two files present, none of them from the error snapshot ────
        if err_active and len(files) == 2 and names.isdisjoint(snapshot):
            status.clear_error()
            err_active = False

        # ── Check: no new files for too long (the only failure condition) ─────
        # The number of files in latest_files is NOT an error condition.
        if time.monotonic() - last_new_file_time > STALE_LIMIT_SECONDS:
            status.set_error("stale_latest_files", names)

        # ── Process a fresh, valid pair ───────────────────────────────────────
        if len(files) == 2 and is_valid_pair(files):
            try:
                process_pair(files, storage)
                prev_names = {p.name for p in list_files()}  # post-delete
            except Exception as exc:  # noqa: BLE001
                logger.exception("Processing failed for %s: %s",
                                 [p.name for p in files], exc)

        time.sleep(WATCH_INTERVAL)


if __name__ == "__main__":
    main()
