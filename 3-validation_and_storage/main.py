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

Watermarking
------------
The staleness check above is a liveness watermark: it asserts that input has
stopped arriving, and reports it. It is complemented by bounded retries.

A pair that fails to validate is retried MAX_PAIR_ATTEMPTS times and then moved to
DEAD_LETTER_DIR. Without that bound, a pair that fails for any persistent reason —
a malformed file, a schema mismatch — is retried every WATCH_INTERVAL forever, and
because parsing publishes a new pair only once this directory is empty, the whole
pipeline stops behind it. Dead-lettering trades one lost slice for continued
progress, which is the same trade the bounded enrichment budget already makes.

Nothing is deleted: an abandoned pair is moved, so it can be inspected or replayed.

Environment variables
---------------------
    LATEST_FILES_DIR    directory watched for incoming files (default /data/latest_files)
    WATCH_INTERVAL      seconds between scans               (default 30)
    STALE_LIMIT_SECONDS staleness threshold                (default 2100 = 35 min)
    CLICKHOUSE_HOST / PORT / DATABASE / USER / PASSWORD / CLUSTER
    STATUS_DIR          shared status dir                   (default /data/status)
    STATE_DIR           durable attempt counts              (default /data/state)
    DEAD_LETTER_DIR     abandoned pairs                     (default /data/dead_letter)
    MAX_PAIR_ATTEMPTS   tries before dead-lettering         (default 3)
"""

import csv
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

from gdelt import is_processable
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

STATE_DIR = Path(os.getenv("STATE_DIR", "/data/state"))
ATTEMPTS_FILE = STATE_DIR / "validation_attempts.json"
DEAD_LETTER_DIR = Path(os.getenv("DEAD_LETTER_DIR", "/data/dead_letter"))
DEAD_LETTER_LOG_FILE = DEAD_LETTER_DIR / "dead_letter_log.csv"
MAX_DEAD_LETTER_LOG_ROWS = int(os.getenv("MAX_DEAD_LETTER_LOG_ROWS", "10000"))
MAX_PAIR_ATTEMPTS = int(os.getenv("MAX_PAIR_ATTEMPTS", "3"))
COMPLETE_SUFFIX = ".complete.json"


def _load_attempts() -> dict:
    """Attempt counts per pair, durable so a restart does not reset the budget."""
    try:
        with ATTEMPTS_FILE.open(encoding="utf-8") as fh:
            return dict(json.load(fh))
    except (OSError, ValueError):
        return {}


def _save_attempts(attempts: dict) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = ATTEMPTS_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(attempts, fh)
        os.replace(tmp, ATTEMPTS_FILE)
    except OSError as exc:
        logger.warning("Could not persist attempt counts: %s", exc)


def _append_dead_letter_log(file_path: Path | str) -> bool:
    """Append a single dead-letter file event into the retention CSV."""
    DEAD_LETTER_DIR.mkdir(parents=True, exist_ok=True)
    file_path = Path(file_path)
    header = ["file_name", "dead_letter_day", "dead_letter_time"]
    rows = [header]
    if DEAD_LETTER_LOG_FILE.exists() and DEAD_LETTER_LOG_FILE.stat().st_size > 0:
        with DEAD_LETTER_LOG_FILE.open("r", newline="", encoding="utf-8") as fh:
            existing = list(csv.reader(fh))
        rows = existing or [header]
        if not rows or rows[0] != header:
            rows = [header] + rows

    used = max(len(rows) - 1, 0)
    if used >= MAX_DEAD_LETTER_LOG_ROWS:
        logger.warning(
            "Dead-letter log full (%d/%d rows); skipping %s (Dead-lettered files and related rows are automatically deleted one year from creation)",
            used,
            MAX_DEAD_LETTER_LOG_ROWS,
            file_path.name,
        )
        return False

    now = datetime.now()
    rows.append([
        file_path.name,
        now.strftime("%Y-%m-%d"),
        now.strftime("%H:%M:%S"),
    ])
    with DEAD_LETTER_LOG_FILE.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerows(rows)
    logger.info(
        "Dead-letter log updated: %d/%d rows filled (Dead-lettered files and related rows are automatically deleted one year from creation)",
        len(rows) - 1,
        MAX_DEAD_LETTER_LOG_ROWS,
    )
    return True


def _dead_letter(paths, reason: str) -> None:
    """
    Move a pair out of latest_files so parsing can publish the next one. The
    files are MOVED, never deleted, so an abandoned slice stays inspectable.
    Each moved file is also recorded in the dead-letter CSV log.
    """
    key = Path(paths[0]).name.split(".")[0]
    target = DEAD_LETTER_DIR / key
    try:
        target.mkdir(parents=True, exist_ok=True)
        for p in paths:
            p = Path(p)
            if p.exists():
                os.replace(p, target / p.name)
                _append_dead_letter_log(target / p.name)
        logger.error("Pair %s abandoned after %s — moved to %s", key, reason, target)
    except OSError as exc:
        logger.error("Could not dead-letter %s (%s); deleting to unblock: %s",
                     key, exc, [Path(p).name for p in paths])
        for p in paths:
            Path(p).unlink(missing_ok=True)


def list_files() -> list[Path]:
    """Return handoff entries currently in latest_files (ignores temp/hidden)."""
    if not LATEST_FILES_DIR.exists():
        return []
    return sorted(
        p for p in LATEST_FILES_DIR.iterdir()
        if p.is_file() and not p.name.startswith(".")
    )


def list_manifests() -> list[Path]:
    """Return only parsing's atomically published slice-complete markers."""
    return sorted(
        p for p in LATEST_FILES_DIR.iterdir()
        if p.is_file() and p.name.endswith(COMPLETE_SUFFIX)
    ) if LATEST_FILES_DIR.exists() else []


def manifest_paths(manifest: Path) -> list[Path]:
    """Resolve the delivered data files declared by one completion marker."""
    with manifest.open(encoding="utf-8") as fh:
        payload = json.load(fh)
    paths = []
    for key in ("events", "mentions"):
        name = payload.get(key)
        if name:
            path = LATEST_FILES_DIR / name
            if not path.is_file():
                raise FileNotFoundError(f"{key} file declared by {manifest.name} is missing")
            paths.append(path)
    if not paths:
        raise ValueError(f"{manifest.name} declares no delivered files")
    return paths


def manifest_entries(manifest: Path) -> list[Path]:
    """Return a manifest plus any data files belonging to its slice."""
    slice_id = manifest.name.removesuffix(COMPLETE_SUFFIX)
    entries = [manifest]
    for path in list_files():
        if path.name.startswith(slice_id) and path != manifest:
            entries.append(path)
    return entries


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


def process_pair(paths, manifest: Path, storage: Storage) -> None:
    """Validate the manifest's complete slice, then delete its handoff entries."""
    summary = validate_pair(paths, storage)
    deduplicate_events(storage)
    for p in [*paths, manifest]:
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
    attempts = _load_attempts()

    logger.info("Validation watcher started on %s (interval=%ds, stale_limit=%ds, "
                "max_attempts=%d)",
                LATEST_FILES_DIR, WATCH_INTERVAL, STALE_LIMIT_SECONDS, MAX_PAIR_ATTEMPTS)

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

        # ── Clear: a fresh processable slice, none of it from the error snapshot
        # A partial slice clears the error just as a full one does — the point is
        # that new work arrived, not how many files it came in.
        manifests = list_manifests()

        if err_active and manifests and names.isdisjoint(snapshot):
            status.clear_error()
            err_active = False

        # ── Check: no new files for too long (the only failure condition) ─────
        # The number of files in latest_files is NOT an error condition.
        if time.monotonic() - last_new_file_time > STALE_LIMIT_SECONDS:
            status.set_error("stale_latest_files", names)

        # ── Process only a parsing-closed slice ──────────────────────────────
        # Data files alone are never sufficient: the completion manifest is
        # renamed into place only after parsing has waited its full companion
        # window and finished writing every delivered file.
        if manifests:
            manifest = manifests[0]
            key = manifest.name.removesuffix(COMPLETE_SUFFIX)
            try:
                data_paths = manifest_paths(manifest)
                if not is_processable(data_paths):
                    raise ValueError(f"Invalid data entries for {manifest.name}")
                process_pair(data_paths, manifest, storage)
                prev_names = {p.name for p in list_files()}  # post-delete
                if attempts.pop(key, None) is not None:
                    _save_attempts(attempts)
            except Exception as exc:  # noqa: BLE001
                n = attempts.get(key, 0) + 1
                attempts[key] = n
                logger.exception("Processing failed for %s (attempt %d/%d): %s",
                                 manifest.name, n, MAX_PAIR_ATTEMPTS, exc)
                if n >= MAX_PAIR_ATTEMPTS:
                    _dead_letter(manifest_entries(manifest), f"{n} failed attempts")
                    attempts.pop(key, None)
                    status.set_error("dead_letter", {key})
                    prev_names = {p.name for p in list_files()}
                _save_attempts(attempts)

        time.sleep(WATCH_INTERVAL)


if __name__ == "__main__":
    main()
