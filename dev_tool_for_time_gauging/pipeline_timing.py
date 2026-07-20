#!/usr/bin/env python
"""
==============================================================================
  TEMPORARY DEV TOOL — DELETE THIS WHOLE FOLDER WHEN YOU ARE DONE.
  Not part of the pipeline. Nothing imports it. Safe to remove at any time.
==============================================================================

Measures how long each incoming 15-minute GDELT CSV takes to reach the SILVER
layer (ClickHouse), alongside how big that CSV was — so you can see how the
latency scales with size.

HOW IT MEASURES  (no ClickHouse credentials needed)
---------------------------------------------------
It watches the pipeline's own progress markers on the shared volume:

    ingestion   -> writes <slice>.<kind>.CSV      into  /data/raw/csv
    parsing     -> republishes the pair           into  /data/latest_files
                   and deletes the raw copy
    validation  -> appends to ClickHouse, then DELETES the pair from latest_files

So a slice's file *disappearing from latest_files* means it landed in silver.
That deletion is the moment this tool timestamps.

COLUMNS IN THE REPORT
---------------------
    csv                         which file it was: events / mentions /
                                translation.events / translation.mentions
    slice                       the GDELT 15-minute slice id (YYYYMMDDHHMMSS)
    gdelt_publish_utc           that slice id as a UTC time — GDELT's reference
                                publish time for the data
    rows                        number of rows in that CSV
    rows_source                 "raw"    = the original count, as downloaded
                                "latest" = counted after parsing's event filter
                                           (only if this tool started mid-flight
                                           and missed the raw file)
    seconds_gdelt_to_silver     gdelt_publish_utc -> landed in silver.
                                NOTE: this includes GDELT's own publishing lag
                                (a slice is typically available a few minutes
                                AFTER its timestamp) plus the poller's up-to-
                                15-minute wait. It is the true end-to-end
                                "age of the data" when it reaches silver.
    seconds_pipeline_to_silver  first seen by the pipeline -> landed in silver.
                                This is the processing time alone — the number
                                to plot against `rows`. (It still includes
                                parsing's back-pressure wait, since parsing only
                                publishes a new pair once latest_files is empty.)

RUNNING IT
----------
The pipeline's /data is a Docker volume, so run this inside a container that
mounts that volume read-only, with this folder bind-mounted for the report:

    VOL=$(docker volume ls -q | grep shared_data | head -1)
    docker run --rm -it \
      -v "$VOL":/data:ro \
      -v "$PWD/dev_tool_for_time_gauging":/out \
      -e DATA_DIR=/data -e OUT_DIR=/out \
      -w /out python:3.11-slim python pipeline_timing.py

Leave it running while the pipeline runs. It appends one line per CSV to
`pipeline_timing_report.csv` in this folder, and prints each measurement live.
Ctrl-C to stop. Re-running appends to the same report.
"""

import csv
import os
import time
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
RAW_DIR = DATA_DIR / "raw" / "csv"
LATEST_DIR = DATA_DIR / "latest_files"
OUT_DIR = Path(os.getenv("OUT_DIR", str(Path(__file__).resolve().parent)))
REPORT = OUT_DIR / "pipeline_timing_report.csv"
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1"))

COLUMNS = [
    "recorded_at_utc", "csv", "slice", "gdelt_publish_utc", "rows",
    "rows_source", "seconds_gdelt_to_silver", "seconds_pipeline_to_silver",
]


def classify(filename: str):
    """Return (slice_id, kind) for a GDELT drop, or (None, None) if it isn't one."""
    parts = filename.split(".")
    slice_id = parts[0]
    if len(slice_id) != 14 or not slice_id.isdigit():
        return None, None
    rest = ".".join(parts[1:]).lower()
    if "export" in rest:
        kind = "events"
    elif "mentions" in rest:
        kind = "mentions"
    elif "gkg" in rest:
        kind = "gkg"
    else:
        return None, None
    if "translation" in rest:
        kind = f"translation.{kind}"
    return slice_id, kind


def slice_to_utc(slice_id: str):
    """'20260514083000' -> aware UTC datetime (GDELT's reference publish time)."""
    try:
        return datetime.strptime(slice_id, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def count_rows(path: Path) -> int:
    """GDELT CSVs are header-less TSV, so a line count IS the row count."""
    try:
        with path.open("rb") as fh:
            return sum(1 for _ in fh)
    except OSError:
        return -1


def scan(directory: Path) -> dict:
    """{(slice, kind): Path} for the GDELT files currently in `directory`."""
    found: dict = {}
    if not directory.exists():
        return found
    try:
        for path in directory.iterdir():
            if not path.is_file() or path.name.startswith("."):
                continue
            slice_id, kind = classify(path.name)
            if slice_id:
                found[(slice_id, kind)] = path
    except OSError:
        pass
    return found


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if not REPORT.exists():
        with REPORT.open("w", newline="") as fh:
            csv.writer(fh).writerow(COLUMNS)

    rows_seen: dict = {}    # (slice, kind) -> (row_count, source)
    first_seen: dict = {}   # (slice, kind) -> when the pipeline first had it
    in_latest: set = set()  # (slice, kind) observed in latest_files at some point
    recorded: set = set()   # already written to the report

    print(f"[timing] watching {RAW_DIR}")
    print(f"[timing]      and {LATEST_DIR}")
    print(f"[timing] report -> {REPORT}\n")

    while True:
        now = datetime.now(timezone.utc)

        # 1. raw/csv — the ORIGINAL file: the right place to count rows.
        for key, path in scan(RAW_DIR).items():
            first_seen.setdefault(key, now)
            if key not in rows_seen:
                rows_seen[key] = (count_rows(path), "raw")

        # 2. latest_files — parsing's hand-off to validation.
        current = scan(LATEST_DIR)
        for key, path in current.items():
            first_seen.setdefault(key, now)
            in_latest.add(key)
            if key not in rows_seen:   # started mid-flight: count is post-filter
                rows_seen[key] = (count_rows(path), "latest")

        # 3. Was in latest_files, now gone => validation stored it in silver.
        for key in sorted(in_latest - set(current) - recorded):
            slice_id, kind = key
            published = slice_to_utc(slice_id)
            rows, source = rows_seen.get(key, (-1, "unknown"))
            gdelt_secs = round((now - published).total_seconds(), 1) if published else ""
            pipe_secs = (round((now - first_seen[key]).total_seconds(), 1)
                         if key in first_seen else "")

            with REPORT.open("a", newline="") as fh:
                csv.writer(fh).writerow([
                    now.isoformat(timespec="seconds"), kind, slice_id,
                    published.isoformat(timespec="seconds") if published else "",
                    rows, source, gdelt_secs, pipe_secs,
                ])
            print(f"[timing] {kind:<22} slice={slice_id}  rows={rows:<8} "
                  f"gdelt->silver={gdelt_secs}s  pipeline->silver={pipe_secs}s")
            recorded.add(key)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[timing] stopped.")
