#!/usr/bin/env python
"""
2-parsing/main.py
-----------------
Parsing layer — a (near) raw file-based pass-through that turns the 15-minute GDELT files and drops them into latest_files for the
validation layer.

Flow
----
The ingestion poller drops the raw GDELT files into RAW_CSV_DIR (/data/raw/csv):
    <slice>.export.CSV     (events,   61 columns, tab-separated, no header)
    <slice>.mentions.CSV   (mentions, 16 columns, tab-separated, no header)
where <slice> is the GDELT 15-minute timestamp (e.g. 20260514083000).

This layer watches that directory and, for every slice whose events AND mentions
files are both present and stable, it:
    1. keeps only supply-chain-relevant events (parser.passes_filter),
    2. keeps ALL mentions raw (validation does the referential-integrity filter),
    3. writes the pair into LATEST_FILES_DIR (/data/latest_files) for layer 3,
    4. deletes the consumed source files from RAW_CSV_DIR.

A "slice" is identified by its 15-minute timestamp, which is uniform within one
GDELT file: DATEADDED (events, column 59) and MentionTimeDate (mentions, col 2).

Hand-off rules to layer 3 (validation):
    * tab-separated, header-less, official GDELT column order — matches
      gdelt.load_table();
    * atomic write (temp name -> rename), mentions renamed LAST, so the watcher
      never sees a half-written file;
    * back-pressure: a new pair is published only when latest_files is empty
      (the previous pair has been consumed), so validation — which can take
      minutes to enrich — is never overrun.

Environment variables
---------------------
    RAW_CSV_DIR              input dir   (default /data/raw/csv)
    LATEST_FILES_DIR         output dir  (default /data/latest_files)
    FILTER_EVENTS            keep only relevant events 1/0 (default 1)
    SCAN_INTERVAL_SECONDS    directory poll interval       (default 5)
    FILE_STABLE_SECONDS      min file age before reading   (default 3)
"""

import json
import logging
import os
import time
from pathlib import Path

import pandas as pd

from parser import passes_filter, GDELT_COLUMNS, MENTIONS_COLUMNS
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("parsing")
 
RAW_CSV_DIR      = Path(os.getenv("RAW_CSV_DIR",      "/data/raw/csv"))
LATEST_FILES_DIR = Path(os.getenv("LATEST_FILES_DIR", "/data/latest_files"))
FILTER_EVENTS    = os.getenv("FILTER_EVENTS", "1") == "1"
SCAN_INTERVAL    = int(os.getenv("SCAN_INTERVAL_SECONDS", "5"))
FILE_STABLE_SECS = int(os.getenv("FILE_STABLE_SECONDS", "3"))
 
EVENTS_SUFFIX   = ".export.CSV"
MENTIONS_SUFFIX = ".mentions.CSV"
 
 
def _slice_of(path: Path) -> str | None:
    """Return the 15-minute slice id from a GDELT filename, or None."""
    name = path.name
    if name.endswith(EVENTS_SUFFIX):
        return name[: -len(EVENTS_SUFFIX)]
    if name.endswith(MENTIONS_SUFFIX):
        return name[: -len(MENTIONS_SUFFIX)]
    return None
 
 
def _stable(path: Path) -> bool:
    """True if the file is old enough to be considered fully written."""
    try:
        return (time.time() - path.stat().st_mtime) >= FILE_STABLE_SECS
    except OSError:
        return False
 
 
def _ready_pairs() -> list[tuple[str, Path, Path]]:
    """Find slices whose events+mentions files are both present and stable."""
    events: dict[str, Path] = {}
    mentions: dict[str, Path] = {}
    if not RAW_CSV_DIR.exists():
        return []
    for p in RAW_CSV_DIR.iterdir():
        if not p.is_file() or p.name.startswith("."):
            continue
        sl = _slice_of(p)
        if sl is None:
            continue
        (events if p.name.endswith(EVENTS_SUFFIX) else mentions)[sl] = p
 
    pairs = []
    for sl in sorted(set(events) & set(mentions)):       # oldest slice first
        ev, mn = events[sl], mentions[sl]
        if _stable(ev) and _stable(mn):
            pairs.append((sl, ev, mn))
    return pairs
 
 
def _latest_files_empty() -> bool:
    """True if no pair is still pending in latest_files."""
    if not LATEST_FILES_DIR.exists():
        return True
    return not any(p.is_file() and not p.name.startswith(".")
                   for p in LATEST_FILES_DIR.iterdir())
 
 
def _atomic_write(df: pd.DataFrame, final: Path) -> None:
    """Write df as header-less TSV via a hidden temp file + rename."""
    tmp = final.with_name(f".{final.name}.tmp")
    df.to_csv(tmp, sep="\t", header=False, index=False)
    os.replace(tmp, final)
 
 
def process_pair(slice_id: str, ev_path: Path, mn_path: Path) -> None:
    """Filter events, keep mentions, publish the pair, delete the sources."""
    events_df = pd.read_csv(ev_path, sep="\t", header=None,
                            names=GDELT_COLUMNS, dtype=str,
                            keep_default_na=False, low_memory=False)
    mentions_df = pd.read_csv(mn_path, sep="\t", header=None,
                              names=MENTIONS_COLUMNS, dtype=str,
                              keep_default_na=False, low_memory=False)
 
    if FILTER_EVENTS:
        mask = events_df.apply(lambda r: passes_filter(r.to_dict()), axis=1)
        events_out = events_df[mask]
    else:
        events_out = events_df
 
    LATEST_FILES_DIR.mkdir(parents=True, exist_ok=True)
    # events first, mentions LAST: layer 3 only acts on a full pair.
    _atomic_write(events_out,   LATEST_FILES_DIR / f"{slice_id}{EVENTS_SUFFIX}")
    _atomic_write(mentions_df,  LATEST_FILES_DIR / f"{slice_id}{MENTIONS_SUFFIX}")
 
    # Parsing owns deletion of the consumed source files.
    ev_path.unlink(missing_ok=True)
    mn_path.unlink(missing_ok=True)
 
    logger.info("Published slice %s (events %d->%d, mentions %d)",
                slice_id, len(events_df), len(events_out), len(mentions_df))
 
 
def main() -> None:
    logger.info("Parsing (file-based) started — %s -> %s (filter=%s)",
                RAW_CSV_DIR, LATEST_FILES_DIR, "on" if FILTER_EVENTS else "off")
    while True:
        published = False
        for slice_id, ev, mn in _ready_pairs():
            if not _latest_files_empty():
                break  # previous pair not consumed yet — wait
            try:
                process_pair(slice_id, ev, mn)
                published = True
            except Exception as exc:
                logger.error("Failed to process slice %s: %s", slice_id, exc)
            break  # one pair at a time; rescan next loop
        if not published:
            time.sleep(SCAN_INTERVAL)
 
 
if __name__ == "__main__":
    main()
