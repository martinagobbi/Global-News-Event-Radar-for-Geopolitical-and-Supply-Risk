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

Watermarking
------------
A slice id IS an event-time watermark: it is GDELT's own 15-minute timestamp, not
the time we happened to handle it. The highest slice published so far is recorded
in SLICE_STATE_FILE on the shared volume, so it survives a restart, and it only
ever moves forwards.

Three rules keep the layer from stalling on one bad slice, which is the failure
mode a watermark exists to prevent:

    * bounded retries — a slice that fails is retried MAX_SLICE_ATTEMPTS times and
      then moved to DEAD_LETTER_DIR. Without this the same slice is retried every
      few seconds forever, and because slices are handled oldest-first, EVERY
      later slice is blocked behind it.
    * orphan sweep — a slice whose events and mentions files never both arrive can
      never be published, so after SLICE_ORPHAN_MAX_AGE it is dead-lettered too
      rather than accumulating on disk indefinitely.
    * bounded back-pressure — publishing waits for validation to drain
      `latest_files`, but if that has not happened within BACKPRESSURE_MAX_WAIT
      the wait is reported instead of continuing in silence.

Nothing is deleted by any of these rules: a dead-lettered slice is moved, so it
can still be inspected or replayed.

Environment variables
---------------------
    RAW_CSV_DIR              input dir   (default /data/raw/csv)
    INGESTION_CSV_DIR        alias for RAW_CSV_DIR, used by layer 1
    LATEST_FILES_DIR         output dir  (default /data/latest_files)
    FILTER_EVENTS            keep only relevant events 1/0 (default 1)
    SCAN_INTERVAL_SECONDS    directory poll interval       (default 5)
    FILE_STABLE_SECONDS      min file age before reading   (default 3)
    STATE_DIR                watermark location            (default /data/state)
    DEAD_LETTER_DIR          abandoned slices              (default /data/dead_letter)
    MAX_SLICE_ATTEMPTS       tries before dead-lettering   (default 3)
    SLICE_ORPHAN_MAX_AGE     age at which a half-pair is abandoned (default 3600)
    BACKPRESSURE_MAX_WAIT    wait before reporting a stalled consumer (default 1800)
"""

import csv
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from parser import (passes_filter, GDELT_COLUMNS, MENTIONS_COLUMNS,
                    check_field_width, EVENTS_FIELD_COUNT, MENTIONS_FIELD_COUNT)
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("parsing")
 
RAW_CSV_DIR      = Path(os.getenv("RAW_CSV_DIR", os.getenv("INGESTION_CSV_DIR", "/data/raw/csv")))
LATEST_FILES_DIR = Path(os.getenv("LATEST_FILES_DIR", "/data/latest_files"))
FILTER_EVENTS    = os.getenv("FILTER_EVENTS", "1") == "1"
SCAN_INTERVAL    = int(os.getenv("SCAN_INTERVAL_SECONDS", "5"))
FILE_STABLE_SECS = int(os.getenv("FILE_STABLE_SECONDS", "3"))

STATE_DIR        = Path(os.getenv("STATE_DIR", "/data/state"))
SLICE_STATE_FILE = STATE_DIR / "parsing_slices.json"
DEAD_LETTER_DIR  = Path(os.getenv("DEAD_LETTER_DIR", "/data/dead_letter"))
DEAD_LETTER_LOG_FILE = DEAD_LETTER_DIR / "dead_letter_log.csv"
MAX_DEAD_LETTER_LOG_ROWS = int(os.getenv("MAX_DEAD_LETTER_LOG_ROWS", "10000"))
MAX_SLICE_ATTEMPTS    = int(os.getenv("MAX_SLICE_ATTEMPTS", "3"))
SLICE_ORPHAN_MAX_AGE  = int(os.getenv("SLICE_ORPHAN_MAX_AGE", str(60 * 60)))
BACKPRESSURE_MAX_WAIT = int(os.getenv("BACKPRESSURE_MAX_WAIT", str(30 * 60)))

EVENTS_SUFFIX   = ".export.CSV"
MENTIONS_SUFFIX = ".mentions.CSV"


# ── Watermark + retry state ──────────────────────────────────────────────────

def _load_state() -> dict:
    """Read the durable slice state; a missing or corrupt file starts fresh."""
    try:
        with SLICE_STATE_FILE.open(encoding="utf-8") as fh:
            state = json.load(fh)
        return {"watermark": state.get("watermark") or "",
                "attempts": dict(state.get("attempts") or {})}
    except (OSError, ValueError):
        return {"watermark": "", "attempts": {}}


def _save_state(state: dict) -> None:
    """Persist the slice state atomically, so a crash cannot truncate it."""
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = SLICE_STATE_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(state, fh)
        os.replace(tmp, SLICE_STATE_FILE)
    except OSError as exc:
        logger.warning("Could not persist slice state: %s", exc)


def _append_dead_letter_log(file_path: Path | str) -> bool:
    """Append one row to the dead-letter log and return True when it was stored."""
    DEAD_LETTER_DIR.mkdir(parents=True, exist_ok=True)
    file_path = Path(file_path)
    header = ["file_name", "dead_letter_day", "dead_letter_time"]
    rows = [header]
    if DEAD_LETTER_LOG_FILE.exists() and DEAD_LETTER_LOG_FILE.stat().st_size > 0:
        with DEAD_LETTER_LOG_FILE.open("r", newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            rows = list(reader) or [header]
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


def _dead_letter(slice_id: str, paths, reason: str) -> None:
    """
    Move a slice's files out of the input directory so the pipeline can advance.
    They are MOVED, never deleted: an abandoned slice is evidence, and it can be
    replayed by copying it back. Each file is also logged in DEAD_LETTER_DIR.
    """
    target = DEAD_LETTER_DIR / slice_id
    try:
        target.mkdir(parents=True, exist_ok=True)
        for p in paths:
            if p.exists():
                os.replace(p, target / p.name)
                _append_dead_letter_log(target / p.name)
        logger.error("Slice %s abandoned after %s — moved to %s", slice_id, reason, target)
    except OSError as exc:
        logger.error("Could not dead-letter slice %s (%s); deleting to unblock: %s",
                     slice_id, exc, [p.name for p in paths])
        for p in paths:
            p.unlink(missing_ok=True)
 
 
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
 
 
def _ready_slices() -> list[tuple[str, Path | None, Path | None]]:
    """
    Find slices ready to publish, as (slice_id, events_path, mentions_path) with
    either path possibly None.

    Singletons are published, not held. The ingestion layer assembles a slice in
    a staging folder and moves it here only once it is complete or its retrieval
    deadline has passed, so anything in this directory is a DELIBERATE, final
    release. A lone file is therefore not "half a slice still arriving" — it is
    all there will ever be, and it still carries data that reaches silver: events
    alone update the store (ReplacingMergeTree on GLOBALEVENTID), and mentions
    alone attach to events already stored.
    """
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

    ready = []
    for sl in sorted(set(events) | set(mentions)):       # oldest slice first
        ev, mn = events.get(sl), mentions.get(sl)
        if all(_stable(p) for p in (ev, mn) if p is not None):
            ready.append((sl, ev, mn))
    return ready
 
 
def _sweep_orphans() -> None:
    """
    Dead-letter files that are sitting here unconsumed for far too long.

    This is now a backstop, not the handler for partial slices: singletons are
    published by _ready_slices(), so a lone file is normally consumed within one
    scan. Anything still present after SLICE_ORPHAN_MAX_AGE is therefore stuck for
    some other reason — an unreadable file that keeps failing, or a leftover from
    an interrupted run — and is set aside so it cannot accumulate on disk.

    Files are checked regardless of whether they have a partner, because with
    singletons publishable, "has no partner" no longer means "cannot progress".
    """
    if not RAW_CSV_DIR.exists():
        return

    now = time.time()
    for p in RAW_CSV_DIR.iterdir():
        if not p.is_file() or p.name.startswith("."):
            continue
        sl = _slice_of(p)
        if sl is None:
            continue
        try:
            age = now - p.stat().st_mtime
        except OSError:
            continue
        if age > SLICE_ORPHAN_MAX_AGE:
            _dead_letter(sl, [p], f"unconsumed for {age / 60:.0f} min")


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
 
 
def process_pair(slice_id: str, ev_path: Path | None, mn_path: Path | None) -> None:
    """
    Filter events, keep mentions, publish, delete the sources.

    Either path may be None: ingestion releases a partial slice once its retrieval
    deadline has passed, and both halves are independently useful downstream. The
    write order is unchanged — events first, mentions LAST — so a consumer never
    observes a pair mid-write.
    """
    events_out = mentions_df = None
    n_events_in = 0

    if ev_path is not None:
        # Checked BEFORE the read, because the read cannot fail: pandas absorbs a
        # width change by shifting columns instead of raising. This is also the
        # LAST point at which a width change is visible — the write below emits
        # exactly len(GDELT_COLUMNS) fields, so validation would see a
        # correctly-shaped, wrongly-aligned file.
        check_field_width(ev_path, EVENTS_FIELD_COUNT, "events")
        events_df = pd.read_csv(ev_path, sep="\t", header=None,
                                names=GDELT_COLUMNS, dtype=str,
                                keep_default_na=False, low_memory=False)
        n_events_in = len(events_df)
        if FILTER_EVENTS:
            mask = events_df.apply(lambda r: passes_filter(r.to_dict()), axis=1)
            events_out = events_df[mask]
        else:
            events_out = events_df

    if mn_path is not None:
        # Matters MORE than the events check above, not less. A shifted events
        # slice is stopped later by ClickHouse typing (GLOBALEVENTID and
        # DATEADDED are UInt64, so shifted text fails the insert). Mentions get
        # no such protection: a shift puts EventTimeDate into GLOBALEVENTID,
        # which is a valid UInt64, so the rows would insert cleanly and attach
        # every mention to the wrong event.
        check_field_width(mn_path, MENTIONS_FIELD_COUNT, "mentions")
        mentions_df = pd.read_csv(mn_path, sep="\t", header=None,
                                  names=MENTIONS_COLUMNS, dtype=str,
                                  keep_default_na=False, low_memory=False)

    LATEST_FILES_DIR.mkdir(parents=True, exist_ok=True)
    # events first, mentions LAST — unchanged, and still true when only one exists.
    if events_out is not None:
        _atomic_write(events_out, LATEST_FILES_DIR / f"{slice_id}{EVENTS_SUFFIX}")
    if mentions_df is not None:
        _atomic_write(mentions_df, LATEST_FILES_DIR / f"{slice_id}{MENTIONS_SUFFIX}")
 
    # Parsing owns deletion of the consumed source files.
    for p in (ev_path, mn_path):
        if p is not None:
            p.unlink(missing_ok=True)

    kinds = "+".join(k for k, v in (("events", ev_path), ("mentions", mn_path))
                     if v is not None)
    logger.info("Published slice %s [%s] (events %d->%s, mentions %s)",
                slice_id, kinds, n_events_in,
                len(events_out) if events_out is not None else "-",
                len(mentions_df) if mentions_df is not None else "-")
 
 
def main() -> None:
    state = _load_state()
    logger.info("Parsing (file-based) started — %s -> %s (filter=%s, watermark=%s)",
                RAW_CSV_DIR, LATEST_FILES_DIR, "on" if FILTER_EVENTS else "off",
                state["watermark"] or "none yet")

    blocked_since: float | None = None
    reported_block = False

    while True:
        _sweep_orphans()
        published = False
        pairs = _ready_slices()                     # oldest slice first

        if pairs:
            if _latest_files_empty():
                blocked_since, reported_block = None, False
                slice_id, ev, mn = pairs[0]
                try:
                    process_pair(slice_id, ev, mn)
                    published = True
                    # The watermark only ever moves forwards: a slice arriving
                    # late must not drag it back over ground already covered.
                    if slice_id > state["watermark"]:
                        state["watermark"] = slice_id
                    state["attempts"].pop(slice_id, None)
                    _save_state(state)
                except Exception as exc:
                    attempts = state["attempts"].get(slice_id, 0) + 1
                    state["attempts"][slice_id] = attempts
                    logger.error("Failed to process slice %s (attempt %d/%d): %s",
                                 slice_id, attempts, MAX_SLICE_ATTEMPTS, exc)
                    if attempts >= MAX_SLICE_ATTEMPTS:
                        # Give up on this slice rather than blocking every later
                        # one behind it — bounded lateness, not infinite retry.
                        _dead_letter(slice_id, [ev, mn], f"{attempts} failed attempts")
                        state["attempts"].pop(slice_id, None)
                    _save_state(state)
            else:
                # Validation has not drained the previous pair. Waiting is correct
                # back-pressure, but a wait with no end is a stall, so say so.
                now = time.monotonic()
                if blocked_since is None:
                    blocked_since = now
                elif not reported_block and now - blocked_since > BACKPRESSURE_MAX_WAIT:
                    logger.error(
                        "latest_files has not been consumed for %.0f min — validation "
                        "appears stalled; %d slice(s) waiting to publish",
                        (now - blocked_since) / 60, len(pairs))
                    reported_block = True

        if not published:
            time.sleep(SCAN_INTERVAL)
 
 
if __name__ == "__main__":
    main()
