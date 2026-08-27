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
     3. projects the retained columns and writes the pair into
         LATEST_FILES_DIR (/data/latest_files) for layer 3,
    4. deletes the consumed source files from RAW_CSV_DIR.

A "slice" is identified by its 15-minute timestamp, which is uniform within one
GDELT file: DATEADDED (events, column 59) and MentionTimeDate (mentions, col 2).

Hand-off rules to layer 3 (validation):
        * tab-separated, header-less, reduced GDELT column order — matches
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

    * bounded retries — a slice that raises PermanentError is dead-lettered at
      once; anything else is assumed transient and retried until TRANSIENT_MAX_WAIT
      has elapsed, then dead-lettered. MAX_SLICE_ATTEMPTS is logged alongside this
      but no longer decides it — a count cannot express "wait out a restart".
      Without this bound the same slice is retried forever, and because slices are
      handled oldest-first, EVERY later slice is blocked behind it.
    * orphan sweeps — two backstops, NOT how a partial slice is normally resolved:
      a slice missing its partner is published as a singleton on its very next
      scan (see _ready_slices()), so neither sweep exists to wait one out.
      _sweep_raw_orphans() catches a file in RAW_CSV_DIR stuck for some OTHER
      reason (unreadable, or left behind by an interrupted run) and dead-letters
      it after RAW_SLICE_ORPHAN_MAX_AGE. _sweep_parsed_orphans() catches the same
      failure on the published side — a manifest or data file left in
      LATEST_FILES_DIR that validation never consumed — after
      PARSED_SLICE_ORPHAN_MAX_AGE, so it cannot deadlock list_files() forever.
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
    MAX_SLICE_ATTEMPTS       attempt count, LOGGED ONLY — see TRANSIENT_MAX_WAIT (default 3)
    TRANSIENT_MAX_WAIT       seconds a transient failure may retry before dead-lettering (default 250)
    RAW_SLICE_ORPHAN_MAX_AGE     age at which raw data is abandoned (default 1800)
    PARSED_SLICE_ORPHAN_MAX_AGE  age at which parsed data left for validation is removed (default 720)
    BACKPRESSURE_MAX_WAIT    wait before reporting a stalled consumer (default 600)
"""

import csv
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from parser import (PermanentError, passes_filter, GDELT_COLUMNS, MENTIONS_COLUMNS,
                    PARSED_EVENT_COLUMNS, PARSED_MENTION_COLUMNS,
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
MAX_SLICE_ATTEMPTS    = int(os.getenv("MAX_SLICE_ATTEMPTS", "3"))   # log line only
# See 3-validation_and_storage/main.py for the reasoning: a deterministic failure
# is set aside at once (PermanentError), and a transient one is given a budget in
# SECONDS, because a count cannot express "wait out a restart".
TRANSIENT_MAX_WAIT    = int(os.getenv("TRANSIENT_MAX_WAIT", "250"))
RAW_SLICE_ORPHAN_MAX_AGE  = int(os.getenv("RAW_SLICE_ORPHAN_MAX_AGE", str(30 * 60)))
PARSED_SLICE_ORPHAN_MAX_AGE = int(os.getenv("PARSED_SLICE_ORPHAN_MAX_AGE", str(12 * 60)))
BACKPRESSURE_MAX_WAIT = int(os.getenv("BACKPRESSURE_MAX_WAIT", str(10 * 60)))

# slice_id -> monotonic time of its first TRANSIENT failure. In memory, unlike
# state["attempts"], because a restart should hand a slice a fresh budget rather
# than resume a stale clock.
_first_failure: dict[str, float] = {}

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
 
 
def _sweep_raw_orphans() -> None:
    """
    Dead-letter files that are sitting here unconsumed for far too long.

    This is now a backstop, not the handler for partial slices: singletons are
    published by _ready_slices(), so a lone file is normally consumed within one
    scan. Anything still present after RAW_SLICE_ORPHAN_MAX_AGE is therefore stuck for
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
            age = now - p.stat().st_ctime
        except OSError:
            continue
        if age > RAW_SLICE_ORPHAN_MAX_AGE:
            _dead_letter(sl, [p], f"unconsumed for {age / 60:.0f} min")

def _sweep_parsed_orphans() -> None:
    """
    Dead-letter files that were left for validation to find but never were.

    If unremoved, the files can block the pipeline entirely.

    ── Manifests are swept too ─────────────────────────────────────────────────
    _slice_of() reads GDELT data filenames and returns None for a .slice.json,
    so sweeping on it alone would carry the data files out and leave the manifest
    behind. That is not a tidier version of the same jam, it is a worse one:
    validation's list_files() finds the manifest, finds the files it names
    missing, and returns nothing — every scan, forever, because the manifest it
    is tripping over is the one file the sweep would never collect. The
    directory then stays non-empty, so publishing stays blocked, which is the
    exact deadlock this sweep exists to break.
    """
    if not LATEST_FILES_DIR.exists():
        return

    now = time.time()
    for p in LATEST_FILES_DIR.iterdir():
        if not p.is_file() or p.name.startswith("."):
            continue
        sl = _slice_of(p)
        if sl is None and p.name.endswith(MANIFEST_SUFFIX):
            sl = p.name[: -len(MANIFEST_SUFFIX)]
        if sl is None:
            continue
        try:
            age = now - p.stat().st_mtime
        except OSError:
            continue
        if age > PARSED_SLICE_ORPHAN_MAX_AGE:
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
 
 
MANIFEST_SUFFIX = ".slice.json"


def _write_manifest(slice_id: str, kinds: list[str]) -> None:
    """
    Declare, to the validation layer, exactly which files this slice consists of.

    ── Why a manifest and not a timing heuristic ────────────────────────────────
    A slice is published as up to TWO files, written by two separate os.replace()
    calls. Each rename is atomic on its own; the pair is not. A consumer scanning
    between them sees an events-only slice and — because a lone file is a
    legitimate final state here — cannot tell that apart from a slice that really
    does contain only events.

    Guessing from timing (is the file old enough to be settled?) answers the
    question with a probability rather than a fact, and silently reopens the race
    if the interval is ever retuned. This states the fact instead: parsing ALREADY
    KNOWS what it is publishing, because _ready_slices() hands it (slice_id, ev,
    mn) with either possibly None, and ingestion has already made that call final
    — a slice reaches /data/raw/csv only once it is complete or its retrieval
    deadline has passed, and ingestion never revisits it.

    ── Written LAST, deliberately ───────────────────────────────────────────────
    The manifest is the last thing written, so its existence proves every file it
    names is already in place. Validation therefore needs no waiting logic at
    all: no manifest means "not ready", and a manifest means "these files, all of
    them, now".

    Writing it FIRST would have expressed the same intent but created a new
    failure mode — a crash between manifest and data would leave validation
    waiting for files that never arrive, needing its own timeout to escape. The
    ordering makes the crash case self-correcting instead: the slice is simply
    not picked up, and the files sit until the orphan sweep collects them.
    """
    payload = {
        "slice_id": slice_id,
        "kinds": sorted(kinds),
        "files": sorted(f"{slice_id}{EVENTS_SUFFIX}" if k == "events"
                        else f"{slice_id}{MENTIONS_SUFFIX}" for k in kinds),
        "complete": sorted(kinds) == ["events", "mentions"],
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    final = LATEST_FILES_DIR / f"{slice_id}{MANIFEST_SUFFIX}"
    tmp = final.with_name(f".{final.name}.tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(tmp, final)
    logger.info("slice %s manifest: %s (complete=%s)",
                slice_id, "+".join(payload["kinds"]) or "none", payload["complete"])


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
        events_out = events_out[PARSED_EVENT_COLUMNS]

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
        mentions_df = mentions_df[PARSED_MENTION_COLUMNS]

    LATEST_FILES_DIR.mkdir(parents=True, exist_ok=True)
    # events first, mentions LAST — unchanged, and still true when only one exists.
    if events_out is not None:
        _atomic_write(events_out, LATEST_FILES_DIR / f"{slice_id}{EVENTS_SUFFIX}")
    if mentions_df is not None:
        _atomic_write(mentions_df, LATEST_FILES_DIR / f"{slice_id}{MENTIONS_SUFFIX}")

    # LAST. Everything above is now on disk, so this file appearing is what tells
    # validation the slice is whole — see _write_manifest.
    _kinds = ([ "events" ] if events_out is not None else []) + \
             ([ "mentions" ] if mentions_df is not None else [])
    _write_manifest(slice_id, _kinds)
 
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
        _sweep_raw_orphans()
        _sweep_parsed_orphans()
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
                    _first_failure.pop(slice_id, None)
                    _save_state(state)
                except PermanentError as exc:
                    # The raw file itself is malformed — wrong column count. No
                    # number of re-reads changes the bytes, and every retry costs
                    # a full parse of the slice while later slices queue behind
                    # this one.
                    logger.error("Permanently rejected slice %s: %s", slice_id, exc)
                    _dead_letter(slice_id, [ev, mn], f"permanently rejected: {exc}")
                    state["attempts"].pop(slice_id, None)
                    _first_failure.pop(slice_id, None)
                    _save_state(state)
                except Exception as exc:
                    # Assumed transient; bounded by elapsed time, not by a count.
                    attempts = state["attempts"].get(slice_id, 0) + 1
                    state["attempts"][slice_id] = attempts
                    first_seen = _first_failure.setdefault(slice_id, time.monotonic())
                    waited = time.monotonic() - first_seen
                    logger.error("Failed to process slice %s (attempt %d, %.0fs of %ds): %s",
                                 slice_id, attempts, waited, TRANSIENT_MAX_WAIT, exc)
                    if waited >= TRANSIENT_MAX_WAIT:
                        # Give up on this slice rather than blocking every later
                        # one behind it — bounded lateness, not infinite retry.
                        _dead_letter(slice_id, [ev, mn],
                                     f"{attempts} attempts over {waited:.0f}s")
                        state["attempts"].pop(slice_id, None)
                        _first_failure.pop(slice_id, None)
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
