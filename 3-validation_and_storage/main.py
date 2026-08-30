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

from gdelt import is_processable, PermanentError
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
# Matches 2-parsing/main.py's FILE_STABLE_SECONDS. See list_files().
FILE_STABLE_SECONDS = int(os.getenv("FILE_STABLE_SECONDS", "3"))
STALE_LIMIT_SECONDS = int(os.getenv("STALE_LIMIT_SECONDS", str(35 * 60)))
STARTUP_RETRY_DELAY = 5

STATE_DIR = Path(os.getenv("STATE_DIR", "/data/state"))
ATTEMPTS_FILE = STATE_DIR / "validation_attempts.json"
DEAD_LETTER_DIR = Path(os.getenv("DEAD_LETTER_DIR", "/data/dead_letter"))
DEAD_LETTER_LOG_FILE = DEAD_LETTER_DIR / "dead_letter_log.csv"
MAX_DEAD_LETTER_LOG_ROWS = int(os.getenv("MAX_DEAD_LETTER_LOG_ROWS", "10000"))
# Retained for the log line only; the dead-letter decision is now made on
# ELAPSED TIME (below), because a count says nothing about how long a store has
# been unavailable.
MAX_PAIR_ATTEMPTS = int(os.getenv("MAX_PAIR_ATTEMPTS", "3"))

# How long a pair may keep failing TRANSIENTLY before it is set aside. Sized
# against what it is meant to survive: a ClickHouse or Keeper restart, which the
# stores take tens of seconds to recover from, plus margin. Deterministic
# failures never reach this budget — they raise PermanentError and are set aside
# on the first attempt.
TRANSIENT_MAX_WAIT = int(os.getenv("TRANSIENT_MAX_WAIT", "250"))


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

    A pair can reach here with its files already gone — parsing's own orphan
    sweep (2-parsing/main.py's _sweep_parsed_orphans) can claim the same slice
    out from under a validation attempt that is still retrying it. That is not
    a new abandonment, just a stale one discovered late, so nothing is moved
    and no "abandoned... moved to" claim is logged for a slice this function
    never actually touched.
    """
    key = Path(paths[0]).name.split(".")[0]
    target = DEAD_LETTER_DIR / key
    # Include the manifest, or latest_files never empties and parsing's
    # back-pressure blocks every later slice behind this one.
    paths = list(paths) + [LATEST_FILES_DIR / f"{key}{MANIFEST_SUFFIX}"]
    moved = []
    try:
        for p in paths:
            p = Path(p)
            if p.exists():
                target.mkdir(parents=True, exist_ok=True)
                os.replace(p, target / p.name)
                _append_dead_letter_log(target / p.name)
                moved.append(p.name)
        if moved:
            logger.error("Pair %s abandoned after %s — moved to %s", key, reason, target)
        else:
            logger.info("Pair %s already gone from latest_files (likely swept by "
                        "parsing already) — nothing to dead-letter", key)
    except OSError as exc:
        logger.error("Could not dead-letter %s (%s); deleting to unblock: %s",
                     key, exc, [Path(p).name for p in paths])
        for p in paths:
            Path(p).unlink(missing_ok=True)


MANIFEST_SUFFIX = ".slice.json"


def read_manifest(slice_id: str) -> dict | None:
    """
    The manifest parsing wrote for this slice, or None if it is not there yet.

    Parsing writes the manifest LAST, after every data file it intends to
    publish, so its presence is the completeness signal: no manifest means the
    slice is still being written or was never published; a manifest means the
    files it names are all on disk now.
    """
    path = LATEST_FILES_DIR / f"{slice_id}{MANIFEST_SUFFIX}"
    try:
        with path.open(encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def list_files() -> list[Path]:
    """
    The data files of the one slice parsing has declared ready — or [] if none.

    ── Why a manifest and not a file-age heuristic ──────────────────────────────
    A slice is up to TWO files, published by two separate os.replace() calls.
    Each rename is atomic; the pair is not. A scan landing between them sees an
    events-only slice — and because a lone file is a LEGITIMATE final state here
    (ingestion deliberately releases partial slices once its retrieval deadline
    passes), the directory alone cannot distinguish that from a slice which
    really does contain only events.

    Judging by file age answers this with a probability rather than a fact: it
    guesses that "too new" means "still being written", it silently reopens the
    race if the interval is ever retuned, and it delays every slice by the
    threshold even when nothing is racing.

    The manifest answers it with a fact. Parsing knows what it is publishing
    BEFORE it publishes anything — _ready_slices() hands it (slice_id, events,
    mentions) with either possibly None, and ingestion has already made that
    determination final. So parsing simply states it.

    Data files are returned only when the manifest exists AND every file it names
    is present. The manifest is not itself a data file; process_pair deletes it
    with the rest.
    """
    if not LATEST_FILES_DIR.exists():
        return []

    manifests = sorted(LATEST_FILES_DIR.glob(f"*{MANIFEST_SUFFIX}"))
    if not manifests:
        return []

    # ── Supersession: a newer slice landed before this one was resolved ────────
    # Back-pressure means parsing publishes only once latest_files is empty, so
    # more than one manifest here should never happen. If it does anyway — the
    # only known path today is parsing's own orphan sweep clearing a slow
    # slice's files out from under this layer, but the check does not assume
    # that's the only cause — a newer manifest already being on disk IS proof
    # the older one is no longer this layer's to finish. Dead-letter it right
    # away, with a reason distinct from the ordinary permanent/transient paths,
    # rather than let a stale retry (or a read against files that may already be
    # gone) run against it.
    while len(manifests) > 1:
        stale = manifests.pop(0)
        stale_id = stale.name[: -len(MANIFEST_SUFFIX)]
        stale_manifest = read_manifest(stale_id)
        stale_paths = [LATEST_FILES_DIR / n
                       for n in (stale_manifest or {}).get("files", [])]
        stale_paths = [p for p in stale_paths if p.is_file()] or [stale]
        newer_id = manifests[0].name[: -len(MANIFEST_SUFFIX)]
        logger.error("slice %s superseded by %s before it was appended or "
                     "dead-lettered — abandoning it", stale_id, newer_id)
        _dead_letter(stale_paths, f"superseded by newer slice {newer_id} "
                                  "published before this one finished")

    slice_id = manifests[0].name[: -len(MANIFEST_SUFFIX)]
    manifest = read_manifest(slice_id)
    if manifest is None:
        return []

    expected = [LATEST_FILES_DIR / n for n in manifest.get("files", [])]
    missing = [q.name for q in expected if not q.is_file()]
    if missing:
        # Should be impossible: the manifest is written last. Report rather than
        # process a subset, which is the exact failure this exists to prevent.
        logger.warning("slice %s manifest lists %s but %s absent — waiting",
                       slice_id, manifest.get("files"), missing)
        return []
    return sorted(expected)


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
    # The manifest is consumed with its slice. Left behind it would advertise a
    # slice whose data files are gone, and list_files() would log "manifest lists
    # X but X absent" on every poll for ever.
    if paths:
        _sid = Path(paths[0]).name.split(".")[0]
        (LATEST_FILES_DIR / f"{_sid}{MANIFEST_SUFFIX}").unlink(missing_ok=True)
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
    attempts = _load_attempts()
    # key -> monotonic time of its FIRST transient failure. In memory only: a
    # restart resets the budget, which is the safe direction (a slice gets a
    # fresh 10 minutes rather than being dead-lettered by a stale timestamp).
    first_failure: dict[str, float] = {}

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
        if err_active and files and is_processable(files) and names.isdisjoint(snapshot):
            status.clear_error()
            err_active = False

        # ── Check: no new files for too long (the only failure condition) ─────
        # The number of files in latest_files is NOT an error condition.
        if time.monotonic() - last_new_file_time > STALE_LIMIT_SECONDS:
            status.set_error("stale_latest_files", names)

        # ── Process a fresh, valid slice (a pair, or a single file) ───────────
        # Parsing publishes a lone file when ingestion released a partial slice
        # after its retrieval deadline. Both halves are independently useful:
        # events update the store on their own, and mentions attach to events
        # already stored. Only an unrecognisable file is rejected.
        if files and is_processable(files):
            key = files[0].name.split(".")[0]
            try:
                process_pair(files, storage)
                prev_names = {p.name for p in list_files()}  # post-delete
                first_failure.pop(key, None)
                if attempts.pop(key, None) is not None:
                    _save_attempts(attempts)
            except PermanentError as exc:
                # Deterministic: the bytes on disk are wrong, and reading them
                # again cannot change that. Retrying would re-run the whole cycle
                # — including the enrichment scrape — twice more to reach an
                # identical conclusion, while parsing stays blocked behind this
                # pair (it publishes only when latest_files is empty).
                logger.error("Permanently rejected %s: %s",
                             [p.name for p in files], exc)
                _dead_letter(files, f"permanently rejected: {exc}")
                attempts.pop(key, None)
                status.set_error("dead_letter", {key})
                prev_names = {p.name for p in list_files()}
                _save_attempts(attempts)
            except Exception as exc:  # noqa: BLE001
                # Assumed TRANSIENT — a store still starting, a dropped
                # connection, a scrape that timed out. Bounded by ELAPSED TIME
                # rather than a count: three attempts at WATCH_INTERVAL apart is
                # only ~90 s, which is shorter than a ClickHouse restart, so a
                # count-bounded budget dead-lettered slices that a few more
                # seconds would have saved. The count is still tracked, for the
                # log line only.
                n = attempts.get(key, 0) + 1
                first_seen = first_failure.setdefault(key, time.monotonic())
                waited = time.monotonic() - first_seen
                attempts[key] = n
                logger.exception("Processing failed for %s (attempt %d, %.0fs of %ds): %s",
                                 [p.name for p in files], n, waited,
                                 TRANSIENT_MAX_WAIT, exc)
                if waited >= TRANSIENT_MAX_WAIT:
                    # Set the pair aside rather than retrying it forever: parsing
                    # publishes only when this directory is empty, so a pair that
                    # can never succeed would halt the whole pipeline behind it.
                    _dead_letter(files, f"{n} attempts over {waited:.0f}s")
                    attempts.pop(key, None)
                    first_failure.pop(key, None)
                    status.set_error("dead_letter", {key})
                    prev_names = {p.name for p in list_files()}
                _save_attempts(attempts)

        time.sleep(WATCH_INTERVAL)


if __name__ == "__main__":
    main()
