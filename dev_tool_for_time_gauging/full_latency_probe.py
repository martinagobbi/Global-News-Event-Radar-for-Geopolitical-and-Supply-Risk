#!/usr/bin/env python
"""
==============================================================================
  MEASUREMENT UTILITY — part of the project's tooling, not of the pipeline.
  Nothing imports it and no service depends on it, so it can be run (or not)
  without affecting the running system.
==============================================================================

Extends pipeline_timing.py's ingestion -> silver measurement with the two legs
that tool cannot see at all: silver -> gold (the Spark recompute triggered by
new data landing), and profile-edit -> gold (the Spark recompute triggered by
a user saving new preferences). Runs continuously against the live pipeline
and writes two CSVs in this folder as events happen — nothing here is a
one-shot report, both keep growing for as long as the process runs.

WHY A SEPARATE SCRIPT, NOT AN EXTENSION OF pipeline_timing.py
---------------------------------------------------------------
pipeline_timing.py answers one question with zero dependencies beyond reading
the shared volume: no DB credentials, no network access to any service. This
script needs an HTTP client and answers a materially different question (what
happens to data once it's already in silver), so it keeps its own file and its
own reports rather than complicating that tool's single-purpose design. It DOES
import pipeline_timing.py directly (same folder) to reuse its file scanner
rather than duplicate it — importing it has no side effects, since its own
main() only runs under `if __name__ == "__main__"`.

TWO THINGS THIS SCRIPT MEASURES
--------------------------------
1. Per-slice (batch) latency, ingestion all the way through gold:
   - Same file-based "landed in silver" signal pipeline_timing.py uses.
   - Then polls the backend's GET /system/status (an unauthenticated read of
     PostgreSQL's `pipeline_status` table — see 5-serving/backend/main.py and
     postgres_store.get_pipeline_status) until its `silver_watermark` reaches
     this slice AND `timestamp_of_last_update` moves past the moment the slice
     landed in silver. That gap is the silver -> gold Spark recompute time,
     INCLUDING the up-to-60s (WATERMARK_POLL_SECONDS) the watermark-poll
     trigger itself waits before even starting — a real, user-relevant delay,
     not polling noise.
   - If a slice's events or mentions file never reaches silver (a GDELT 404
     during ingestion's retry window is the observed real-world cause of
     this), the slice is marked INCOMPLETE with a note instead of hanging
     forever: as soon as a STRICTLY NEWER slice is observed anywhere in the
     watched directories, ingestion has necessarily already finalized and
     moved past the stuck one — its own poller (1-ingestion) is
     single-threaded and processes one slice fully (complete or released as
     partial) before ever looking at the next — so it is safe to close the
     row out immediately rather than guess at a timeout. A generous absolute
     timeout (BATCH_STALL_TIMEOUT_SECONDS) is kept only as a fallback for the
     case where ingestion stalls completely and no newer slice ever appears.

2. Profile-edit -> gold latency:
   - Discovers known users via GET /users/all-profiles, then polls each
     user's GET /users/{id}/profile, hashing its full content. A hash change
     is a genuine preference edit — unrelated to which articles currently
     match (see get_events_version's own docstring in
     5-serving/backend/postgres_store.py: the fingerprint is a pure function
     of PostgreSQL user_articles, with no dependency on the profile at all).
   - The moment a hash changes, the CURRENT GET /users/{id}/events-version
     fingerprint is remembered as the baseline. Latency is measured until
     that fingerprint changes.
   - This is passive: the script never edits anyone's profile itself. If
     nobody touches their preferences while it runs, preference_update_report
     stays empty except its header — that is correct, not a malfunction. To
     force a data point for testing, edit a profile through the frontend, or:

         curl -s http://localhost:8000/users/<id>/profile | python3 -c "
import json, sys
p = json.load(sys.stdin)
p['keywords'].setdefault('sourcing', []).append('probe-test')
print(json.dumps(p))" | curl -s -X PUT http://localhost:8000/users/<id>/profile \
           -H 'Content-Type: application/json' -d @-

   - CAVEAT: events-version also changes when NEW DATA changes which articles
     a user matches, with no profile edit involved. This script and the
     frontend's own recompute_notice.py share this ambiguity structurally —
     nothing exposed anywhere says "this recompute was caused by a profile
     edit" versus "by new data". To make the confound visible rather than
     silently misleading, each row also reports whether `silver_watermark`
     also moved during the same window.

RUNNING IT
----------
Needs everything pipeline_timing.py needs (the shared_data volume, read-only),
PLUS network access to the backend service, which only exists inside
`pipeline_network` (per the main README: the pipeline exposes nothing else).
Run it from the repo root, with the pipeline already up:

    VOL=$(docker volume ls -q | grep shared_data | head -1)
    docker run --rm -it \
      --network pipeline_network \
      -v "$VOL":/data:ro \
      -v "$PWD/dev_tool_for_time_gauging":/out \
      -e DATA_DIR=/data -e OUT_DIR=/out -e BACKEND_URL=http://backend:8000 \
      -w /out python:3.11-slim python full_latency_probe.py

Ctrl-C to stop. Re-running appends to both existing reports, exactly like
pipeline_timing.py does with its own.

OUTPUT
------
slice_latency_report.csv — one row per 15-minute slice, written once the slice
is fully resolved (gold caught up, gold timed out, or the slice was marked
incomplete). Columns:
    slice, status,
    events_rows, events_seconds_gdelt_to_silver, events_seconds_pipeline_to_silver,
    events_seconds_pipeline_to_gold,
    mentions_rows, mentions_seconds_gdelt_to_silver, mentions_seconds_pipeline_to_silver,
    mentions_seconds_pipeline_to_gold,
    silver_landed_at_utc, seconds_silver_to_gold,
    gold_confirmed_at_utc, seconds_pipeline_to_gold, note

    *_seconds_pipeline_to_gold is that KIND's own first-seen-by-the-pipeline
    moment (the same "pipeline" anchor *_seconds_pipeline_to_silver already
    uses) all the way to gold confirmation — i.e. the full, per-file version of
    the existing (combined, whichever kind arrived first) seconds_pipeline_to_gold
    column. events' and mentions' values normally differ, since the two files
    are typically first seen by the pipeline at slightly different moments.
    Blank for INCOMPLETE and GOLD_TIMEOUT rows, same as the other gold-side
    columns, since no gold confirmation ever happened to measure to.
    A report from before these two columns existed (including one written by
    an earlier version of this script that used *_seconds_gdelt_to_gold
    instead — dropped on migration, since pipeline_to_gold is what's wanted,
    not gdelt_to_gold) is migrated in place the next time this script starts
    (see _migrate_slice_report) — existing rows are backfilled, nothing else
    is lost or re-ordered.

preference_update_report.csv — one row per detected profile edit. Columns:
    user_id, profile_edit_detected_at_utc, gold_confirmed_at_utc,
    seconds_to_gold, status, watermark_also_moved, note
"""

import csv
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pipeline_timing as pt   # same folder — reuses scan()/classify()/etc.

OUT_DIR = Path(os.getenv("OUT_DIR", str(Path(__file__).resolve().parent)))
BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000").rstrip("/")

SLICE_REPORT = OUT_DIR / "slice_latency_report.csv"
PREF_REPORT = OUT_DIR / "preference_update_report.csv"

MAIN_LOOP_SLEEP_SECONDS = float(os.getenv("POLL_SECONDS", "2"))
STATUS_POLL_SECONDS = float(os.getenv("STATUS_POLL_SECONDS", "5"))
USER_LIST_REFRESH_SECONDS = float(os.getenv("USER_LIST_REFRESH_SECONDS", "30"))
USER_POLL_SECONDS = float(os.getenv("USER_POLL_SECONDS", "5"))

# How long to wait, once a slice HAS landed in silver, for gold to catch up
# before giving up on it. Generous: a full (non-incremental) recompute over a
# large catalogue was observed to take ~14 minutes in this project's own
# testing, so this sits well above that rather than above the ~30-45s
# incremental case that is typical.
GOLD_WAIT_TIMEOUT_SECONDS = float(os.getenv("GOLD_WAIT_TIMEOUT_SECONDS", str(25 * 60)))

# Fallback only: a slice is normally closed out the moment a newer slice is
# observed (ingestion is single-threaded, so that already proves the old one
# is finalized). This only fires if ingestion stalls completely and no newer
# slice ever appears at all.
BATCH_STALL_TIMEOUT_SECONDS = float(os.getenv("BATCH_STALL_TIMEOUT_SECONDS", str(30 * 60)))

# Generous vs. the ~200s worst-case observed in this project's own testing
# (a single-user edit landing right behind a large full recompute).
PREF_UPDATE_TIMEOUT_SECONDS = float(os.getenv("PREF_UPDATE_TIMEOUT_SECONDS", str(20 * 60)))

SLICE_COLUMNS = [
    "slice", "status",
    "events_rows", "events_seconds_gdelt_to_silver", "events_seconds_pipeline_to_silver",
    "events_seconds_pipeline_to_gold",
    "mentions_rows", "mentions_seconds_gdelt_to_silver", "mentions_seconds_pipeline_to_silver",
    "mentions_seconds_pipeline_to_gold",
    "silver_landed_at_utc", "seconds_silver_to_gold",
    "gold_confirmed_at_utc", "seconds_pipeline_to_gold", "note",
]
PREF_COLUMNS = [
    "user_id", "profile_edit_detected_at_utc", "gold_confirmed_at_utc",
    "seconds_to_gold", "status", "watermark_also_moved", "note",
]


def _ensure_report(path: Path, columns: list) -> None:
    if not path.exists():
        with path.open("w", newline="") as fh:
            csv.writer(fh).writerow(columns)


def _reconstruct_pipeline_to_gold(row: dict, kind: str, gold_ts, published):
    """
    A slice's per-kind first-seen-by-the-pipeline moment isn't stored directly
    in the CSV, but it's reconstructible from two columns that already are:

        silver_landed_at_KIND = gdelt_publish_time + KIND_seconds_gdelt_to_silver
        first_seen_at_KIND    = silver_landed_at_KIND - KIND_seconds_pipeline_to_silver
        KIND_seconds_pipeline_to_gold = gold_confirmed_at_utc - first_seen_at_KIND

    Returns "" (not 0) when any needed input is missing — an INCOMPLETE or
    GOLD_TIMEOUT row has no gold_confirmed_at_utc, so there is nothing to
    reconstruct, same as for a freshly-computed row.
    """
    gdelt_to_silver = row.get(f"{kind}_seconds_gdelt_to_silver")
    pipeline_to_silver = row.get(f"{kind}_seconds_pipeline_to_silver")
    if not gold_ts or not published or gdelt_to_silver in (None, "") or pipeline_to_silver in (None, ""):
        return ""
    silver_landed_at_kind = published + timedelta(seconds=float(gdelt_to_silver))
    first_seen_at_kind = silver_landed_at_kind - timedelta(seconds=float(pipeline_to_silver))
    return round((gold_ts - first_seen_at_kind).total_seconds(), 1)


def _migrate_slice_report() -> None:
    """
    Backfills events_seconds_pipeline_to_gold / mentions_seconds_pipeline_to_gold
    onto a report written before those columns existed — including one written
    by an earlier version of this script that used *_seconds_gdelt_to_gold
    instead (a different, since-corrected metric); those columns are simply
    dropped on migration, since pipeline_to_gold is the wanted one.

    No-op if the report doesn't exist yet, or already has the current columns.
    """
    if not SLICE_REPORT.exists():
        return
    with SLICE_REPORT.open("r", newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows or "events_seconds_pipeline_to_gold" in rows[0]:
        return

    for row in rows:
        gold_raw = row.get("gold_confirmed_at_utc")
        slice_raw = row.get("slice")
        gold_ts = datetime.fromisoformat(gold_raw) if gold_raw else None
        published = pt.slice_to_utc(slice_raw) if slice_raw else None
        for kind in ("events", "mentions"):
            row[f"{kind}_seconds_pipeline_to_gold"] = _reconstruct_pipeline_to_gold(
                row, kind, gold_ts, published)

    with SLICE_REPORT.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(SLICE_COLUMNS)
        for row in rows:
            writer.writerow([row.get(c, "") for c in SLICE_COLUMNS])
    print(f"[probe] migrated {SLICE_REPORT} — added events/mentions_seconds_pipeline_to_gold "
          f"to {len(rows)} existing row(s)")


def _append_row(path: Path, columns: list, row: dict) -> None:
    with path.open("a", newline="") as fh:
        csv.writer(fh).writerow([row.get(c, "") for c in columns])


def _get_json(path: str, timeout: float = 10.0):
    """GET {BACKEND_URL}{path}, return parsed JSON or None on any failure."""
    try:
        with urllib.request.urlopen(f"{BACKEND_URL}{path}", timeout=timeout) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        print(f"[probe] GET {path} failed: {exc}")
        return None


def _parse_pg_timestamp(value):
    """Postgres's naive 'YYYY-MM-DD HH:MM:SS[.ffffff]' string -> aware UTC datetime."""
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


# ── Batch (per-slice) tracking ───────────────────────────────────────────────

class BatchTracker:
    """
    Mirrors pipeline_timing.py's own file-scanning to find each slice's
    ingestion -> silver timing, then hands complete slices off to be watched
    for silver -> gold via /system/status.
    """

    def __init__(self):
        self.rows_seen = {}       # (slice, kind) -> (rows, source)
        self.first_seen = {}      # (slice, kind) -> datetime
        self.in_latest = set()    # (slice, kind) ever seen in latest_files
        self.silver_landed = {}   # (slice, kind) -> (datetime, rows, gdelt_secs, pipeline_secs)
        self.newest_slice = ""    # highest slice id observed anywhere
        self.resolved_slices = set()   # slice ids already finalized (written or handed to gold)
        self.pending_gold = {}    # slice -> {"t_silver": dt, "first_seen": dt, "row": {...}}

    def scan_tick(self) -> None:
        now = datetime.now(timezone.utc)

        for key, path in pt.scan(pt.RAW_DIR).items():
            self.first_seen.setdefault(key, now)
            if key not in self.rows_seen:
                self.rows_seen[key] = (pt.count_rows(path), "raw")
            self.newest_slice = max(self.newest_slice, key[0])

        current = pt.scan(pt.LATEST_DIR)
        for key, path in current.items():
            self.first_seen.setdefault(key, now)
            self.in_latest.add(key)
            if key not in self.rows_seen:
                self.rows_seen[key] = (pt.count_rows(path), "latest")
            self.newest_slice = max(self.newest_slice, key[0])

        # Disappeared from latest_files => landed in silver, same signal
        # pipeline_timing.py uses.
        for key in sorted(self.in_latest - set(current) - set(self.silver_landed)):
            slice_id, _kind = key
            published = pt.slice_to_utc(slice_id)
            rows, _source = self.rows_seen.get(key, (-1, "unknown"))
            gdelt_secs = round((now - published).total_seconds(), 1) if published else None
            pipe_secs = (round((now - self.first_seen[key]).total_seconds(), 1)
                         if key in self.first_seen else None)
            self.silver_landed[key] = (now, rows, gdelt_secs, pipe_secs)

        self._resolve_finished_slices(now)

    def _resolve_finished_slices(self, now) -> None:
        open_slices = {s for (s, _k) in self.first_seen} - self.resolved_slices
        for slice_id in sorted(open_slices):
            kinds_landed = {k for (s, k) in self.silver_landed if s == slice_id}
            earliest_first_seen = min(
                v for (s, _k), v in self.first_seen.items() if s == slice_id)

            complete = {"events", "mentions"} <= kinds_landed
            newer_seen = slice_id < self.newest_slice
            stalled = (now - earliest_first_seen).total_seconds() > BATCH_STALL_TIMEOUT_SECONDS

            if complete:
                self._finalize_complete(slice_id, earliest_first_seen)
            elif newer_seen or stalled:
                reason = "next slice already arrived" if newer_seen else "stalled"
                self._finalize_incomplete(slice_id, kinds_landed, reason)

    def _finalize_complete(self, slice_id: str, earliest_first_seen) -> None:
        row = {"slice": slice_id, "status": "COMPLETE", "note": ""}
        t_silver = None
        first_seen_by_kind = {}
        for kind in ("events", "mentions"):
            landed = self.silver_landed.get((slice_id, kind))
            if landed:
                landed_at, rows, gdelt_secs, pipe_secs = landed
                row[f"{kind}_rows"] = rows
                row[f"{kind}_seconds_gdelt_to_silver"] = gdelt_secs
                row[f"{kind}_seconds_pipeline_to_silver"] = pipe_secs
                t_silver = landed_at if t_silver is None else max(t_silver, landed_at)
                first_seen_by_kind[kind] = self.first_seen.get((slice_id, kind))
        row["silver_landed_at_utc"] = t_silver.isoformat(timespec="seconds")

        self.resolved_slices.add(slice_id)
        self.pending_gold[slice_id] = {
            "t_silver": t_silver,
            "first_seen": earliest_first_seen,
            "first_seen_by_kind": first_seen_by_kind,
            "row": row,
        }
        print(f"[probe] slice {slice_id} COMPLETE in silver, watching for gold…")

    def _finalize_incomplete(self, slice_id, kinds_landed, reason) -> None:
        missing = {"events", "mentions"} - kinds_landed
        row = {"slice": slice_id, "status": "INCOMPLETE"}
        for kind in ("events", "mentions"):
            landed = self.silver_landed.get((slice_id, kind))
            if landed:
                landed_at, rows, gdelt_secs, pipe_secs = landed
                row[f"{kind}_rows"] = rows
                row[f"{kind}_seconds_gdelt_to_silver"] = gdelt_secs
                row[f"{kind}_seconds_pipeline_to_silver"] = pipe_secs
        row["note"] = (
            f"{'/'.join(sorted(missing))} never reached silver "
            f"({reason} — likely a GDELT 404 during ingestion's retry window); "
            f"slice excluded from gold, not tracked further"
        )
        _append_row(SLICE_REPORT, SLICE_COLUMNS, row)
        self.resolved_slices.add(slice_id)
        print(f"[probe] slice {slice_id} INCOMPLETE — {row['note']}")

    def gold_tick(self, status) -> None:
        if status:
            watermark = status.get("silver_watermark")
            gold_ts = _parse_pg_timestamp(status.get("timestamp_of_last_update"))
            if watermark and gold_ts:
                done = []
                for slice_id, pending in self.pending_gold.items():
                    if watermark >= slice_id and gold_ts > pending["t_silver"]:
                        row = pending["row"]
                        row["status"] = "COMPLETE"
                        row["gold_confirmed_at_utc"] = gold_ts.isoformat(timespec="seconds")
                        row["seconds_silver_to_gold"] = round(
                            (gold_ts - pending["t_silver"]).total_seconds(), 1)
                        row["seconds_pipeline_to_gold"] = round(
                            (gold_ts - pending["first_seen"]).total_seconds(), 1)
                        # Per-kind version of the line above: each file's OWN
                        # first-seen-by-the-pipeline moment (not the earliest
                        # across both kinds) all the way to gold. Normally
                        # differ from each other, since events and mentions are
                        # typically first seen at slightly different moments.
                        for kind, fs in pending["first_seen_by_kind"].items():
                            if fs is not None:
                                row[f"{kind}_seconds_pipeline_to_gold"] = round(
                                    (gold_ts - fs).total_seconds(), 1)
                        _append_row(SLICE_REPORT, SLICE_COLUMNS, row)
                        print(f"[probe] slice {slice_id} GOLD confirmed "
                              f"(silver->gold={row['seconds_silver_to_gold']}s)")
                        done.append(slice_id)
                for slice_id in done:
                    del self.pending_gold[slice_id]

        self._timeout_stale_pending()

    def _timeout_stale_pending(self) -> None:
        now = datetime.now(timezone.utc)
        timed_out = [s for s, p in self.pending_gold.items()
                     if (now - p["t_silver"]).total_seconds() > GOLD_WAIT_TIMEOUT_SECONDS]
        for slice_id in timed_out:
            pending = self.pending_gold.pop(slice_id)
            row = pending["row"]
            row["status"] = "GOLD_TIMEOUT"
            row["note"] = (f"landed in silver but gold had not caught up after "
                            f"{GOLD_WAIT_TIMEOUT_SECONDS:.0f}s — pipeline may be stalled "
                            f"or stuck behind a very large recompute")
            _append_row(SLICE_REPORT, SLICE_COLUMNS, row)
            print(f"[probe] slice {slice_id} GOLD_TIMEOUT")


# ── Preference-update tracking ───────────────────────────────────────────────

class PreferenceTracker:
    """
    Passive observer: never edits anyone's profile, just notices when someone
    else does (frontend user, seeder, curl, whatever) and times gold's reaction.
    """

    def __init__(self):
        self.known = {}     # user_id -> {"profile_hash": str, "version": str|None}
        self.pending = {}   # user_id -> {"t_trigger": dt, "baseline_version": str, "watermark_before": str}
        self.last_user_list_refresh = 0.0
        self.last_user_poll = 0.0

    @staticmethod
    def _profile_hash(profile: dict) -> str:
        return hashlib.sha256(
            json.dumps(profile, sort_keys=True, default=str).encode()
        ).hexdigest()

    def refresh_user_list(self) -> None:
        now = time.monotonic()
        if now - self.last_user_list_refresh < USER_LIST_REFRESH_SECONDS:
            return
        self.last_user_list_refresh = now
        data = _get_json("/users/all-profiles")
        if not data:
            return
        for profile in data.get("profiles", []):
            uid = profile.get("user_id")
            if not uid or uid in self.known:
                continue
            # Seed BOTH baseline fields together at discovery, from the SAME
            # moment — if only profile_hash were seeded here, the first real
            # poll_tick would see version go from None to something and
            # misreport that as gold having already reacted to an edit that
            # never happened.
            version_payload = _get_json(f"/users/{uid}/events-version")
            self.known[uid] = {
                "profile_hash": self._profile_hash(profile),
                "version": version_payload.get("version") if version_payload else None,
            }
            print(f"[probe] now watching profile edits for {uid}")

    def poll_tick(self, current_watermark) -> None:
        now_mono = time.monotonic()
        if now_mono - self.last_user_poll >= USER_POLL_SECONDS:
            self.last_user_poll = now_mono
            for uid in list(self.known):
                self._poll_one(uid, current_watermark)

        self._timeout_stale_pending()

    def _poll_one(self, uid: str, current_watermark) -> None:
        profile = _get_json(f"/users/{uid}/profile")
        version_payload = _get_json(f"/users/{uid}/events-version")
        if profile is None or version_payload is None:
            return
        version = version_payload.get("version")
        state = self.known[uid]
        new_hash = self._profile_hash(profile)

        if uid not in self.pending and new_hash != state["profile_hash"]:
            self.pending[uid] = {
                "t_trigger": datetime.now(timezone.utc),
                "baseline_version": state["version"],
                "watermark_before": current_watermark,
            }
            print(f"[probe] {uid}: profile edit detected, watching for gold…")

        state["profile_hash"] = new_hash

        if uid in self.pending and version != self.pending[uid]["baseline_version"]:
            pending = self.pending.pop(uid)
            now = datetime.now(timezone.utc)
            seconds = round((now - pending["t_trigger"]).total_seconds(), 1)
            watermark_moved = (current_watermark is not None
                                and current_watermark != pending["watermark_before"])
            _append_row(PREF_REPORT, PREF_COLUMNS, {
                "user_id": uid,
                "profile_edit_detected_at_utc": pending["t_trigger"].isoformat(timespec="seconds"),
                "gold_confirmed_at_utc": now.isoformat(timespec="seconds"),
                "seconds_to_gold": seconds,
                "status": "CONFIRMED",
                "watermark_also_moved": watermark_moved,
                "note": ("new data ALSO landed during this window — some of the "
                          "latency above may be unrelated to the profile edit"
                          if watermark_moved else ""),
            })
            print(f"[probe] {uid}: gold confirmed after {seconds}s")

        state["version"] = version

    def _timeout_stale_pending(self) -> None:
        now = datetime.now(timezone.utc)
        timed_out = [uid for uid, p in self.pending.items()
                     if (now - p["t_trigger"]).total_seconds() > PREF_UPDATE_TIMEOUT_SECONDS]
        for uid in timed_out:
            pending = self.pending.pop(uid)
            _append_row(PREF_REPORT, PREF_COLUMNS, {
                "user_id": uid,
                "profile_edit_detected_at_utc": pending["t_trigger"].isoformat(timespec="seconds"),
                "gold_confirmed_at_utc": "",
                "seconds_to_gold": "",
                "status": "TIMEOUT",
                "watermark_also_moved": "",
                "note": f"events-version had not changed after {PREF_UPDATE_TIMEOUT_SECONDS:.0f}s",
            })
            print(f"[probe] {uid}: TIMEOUT waiting for gold")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    _migrate_slice_report()
    _ensure_report(SLICE_REPORT, SLICE_COLUMNS)
    _ensure_report(PREF_REPORT, PREF_COLUMNS)

    print(f"[probe] watching {pt.RAW_DIR} and {pt.LATEST_DIR}")
    print(f"[probe] backend  {BACKEND_URL}")
    print(f"[probe] slice report -> {SLICE_REPORT}")
    print(f"[probe] preference-update report -> {PREF_REPORT}\n")

    batches = BatchTracker()
    prefs = PreferenceTracker()
    last_status_poll = 0.0
    latest_status = None

    while True:
        batches.scan_tick()

        now_mono = time.monotonic()
        if now_mono - last_status_poll >= STATUS_POLL_SECONDS:
            last_status_poll = now_mono
            latest_status = _get_json("/system/status")

        batches.gold_tick(latest_status)

        prefs.refresh_user_list()
        prefs.poll_tick(latest_status.get("silver_watermark") if latest_status else None)

        time.sleep(MAIN_LOOP_SLEEP_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[probe] stopped.")
