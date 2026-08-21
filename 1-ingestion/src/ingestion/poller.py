import argparse
import json
import os
import re
import shutil
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict

import pandas as pd
import requests

from src.ingestion.gdelt_urls import build_file_url, round_to_15min
from src.ingestion.paths import (
    RAW_ZIP_DIR, RAW_CSV_DIR, RAW_STAGING_DIR, STATE_FILE, ensure_ingestion_dirs,
)

# GDELT v2 publishes the latest 15-minute file list here.
#
# This is the ENGLISH feed only. GDELT publishes a parallel translingual feed at
# lastupdate-translation.txt whose files are named <slice>.translation.export.CSV
# and <slice>.translation.mentions.CSV. It is deliberately not ingested; note that
# it also runs one full slice (15 minutes) behind this one, so the two can never
# be treated as a single atomic payload.
LAST_15MIN_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

POLL_INTERVAL_SECONDS = 15 * 60

# A slice is worth chasing for this long after its own timestamp. GDELT publishes
# every 15 minutes, so a file still missing after 10 is very unlikely to appear,
# and continuing to wait would only delay the next slice. Past this the slice is
# released with whatever was retrieved — possibly nothing.
SLICE_RETRIEVAL_DEADLINE = int(os.getenv("SLICE_RETRIEVAL_DEADLINE", "600"))
# How often to re-attempt a missing file inside that window.
RETRY_TICK = int(os.getenv("RETRY_TICK", "60"))

EVENTS_NAME_SUFFIX = ".export.CSV"
MENTIONS_NAME_SUFFIX = ".mentions.CSV"

def ensure_directories() -> None:
    ensure_ingestion_dirs()

def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    with STATE_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state: dict) -> None:
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

# GDELT names every file after its own 15-minute slice, e.g.
# .../20260810121500.export.CSV.zip. That timestamp is the EVENT time, which is
# what progress should be measured in — the URL alone cannot be compared or
# ordered, so a gap in the feed is invisible if only the URL is recorded.
_SLICE_RE = re.compile(r"(\d{14})")
SLICE_SECONDS = 15 * 60


def slice_of_url(url: str) -> str:
    """The 14-digit slice id embedded in a GDELT file URL, or '' if absent."""
    match = _SLICE_RE.search(url or "")
    return match.group(1) if match else ""


def report_gap(previous: str, current: str) -> None:
    """
    Log any slices that were released between two polls and never collected.

    This poller only ever fetches whatever lastupdate.txt currently points at, so
    anything published while it was stopped is simply missed. That is deliberate —
    a 15-minute poller silently back-filling hours of history would violate the
    bounded lateness the rest of the pipeline relies on — but a gap should be
    stated rather than passed over, because it is otherwise undetectable later.
    Use bootstrap/bulk_load.py to load a gap on purpose.
    """
    if not previous or not current or current <= previous:
        return
    try:
        prev_dt = datetime.strptime(previous, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        curr_dt = datetime.strptime(current, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return
    missed = int((curr_dt - prev_dt).total_seconds() // SLICE_SECONDS) - 1
    if missed > 0:
        first = (prev_dt + timedelta(seconds=SLICE_SECONDS)).strftime("%Y%m%d%H%M%S")
        print(f"[WARNING] {missed} slice(s) were never collected between {first} and "
              f"{current}. They are not retried; use bootstrap/bulk_load.py to load "
              f"that period deliberately.")

def fetch_latest_urls(session: requests.Session) -> Dict[str, str]:
    """
    Legge il file temporaneo di GDELT ed estrae gli ultimi URL di Events e Mentions.
    Include un meccanismo di retry in caso di 404 temporaneo del server.
    """
    retries = 3
    delay = 5  # secondi da aspettare tra i tentativi
    
    for attempt in range(1, retries + 1):
        try:
            response = session.get(LAST_15MIN_URL, timeout=30)
            response.raise_for_status()
            
            urls = {}
            for line in response.text.splitlines():
                parts = line.strip().split()
                if len(parts) < 3:
                    continue
                candidate_url = parts[2]
                
                if candidate_url.endswith(".export.CSV.zip"):
                    urls["events"] = candidate_url
                elif candidate_url.endswith(".mentions.CSV.zip"):
                    urls["mentions"] = candidate_url
                    
            return urls
            
        except requests.exceptions.HTTPError as http_err:
            # Se è un 404 ed abbiamo ancora tentativi, aspettiamo e riproviamo
            if response.status_code == 404 and attempt < retries:
                print(f"[WARNING] GDELT returned 404 (attempt {attempt}/{retries}). The file is being updated. Retrying in {delay} seconds...")
                time.sleep(delay)
                continue
            raise http_err  # Se i tentativi sono finiti o è un altro errore, fallisci
        except requests.exceptions.RequestException as req_err:
            if attempt < retries:
                time.sleep(delay)
                continue
            raise req_err
            
    return {}

def download_and_extract(session: requests.Session, file_url: str,
                         dest_dir: Path = None) -> tuple[Path, Path]:
    """
    Scarica, salva il file zip ed estrae il CSV. Restituisce (csv_path, zip_path).

    The CSV now lands in a per-slice STAGING directory rather than straight in the
    hand-off directory, so a slice is never published half-finished. The download
    and unzip logic itself is unchanged.
    """
    response = session.get(file_url, timeout=120)
    response.raise_for_status()
    content = response.content

    # Salva ZIP
    zip_path = RAW_ZIP_DIR / Path(file_url).name
    zip_path.write_bytes(content)

    # Estrae CSV
    with zipfile.ZipFile(zip_path, "r") as zf:
        first_member = zf.namelist()[0]
        target_dir = dest_dir if dest_dir is not None else staging_dir_for(
            slice_of_url(file_url))
        target_dir.mkdir(parents=True, exist_ok=True)
        extracted_path = target_dir / Path(first_member).name
        extracted_path.write_bytes(zf.read(first_member))
        return extracted_path, zip_path

def validate_and_cleanup(csv_path: Path, zip_path: Path) -> None:
    """Valida il CSV (ora nello staging), poi rimuove lo ZIP."""
    df = pd.read_csv(csv_path, sep="\t", header=None, low_memory=False)
    print(f"[OK] Valid CSV: {csv_path.name} | Rows detected: {len(df)}")

    # Il CSV resta nello staging finché lo slice non è completo (o scade il
    # deadline): solo allora viene spostato in RAW_CSV_DIR, dove il parsing lo
    # leggerà. Il parsing resta responsabile di cancellarlo dopo averlo processato.
    zip_path.unlink(missing_ok=True)
    print(f"[CLEANUP] Removed raw ZIP: {zip_path.name}")


# ── Staging and atomic release ───────────────────────────────────────────────

def staging_dir_for(slice_id: str) -> Path:
    """The staging folder holding the files retrieved so far for one slice."""
    return RAW_STAGING_DIR / (slice_id or "unknown")


def staged_kinds(slice_id: str) -> dict:
    """
    Map 'events'/'mentions' -> staged Path, for whatever has been retrieved.

    A file is only counted as part of this slice if its NAME carries the slice id.
    Without that check, a download that returned a neighbouring slice would be
    staged here and released as if it belonged, handing parsing a "pair" made of
    two different timestamps. Anything else in the folder is ignored and left for
    the caller to clean up.
    """
    found = {}
    d = staging_dir_for(slice_id)
    if not d.exists():
        return found
    for p in d.iterdir():
        if not p.is_file() or p.name.startswith("."):
            continue
        if slice_id and not p.name.startswith(slice_id):
            print(f"[WARNING] {p.name} does not belong to slice {slice_id}; ignored")
            continue
        if p.name.endswith(EVENTS_NAME_SUFFIX):
            found["events"] = p
        elif p.name.endswith(MENTIONS_NAME_SUFFIX):
            found["mentions"] = p
    return found


def slice_deadline_passed(slice_id: str) -> bool:
    """True once a slice is older than SLICE_RETRIEVAL_DEADLINE."""
    try:
        published = datetime.strptime(slice_id, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return True          # unparseable id: do not hold it forever
    age = (datetime.now(timezone.utc) - published).total_seconds()
    return age >= SLICE_RETRIEVAL_DEADLINE


def release_slice(slice_id: str) -> list:
    """
    Move a slice's staged files into the hand-off directory, mentions LAST so the
    parsing layer never sees a pair mid-write — the same ordering it already
    relies on. Returns the released file names.
    """
    staged = staged_kinds(slice_id)
    released = []
    for kind in ("events", "mentions"):        # mentions last, deliberately
        path = staged.get(kind)
        if path is None:
            continue
        target = RAW_CSV_DIR / path.name
        os.replace(path, target)
        released.append(target.name)

    d = staging_dir_for(slice_id)
    if d.exists():
        shutil.rmtree(d, ignore_errors=True)

    if released:
        kinds = "+".join(sorted(staged.keys()))
        print(f"[RELEASE] slice {slice_id} -> parsing ({kinds}): {', '.join(released)}")
    return released

def _retrieve_into_staging(session: requests.Session, url: str, kind: str,
                           slice_id: str) -> bool:
    """Download one file into the slice's staging folder. Never raises."""
    try:
        print(f"[INFO] New {kind} file detected: {Path(url).name}")
        csv_path, zip_path = download_and_extract(
            session, url, dest_dir=staging_dir_for(slice_id))
        validate_and_cleanup(csv_path, zip_path)
        return True
    except Exception as exc:                      # noqa: BLE001
        print(f"[WARNING] Could not retrieve {kind} ({Path(url).name}): {exc}")
        return False


def process_pipeline(session: requests.Session) -> None:
    """
    Retrieve one 15-minute payload and hand it to parsing ATOMICALLY.

    The payload is the two English CSVs — events and mentions. They are assembled
    in a staging folder and moved into the hand-off directory only when the slice
    is complete, or when SLICE_RETRIEVAL_DEADLINE has passed. Nothing is ever
    published half-finished.

    While a file is missing and the deadline has not passed, it is re-attempted
    every RETRY_TICK using the slice-addressed URL (build_file_url). That fallback
    runs ONLY for a file the normal lastupdate.txt path failed to fetch.
    """
    ensure_directories()
    state = load_state()

    # 1. Recupera gli ultimi URL disponibili
    latest_urls = fetch_latest_urls(session)

    if not latest_urls.get("events") or not latest_urls.get("mentions"):
        print("[WARNING] Could not find the Events or Mentions URLs in the control file.")
        return

    event_url = latest_urls["events"]
    mention_url = latest_urls["mentions"]
    slice_id = slice_of_url(event_url) or slice_of_url(mention_url)

    # Already handled: this slice was released on an earlier cycle.
    if state.get("events") == event_url and state.get("mentions") == mention_url:
        print("[SKIP] Events and Mentions tables are already up to date with the latest release.")
        return

    # 2. First attempt, through the normal path, for whatever is not yet staged.
    staged = staged_kinds(slice_id)
    for kind, url in (("Events", event_url), ("Mentions", mention_url)):
        key = "events" if kind == "Events" else "mentions"
        if key in staged:
            continue
        if _retrieve_into_staging(session, url, kind, slice_id):
            state[key] = url

    # 3. Anything still missing is retried by slice-addressed URL until the
    #    deadline. Retrieval is decoupled from processing, so waiting here delays
    #    only this slice's hand-off, never the layers downstream.
    while len(staged_kinds(slice_id)) < 2 and not slice_deadline_passed(slice_id):
        time.sleep(RETRY_TICK)
        staged = staged_kinds(slice_id)
        for key, file_type in (("events", "events"), ("mentions", "mentions")):
            if key in staged:
                continue
            try:
                published = datetime.strptime(slice_id, "%Y%m%d%H%M%S").replace(
                    tzinfo=timezone.utc)
            except (ValueError, TypeError):
                break
            # Build the URL from the slice id EXACTLY as given. Rounding it would
            # be worse than redundant: a slice id that is not on a 15-minute
            # boundary would be rounded to a DIFFERENT slice, and its file would
            # then be staged into this slice's folder — releasing a mismatched
            # pair of two different timestamps to parsing.
            retry_url = build_file_url(published, file_type)
            print(f"[RETRY] {key} missing for slice {slice_id}; retrying from {retry_url}")
            if _retrieve_into_staging(session, retry_url, key, slice_id):
                state[key] = retry_url

    # 4. Release: complete, or out of time. A partial release is deliberate —
    #    events alone still update the store (ReplacingMergeTree on
    #    GLOBALEVENTID), and mentions alone still attach to events already stored.
    final = staged_kinds(slice_id)
    if not final:
        print(f"[WARNING] slice {slice_id}: no files retrieved before the deadline.")
    elif len(final) < 2:
          print(f"[WARNING] slice {slice_id}: partial payload "
              f"({'+'.join(sorted(final))}) released after the deadline.")
    release_slice(slice_id)

    # 5. Advance the event-time watermark and report anything that was missed.
    #    Written last, and only forwards, so a partial failure above leaves the
    #    previous watermark in place rather than claiming ground never covered.
    if slice_id:
        previous = state.get("last_slice", "")
        report_gap(previous, slice_id)
        if slice_id > previous:
            state["last_slice"] = slice_id

    # 6. Salva lo stato aggiornato
    save_state(state)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poller GDELT (Events & Mentions)")
    parser.add_argument("--loop", action="store_true", help="Resta in ascolto ogni 15 minuti")
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    with requests.Session() as session:
        if args.loop:
            print("[START] Poller started in continuous mode (loop = 15 min)...")
            while True:
                started = time.monotonic()
                try:
                    process_pipeline(session)
                except Exception as exc:
                    print(f"[ERROR] Error during execution cycle: {exc}")
                # Sleep the REMAINDER of the interval, not a fixed 15 minutes: a
                # cycle that spent time retrying a missing file would otherwise
                # push the next one to T+25 and drift further every time.
                elapsed = time.monotonic() - started
                time.sleep(max(0.0, POLL_INTERVAL_SECONDS - elapsed))
        else:
            print("[START] Poller started for a single run...")
            process_pipeline(session)

if __name__ == "__main__":
    main()
