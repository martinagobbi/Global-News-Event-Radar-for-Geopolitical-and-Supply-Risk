#!/usr/bin/env python
"""
bootstrap/bulk_load.py — fill the silver layer directly from a folder of GDELT
ZIP files, bypassing the 15-minute-at-a-time pipeline.

Why this exists
---------------
The live pipeline processes ONE 15-minute slice at a time: parsing publishes a
pair only when `latest_files` is empty, and validation then enriches and stores
it. Measured on this project, a slice costs about a minute end to end, so a
30-day backfill (2,880 slices) would take two to four days to appear.

This loader performs exactly the same transformations, but in bulk and in one
process, so the same 30 days land in silver in minutes rather than days.

It is deliberately faithful to the pipeline it replaces:
    * events   — filtered with 2-parsing/parser.passes_filter (the bronze->silver
                 supply-chain relevance filter)
    * mentions — kept only when their GLOBALEVENTID exists in the events of the
                 same slice (validation's referential-integrity rule)
    * writes   — through 3-validation_and_storage/storage.Storage, so the schema,
                 the sharding and the ReplacingMergeTree dedup are identical
    * gold     — NOT written here. Once silver grows, the processing layer's own
                 watermark trigger fires and builds the gold exactly as it always
                 does, so the gold can never drift from what the pipeline
                 would have produced.

Enrichment (Newspaper3k article titles/keywords) is OFF by default: it scrapes
every article URL and is by far the slowest step. Turn it on with ENRICH=1 if you
want the enriched fields, and expect the load to take hours instead of minutes.

Progress and timings are appended to the report file so the run can be measured.

Usage (see docker-compose.bootstrap.yml):
    docker compose -f docker-compose.bootstrap.yml run --rm bootstrap
"""

import io
import logging
import os
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, "/app/parsing")
sys.path.insert(0, "/app/validation")

from parser import passes_filter, GDELT_COLUMNS, MENTIONS_COLUMNS   # 2-parsing
from gdelt import (EVENT_COLUMNS, MENTION_COLUMNS,                     # 3-validation
                   check_field_width, EXPECTED_FIELD_COUNT)
from storage import Storage                                          # 3-validation

# The two layers name the same GDELT columns differently: parsing calls the key
# "GlobalEventID", while validation and the ClickHouse schema call it
# "GLOBALEVENTID". Both lists describe the same columns in the same order, so the
# frames are read with parsing's names (which passes_filter expects) and then
# renamed positionally to validation's names before being written.

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("bulk_load")

SOURCE_DIR = Path(os.getenv("SOURCE_DIR", "/source"))
REPORT_FILE = Path(os.getenv("REPORT_DIR", "/report")) / "bulk_load_report.csv"
ENRICH = os.getenv("ENRICH", "0") == "1"
BATCH_SLICES = int(os.getenv("BATCH_SLICES", "50"))
LIMIT_SLICES = int(os.getenv("LIMIT_SLICES", "0"))    # 0 = no limit

EVENTS_SUFFIX = ".export.CSV"
MENTIONS_SUFFIX = ".mentions.CSV"


def find_slices() -> dict:
    """
    Map slice-id -> {"events": path, "mentions": path} for every GDELT ZIP found
    anywhere under SOURCE_DIR (the release archive nests them in a sub-folder,
    so the search is recursive).
    """
    slices: dict = defaultdict(dict)
    for path in SOURCE_DIR.rglob("*.zip"):
        name = path.name
        if name.startswith("."):
            continue
        if name.endswith(".export.CSV.zip"):
            slices[name[: -len(".export.CSV.zip")]]["events"] = path
        elif name.endswith(".mentions.CSV.zip"):
            slices[name[: -len(".mentions.CSV.zip")]]["mentions"] = path
    return slices


def read_zip(path: Path, columns: list) -> pd.DataFrame:
    """
    Read the single CSV inside a GDELT ZIP as a header-less, all-string frame.

    Width-checked first, and this site needs it as much as parsing does: the
    loader reads GDELT archives DIRECTLY, never through 2-parsing, so nothing
    upstream has normalised the file. It also writes thousands of slices in one
    run, so an unnoticed column shift here would corrupt a whole backfill rather
    than a single 15-minute slice.
    """
    with zipfile.ZipFile(path) as zf:
        member = zf.namelist()[0]
        raw = zf.read(member)
        kind = "events" if len(columns) >= 61 else "mentions"
        check_field_width(raw, EXPECTED_FIELD_COUNT[kind], kind, path)
        return pd.read_csv(io.BytesIO(raw), sep="\t", header=None, names=columns,
                           dtype=str, keep_default_na=False, low_memory=False)


def main() -> None:
    started = time.time()
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    new_report = not REPORT_FILE.exists()
    report = REPORT_FILE.open("a", encoding="utf-8")
    if new_report:
        report.write("finished_at_utc,slices_done,events_kept,mentions_kept,"
                     "elapsed_seconds,slices_per_minute\n")

    slices = find_slices()
    if not slices:
        log.error("no GDELT ZIPs found under %s — nothing to load", SOURCE_DIR)
        return

    # Only slices with BOTH files are usable: the referential-integrity filter
    # needs the events of the same slice to validate its mentions against.
    complete = sorted(s for s, f in slices.items() if "events" in f and "mentions" in f)
    incomplete = len(slices) - len(complete)
    if LIMIT_SLICES:
        complete = complete[-LIMIT_SLICES:]          # newest N

    log.info("found %d complete slices (%d incomplete, skipped)", len(complete), incomplete)
    log.info("enrichment is %s", "ON (slow)" if ENRICH else "OFF")

    storage = Storage(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse-s1r1"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        database=os.getenv("CLICKHOUSE_DATABASE", "default"),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )
    while True:
        try:
            storage.ensure_tables()
            break
        except Exception as exc:                      # noqa: BLE001
            log.warning("ClickHouse not ready (%s) — retrying in 5s", exc)
            time.sleep(5)

    total_events = total_mentions = 0
    ev_batch: list = []
    mn_batch: list = []

    def flush() -> None:
        nonlocal ev_batch, mn_batch, total_events, total_mentions
        if ev_batch:
            df = pd.concat(ev_batch, ignore_index=True)
            storage.append_events(df)
            total_events += len(df)
            ev_batch = []
        if mn_batch:
            df = pd.concat(mn_batch, ignore_index=True)
            storage.append_mentions(df)
            total_mentions += len(df)
            mn_batch = []

    for n, slice_id in enumerate(complete, start=1):
        files = slices[slice_id]
        try:
            events = read_zip(files["events"], GDELT_COLUMNS)
            mentions = read_zip(files["mentions"], MENTIONS_COLUMNS)
        except Exception as exc:                      # noqa: BLE001
            log.warning("slice %s unreadable (%s) — skipped", slice_id, exc)
            continue

        # bronze -> silver: supply-chain relevance (same filter as 2-parsing).
        # Applied while the frame still carries parsing's column names.
        kept = events[events.apply(lambda r: passes_filter(r.to_dict()), axis=1)].copy()
        if kept.empty:
            continue

        # Switch to the names the silver schema uses (positional: same columns,
        # same order, e.g. GlobalEventID -> GLOBALEVENTID).
        kept.columns = EVENT_COLUMNS
        mentions = mentions.copy()
        mentions.columns = MENTION_COLUMNS[:len(mentions.columns)]

        # validation's referential-integrity rule
        valid_ids = set(pd.to_numeric(kept["GLOBALEVENTID"], errors="coerce")
                          .fillna(0).astype("int64").tolist())
        m_ids = pd.to_numeric(mentions["GLOBALEVENTID"], errors="coerce").fillna(0).astype("int64")
        mentions = mentions[m_ids.isin(valid_ids)].copy()

        # the three enrichment columns must exist — the table has 19 columns
        if ENRICH and not mentions.empty:
            from enrichment import enrich_dataframe
            enrich_dataframe(mentions, url_column="MentionIdentifier",
                             max_workers=int(os.getenv("MENTION_ENRICH_WORKERS", "8")),
                             do_nlp=os.getenv("MENTION_ENRICH_NLP", "0") == "1",
                             time_budget_s=int(os.getenv("ENRICH_TIMEOUT_SECONDS", "60")))
        else:
            mentions["article_title"] = ""
            mentions["article_keywords"] = ""
            mentions["enriched"] = 0

        ev_batch.append(kept)
        if not mentions.empty:
            mn_batch.append(mentions)

        if n % BATCH_SLICES == 0:
            flush()
            elapsed = time.time() - started
            rate = n / (elapsed / 60) if elapsed else 0
            eta = (len(complete) - n) / rate if rate else 0
            log.info("%d/%d slices | %d events, %d mentions | %.1f slices/min | ETA %.1f min",
                     n, len(complete), total_events, total_mentions, rate, eta)

    flush()
    elapsed = time.time() - started
    rate = len(complete) / (elapsed / 60) if elapsed else 0
    log.info("DONE — %d slices, %d events, %d mentions in %.1f min (%.1f slices/min)",
             len(complete), total_events, total_mentions, elapsed / 60, rate)
    report.write(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')},"
                 f"{len(complete)},{total_events},{total_mentions},"
                 f"{elapsed:.0f},{rate:.1f}\n")
    report.close()
    log.info("timings appended to %s", REPORT_FILE)
    storage.close()


if __name__ == "__main__":
    main()
