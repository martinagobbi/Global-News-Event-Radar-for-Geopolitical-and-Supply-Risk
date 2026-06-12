#!/usr/bin/env python
"""
2-parsing/main.py
-----------------
Parsing layer — a (near) raw pass-through that turns the per-row Kafka stream
back into per-15-minute GDELT files and drops them into latest_files for the
validation layer.

Flow
----
Ingestion publishes each GDELT row to Kafka (one message per row) on two topics:
    gdelt_events_raw    — raw Events rows   (61 columns)
    gdelt_mentions_raw  — raw Mentions rows (16 columns)

This layer reassembles those rows into the original 15-minute "slices":
    * events   — kept ONLY if they pass the supply-chain relevance filter, then
                 written RAW (untransformed) so validation sees real GDELT columns.
    * mentions — passed through COMPLETELY RAW: no filtering, no enrichment, no
                 extra columns. (Validation does the GLOBALEVENTID filter and the
                 Newspaper3k enrichment.)

A "slice" is identified by its 15-minute timestamp, which is uniform within one
GDELT file: DATEADDED (events, column 59) and MentionTimeDate (mentions, col 2).

Flush conditions — a slice T is closed and queued for publishing when EITHER:
    1. BOTH topics have advanced past T, i.e. min(newest_event_ts,
       newest_mention_ts) > T. Each topic preserves its own order, so once both
       have shown a newer slice, every row of T has been consumed. This is the
       catch-up/backlog trigger and is immune to cross-topic lag.
    2. No new row has arrived for T within PARSING_SLICE_IDLE_SECONDS — the live
       (15-min cadence) trigger and the safety net for the newest slice.

Back-pressure: completed slices are published ONE pair at a time, only when
latest_files is empty, so validation (which can take minutes to enrich) is never
handed more than one pair at once.

Environment variables
---------------------
    KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPIC_EVENTS / KAFKA_TOPIC_MENTIONS / KAFKA_CONSUMER_GROUP
    LATEST_FILES_DIR             output dir (default /data/latest_files)
    PARSING_SLICE_IDLE_SECONDS   idle flush threshold (default 90)
    POLL_TIMEOUT                 Kafka poll timeout, seconds (default 1.0)
"""

import json
import logging
import os
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException

from parser import passes_filter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("parsing")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_EVENTS    = os.getenv("KAFKA_TOPIC_EVENTS",   "gdelt_events_raw")
TOPIC_MENTIONS  = os.getenv("KAFKA_TOPIC_MENTIONS", "gdelt_mentions_raw")
KAFKA_GROUP     = os.getenv("KAFKA_CONSUMER_GROUP", "parsing-group")

LATEST_FILES_DIR = Path(os.getenv("LATEST_FILES_DIR", "/data/latest_files"))
IDLE_SECONDS     = int(os.getenv("PARSING_SLICE_IDLE_SECONDS", "90"))
POLL_TIMEOUT     = float(os.getenv("POLL_TIMEOUT", "1.0"))
STARTUP_RETRY_DELAY = 5

# Column counts and the index of the 15-minute slice timestamp in each table.
EVENT_NCOLS = 61
MENTION_NCOLS = 16
EVENT_SLICE_IDX = 59     # DATEADDED
MENTION_SLICE_IDX = 2    # MentionTimeDate


def build_consumer() -> Consumer:
    conf = {
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           KAFKA_GROUP,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_EVENTS, TOPIC_MENTIONS])
    logger.info("Kafka consumer connected to %s; topics '%s','%s' (group %s)",
                KAFKA_BOOTSTRAP, TOPIC_EVENTS, TOPIC_MENTIONS, KAFKA_GROUP)
    return consumer


# ── Raw-row helpers ───────────────────────────────────────────────────────────

def _field(record: dict, i: int) -> str:
    """Read column i from a JSON record whose keys are "0".."N" (or ints)."""
    v = record.get(str(i), record.get(i, ""))
    return "" if v is None else str(v)


def _raw_line(record: dict, ncols: int) -> str:
    """Reconstruct the original tab-separated GDELT row from the record."""
    return "\t".join(_field(record, i) for i in range(ncols))


def _atomic_write(path: Path, text: str) -> None:
    """Write via a hidden temp file + rename so readers never see a partial file.
    Validation ignores dotfiles, so the temp stays invisible until the rename."""
    tmp = path.with_name("." + path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _latest_files_empty() -> bool:
    if not LATEST_FILES_DIR.exists():
        return True
    return not any(p.is_file() and not p.name.startswith(".")
                   for p in LATEST_FILES_DIR.iterdir())


def _publish(slice_ts: str, slc: dict) -> None:
    """Write the raw events+mentions pair for one slice into latest_files."""
    events_text   = "\n".join(slc["events"])   + ("\n" if slc["events"]   else "")
    mentions_text = "\n".join(slc["mentions"]) + ("\n" if slc["mentions"] else "")
    _atomic_write(LATEST_FILES_DIR / f"{slice_ts}.export.CSV",   events_text)
    _atomic_write(LATEST_FILES_DIR / f"{slice_ts}.mentions.CSV", mentions_text)
    logger.info("Published slice %s (events=%d, mentions=%d)",
                slice_ts, len(slc["events"]), len(slc["mentions"]))


# ── Main loop ───────────────────────────────────────────────────────────────

def main() -> None:
    LATEST_FILES_DIR.mkdir(parents=True, exist_ok=True)

    consumer = None
    while consumer is None:
        try:
            consumer = build_consumer()
        except KafkaException as exc:
            logger.warning("Kafka not ready (%s) — retry in %ds…", exc, STARTUP_RETRY_DELAY)
            time.sleep(STARTUP_RETRY_DELAY)

    slices: dict[str, dict] = {}     # slice_ts -> {events:[], mentions:[], last_update}
    completed: list[tuple] = []      # ordered (slice_ts, slice) ready to publish
    newest_event = ""
    newest_mention = ""

    def _close(ts: str) -> None:
        if ts in slices:
            completed.append((ts, slices.pop(ts)))
            completed.sort(key=lambda x: x[0])

    def _close_older_than(safe_ts: str) -> None:
        if not safe_ts:
            return
        for ts in [t for t in list(slices) if t < safe_ts]:
            _close(ts)

    def _close_idle(now: float) -> None:
        for ts in [t for t, s in list(slices.items())
                   if now - s["last_update"] > IDLE_SECONDS]:
            _close(ts)

    def _publish_if_clear() -> None:
        if completed and _latest_files_empty():
            ts, slc = completed.pop(0)
            try:
                _publish(ts, slc)
            except Exception as exc:  # noqa: BLE001 — retry on next tick
                logger.error("Publish failed for slice %s: %s", ts, exc)
                completed.insert(0, (ts, slc))

    logger.info("Parsing started — reassembling slices into %s", LATEST_FILES_DIR)
    try:
        while True:
            msg = consumer.poll(timeout=POLL_TIMEOUT)
            now = time.monotonic()

            if msg is None:                          # idle tick
                _close_idle(now)
                _publish_if_clear()
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka error: %s", msg.error())
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.warning("Bad message at offset %s: %s", msg.offset(), exc)
                continue

            topic = msg.topic()
            if topic == TOPIC_EVENTS:
                ts = _field(record, EVENT_SLICE_IDX)
                if not ts:
                    continue
                if ts > newest_event:
                    newest_event = ts
                if passes_filter(record):            # only relevant events kept
                    slc = slices.get(ts)
                    if slc is None:
                        slc = slices[ts] = {"events": [], "mentions": [], "last_update": now}
                    slc["events"].append(_raw_line(record, EVENT_NCOLS))
                    slc["last_update"] = now
            elif topic == TOPIC_MENTIONS:
                ts = _field(record, MENTION_SLICE_IDX)
                if not ts:
                    continue
                if ts > newest_mention:
                    newest_mention = ts
                slc = slices.get(ts)                 # mentions: keep ALL, raw
                if slc is None:
                    slc = slices[ts] = {"events": [], "mentions": [], "last_update": now}
                slc["mentions"].append(_raw_line(record, MENTION_NCOLS))
                slc["last_update"] = now
            else:
                continue

            # Condition 1 — both topics moved past a slice ⇒ that slice is complete.
            safe_ts = min(newest_event, newest_mention) if (newest_event and newest_mention) else ""
            _close_older_than(safe_ts)
            # Condition 2 — idle.
            _close_idle(now)
            _publish_if_clear()

    except KeyboardInterrupt:
        logger.info("Interrupt received — shutting down parsing layer…")
    finally:
        consumer.close()
        pending = len(slices) + len(completed)
        if pending:
            logger.warning("Stopped with %d slice(s) not yet published", pending)
        logger.info("Parsing stopped.")


if __name__ == "__main__":
    main()
