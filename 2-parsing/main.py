#!/usr/bin/env python
"""
2-parsing/main.py
------------------
Parsing layer — consumes TWO GDELT streams from Kafka and writes two silver
tables to ClickHouse:

    gdelt_events_raw    → filter (supply-chain relevance) → silver_events
    gdelt_mentions_raw  → Newspaper3k enrichment          → silver_mentions

The ingestion poller publishes the events CSV to 'gdelt_events_raw' and the
mentions CSV to 'gdelt_mentions_raw'. Both arrive as JSON dicts with integer
keys (pandas reads the CSV without a header), so each is renamed to GDELT
column names before processing.

Mentions handling
-----------------
Mentions reference events (via GlobalEventID). To avoid scraping URLs for
events we have already discarded, by default only mentions whose event_id
belongs to a relevant event (one that passed the events filter) are enriched
and stored. This keeps scraping volume proportional to relevant events.
Set ENRICH_ONLY_RELEVANT=false to enrich every mention instead.

Limitation: relevance is tracked in memory from events seen so far. Within a
15-minute cycle the poller sends events before mentions, so ordering is fine
in practice; a mention whose event arrives in a *later* cycle would be missed.
For a fully order-independent design, cross-reference silver_events in
ClickHouse instead of the in-memory set.

Parallelization
---------------
Article scraping (the slow, network-bound step) is parallelized with a thread
pool inside enrichment.enrich_mentions_parallel(); worker count is set by
MENTION_ENRICH_WORKERS. Event filtering is CPU-light and stays single-threaded.

Environment variables
---------------------
    KAFKA_BOOTSTRAP_SERVERS   bootstrap brokers           (default: kafka:9092)
    KAFKA_TOPIC_EVENTS        events topic                (default: gdelt_events_raw)
    KAFKA_TOPIC_MENTIONS      mentions topic              (default: gdelt_mentions_raw)
    KAFKA_CONSUMER_GROUP      consumer group id           (default: parsing-group)
    CLICKHOUSE_HOST/PORT/DATABASE/USER/PASSWORD
    PARSING_BATCH_SIZE        events per ClickHouse INSERT (default: 500)
    MENTION_BATCH_SIZE        mentions per enrich+write    (default: 200)
    MENTION_ENRICH_WORKERS    scraping threads             (default: 8)
    MENTION_ENRICH_NLP        extract keywords (1/0)       (default: 1)
    ENRICH_ONLY_RELEVANT      only enrich relevant (1/0)   (default: 1)
    RELEVANT_IDS_MAX          cap on in-memory id set      (default: 200000)
"""

import json
import logging
import os
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

from parser import (
    passes_filter, rename_integer_keys, to_silver_event,
    rename_mention_integer_keys, to_silver_mention,
)
from enrichment import enrich_mentions_parallel
from clickhouse_writer import ClickHouseWriter

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("parsing")

# Configuration
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_EVENTS      = os.getenv("KAFKA_TOPIC_EVENTS",   "gdelt_events_raw")
TOPIC_MENTIONS    = os.getenv("KAFKA_TOPIC_MENTIONS", "gdelt_mentions_raw")
KAFKA_GROUP       = os.getenv("KAFKA_CONSUMER_GROUP", "parsing-group")

CH_HOST     = os.getenv("CLICKHOUSE_HOST",     "clickhouse")
CH_PORT     = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CH_USER     = os.getenv("CLICKHOUSE_USER",     "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

EVENT_BATCH_SIZE   = int(os.getenv("PARSING_BATCH_SIZE",   "500"))
MENTION_BATCH_SIZE = int(os.getenv("MENTION_BATCH_SIZE",   "200"))
ENRICH_WORKERS     = int(os.getenv("MENTION_ENRICH_WORKERS", "8"))
ENRICH_NLP         = os.getenv("MENTION_ENRICH_NLP", "1") == "1"
ENRICH_ONLY_RELEVANT = os.getenv("ENRICH_ONLY_RELEVANT", "1") == "1"
RELEVANT_IDS_MAX   = int(os.getenv("RELEVANT_IDS_MAX", "200000"))

STARTUP_RETRY_DELAY = 5


def build_consumer() -> Consumer:
    conf = {
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           KAFKA_GROUP,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_EVENTS, TOPIC_MENTIONS])
    logger.info(
        "Kafka consumer connected to %s, subscribed to '%s' and '%s' (group: %s)",
        KAFKA_BOOTSTRAP, TOPIC_EVENTS, TOPIC_MENTIONS, KAFKA_GROUP,
    )
    return consumer


# Flush helpers

def flush_events(writer, buffer, relevant_ids) -> int:
    """Write a batch of silver events and record their ids as relevant."""
    if not buffer:
        return 0
    written = writer.write_batch(buffer)
    for ev in buffer:
        if len(relevant_ids) < RELEVANT_IDS_MAX:
            relevant_ids.add(ev.get("event_id", ""))
    buffer.clear()
    return written


def flush_mentions(writer, buffer) -> int:
    """Enrich a batch of mentions in parallel, then write them."""
    if not buffer:
        return 0
    enrich_mentions_parallel(buffer, max_workers=ENRICH_WORKERS, do_nlp=ENRICH_NLP)
    written = writer.write_mentions_batch(buffer)
    buffer.clear()
    return written


# Main loop

def main() -> None:
    # ClickHouse (retry until ready)
    writer = None
    while writer is None:
        try:
            writer = ClickHouseWriter(
                host=CH_HOST, port=CH_PORT,
                database=CH_DATABASE, user=CH_USER, password=CH_PASSWORD,
            )
            writer.ensure_table()
        except Exception as exc:
            logger.warning("ClickHouse not ready (%s) — retrying in %ds…", exc, STARTUP_RETRY_DELAY)
            writer = None
            time.sleep(STARTUP_RETRY_DELAY)

    # Kafka (retry until ready)
    consumer = None
    while consumer is None:
        try:
            consumer = build_consumer()
        except KafkaException as exc:
            logger.warning("Kafka not ready (%s) — retrying in %ds…", exc, STARTUP_RETRY_DELAY)
            time.sleep(STARTUP_RETRY_DELAY)

    event_buffer: list[dict] = []
    mention_buffer: list[dict] = []
    relevant_ids: set[str] = set()

    totals = {"events_written": 0, "mentions_written": 0,
              "events_seen": 0, "mentions_seen": 0, "mentions_skipped": 0}

    logger.info("Parsing layer started — consuming events + mentions…")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Idle: flush both buffers so nothing is held too long
                totals["events_written"]   += flush_events(writer, event_buffer, relevant_ids)
                totals["mentions_written"] += flush_mentions(writer, mention_buffer)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka error: %s", msg.error())
                continue

            # Deserialise
            try:
                raw_record: dict = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.warning("Could not decode message (offset %d): %s", msg.offset(), exc)
                continue

            topic = msg.topic()

            # EVENTS
            if topic == TOPIC_EVENTS:
                totals["events_seen"] += 1
                named = rename_integer_keys(raw_record)
                if not passes_filter(named):
                    continue
                event_buffer.append(to_silver_event(named))
                if len(event_buffer) >= EVENT_BATCH_SIZE:
                    totals["events_written"] += flush_events(writer, event_buffer, relevant_ids)
                    logger.info(
                        "Events batch | seen=%d written=%d relevant_ids=%d",
                        totals["events_seen"], totals["events_written"], len(relevant_ids),
                    )

            # MENTIONS
            elif topic == TOPIC_MENTIONS:
                totals["mentions_seen"] += 1
                named = rename_mention_integer_keys(raw_record)
                mention = to_silver_mention(named)

                # Only enrich mentions of events we kept (cheaper scraping)
                if ENRICH_ONLY_RELEVANT and mention["event_id"] not in relevant_ids:
                    totals["mentions_skipped"] += 1
                    continue

                mention_buffer.append(mention)
                if len(mention_buffer) >= MENTION_BATCH_SIZE:
                    totals["mentions_written"] += flush_mentions(writer, mention_buffer)
                    logger.info(
                        "Mentions batch | seen=%d written=%d skipped=%d",
                        totals["mentions_seen"], totals["mentions_written"],
                        totals["mentions_skipped"],
                    )

            else:
                logger.debug("Ignoring message from unexpected topic: %s", topic)

    except KeyboardInterrupt:
        logger.info("Interrupt received — shutting down parsing layer…")
    finally:
        try:
            flush_events(writer, event_buffer, relevant_ids)
            flush_mentions(writer, mention_buffer)
        except Exception as exc:
            logger.error("Failed to flush final buffers: %s", exc)
        consumer.close()
        writer.close()
        logger.info(
            "Parsing stopped. events: seen=%d written=%d | mentions: seen=%d "
            "written=%d skipped=%d",
            totals["events_seen"], totals["events_written"],
            totals["mentions_seen"], totals["mentions_written"],
            totals["mentions_skipped"],
        )


if __name__ == "__main__":
    main()
