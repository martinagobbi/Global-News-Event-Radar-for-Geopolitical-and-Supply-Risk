#!/usr/bin/env python
"""
2-parsing/main.py

Environment variables
---------------------
    KAFKA_BOOTSTRAP_SERVERS   bootstrap brokers          (default: kafka:9092)
    KAFKA_TOPIC_RAW           input topic                (default: gdelt_raw)
    KAFKA_CONSUMER_GROUP      consumer group id          (default: parsing-group)
    CLICKHOUSE_HOST           ClickHouse hostname        (default: clickhouse)
    CLICKHOUSE_PORT           ClickHouse native port     (default: 9000)
    CLICKHOUSE_DATABASE       database name              (default: default)
    CLICKHOUSE_USER           username                   (default: default)
    CLICKHOUSE_PASSWORD       password                   (default: "")
    PARSING_BATCH_SIZE        rows per ClickHouse INSERT (default: 500)
"""

import json
import logging
import os
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

from parser import passes_filter, rename_integer_keys, to_silver_event
from clickhouse_writer import ClickHouseWriter

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("parsing")

# ── Configuration from environment ───────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_IN  = os.getenv("KAFKA_TOPIC_RAW",         "gdelt_raw")
KAFKA_GROUP     = os.getenv("KAFKA_CONSUMER_GROUP",    "parsing-group")

CH_HOST     = os.getenv("CLICKHOUSE_HOST",     "clickhouse")
CH_PORT     = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CH_USER     = os.getenv("CLICKHOUSE_USER",     "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

BATCH_SIZE  = int(os.getenv("PARSING_BATCH_SIZE", "500"))

# Seconds to wait between retries when Kafka or ClickHouse is not yet ready
STARTUP_RETRY_DELAY = 5


# ── Kafka consumer factory ────────────────────────────────────────────────────

def build_consumer() -> Consumer:
    conf = {
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           KAFKA_GROUP,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_IN])
    logger.info(
        "Kafka consumer connected to %s, subscribed to topic '%s' (group: %s)",
        KAFKA_BOOTSTRAP, KAFKA_TOPIC_IN, KAFKA_GROUP,
    )
    return consumer


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    # Initialise ClickHouse writer and ensure the silver_events table exists.
    # Retry on startup to handle docker-compose bring-up order.
    writer = None
    while writer is None:
        try:
            writer = ClickHouseWriter(
                host=CH_HOST, port=CH_PORT,
                database=CH_DATABASE, user=CH_USER, password=CH_PASSWORD,
            )
            writer.ensure_table()
        except Exception as exc:
            logger.warning(
                "ClickHouse not ready (%s) — retrying in %ds…",
                exc, STARTUP_RETRY_DELAY,
            )
            writer = None
            time.sleep(STARTUP_RETRY_DELAY)

    # Build the Kafka consumer with the same retry approach.
    consumer = None
    while consumer is None:
        try:
            consumer = build_consumer()
        except KafkaException as exc:
            logger.warning(
                "Kafka not ready (%s) — retrying in %ds…",
                exc, STARTUP_RETRY_DELAY,
            )
            time.sleep(STARTUP_RETRY_DELAY)

    buffer: list[dict] = []
    total_written = 0
    total_seen = 0
    total_passed = 0

    logger.info("Parsing layer started — waiting for messages on '%s'…", KAFKA_TOPIC_IN)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Timeout: flush any pending buffer so events are not held too long.
                if buffer:
                    written = writer.write_batch(buffer)
                    total_written += written
                    logger.info(
                        "Idle flush: wrote %d silver events (cumulative: %d)",
                        written, total_written,
                    )
                    buffer.clear()
                continue

            if msg.error():
                # Partition EOF is informational, not an error.
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka error: %s", msg.error())
                continue

            total_seen += 1

            # ── Deserialise ──────────────────────────────────────────────────
            try:
                raw_record: dict = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.warning("Could not decode message (offset %d): %s", msg.offset(), exc)
                continue

            # ── Rename integer keys → GDELT column names ─────────────────────
            # The ingestion poller sends records with integer keys (0, 1, 2 …)
            # because pandas reads the CSV without specifying column names.
            named_record = rename_integer_keys(raw_record)

            # ── Apply filter: F1 AND (F2 OR F3) AND has_source_url ───────────
            if not passes_filter(named_record):
                continue

            total_passed += 1

            # ── Build silver event with risk score ───────────────────────────
            silver = to_silver_event(named_record)
            buffer.append(silver)

            # ── Flush buffer to ClickHouse ───────────────────────────────────
            if len(buffer) >= BATCH_SIZE:
                written = writer.write_batch(buffer)
                total_written += written
                logger.info(
                    "Batch: wrote %d silver events | seen=%d passed=%d total_written=%d",
                    written, total_seen, total_passed, total_written,
                )
                buffer.clear()

    except KeyboardInterrupt:
        logger.info("Interrupt received — shutting down parsing layer…")
    finally:
        # Flush remaining buffer before exit
        if buffer:
            try:
                writer.write_batch(buffer)
            except Exception as exc:
                logger.error("Failed to flush final buffer: %s", exc)
        consumer.close()
        writer.close()
        logger.info(
            "Parsing layer stopped. Total: seen=%d, passed=%d, written=%d",
            total_seen, total_passed, total_written,
        )


if __name__ == "__main__":
    main()
