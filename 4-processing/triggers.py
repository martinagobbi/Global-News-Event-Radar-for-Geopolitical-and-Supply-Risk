"""
4-processing/triggers.py — background triggers that drive the recompute funcs.

Two daemon threads, started on FastAPI startup:

  * silver-watermark poller — polls ClickHouse max(DATEADDED) on gdelt_events;
    when it advances (validation appended a fresh 15-min batch) -> recompute_all().
    This keeps processing a PURE READER: no marker has to be written by the
    validation layer (ClickHouse has no change stream, so we poll a cheap
    monotonic aggregate instead).
  * users change-stream watcher — watches the Mongo radar.users collection; on
    insert / update / replace of a profile -> recompute_user(uid). Requires the
    rs0 replica set (which is what enables Mongo change streams).

Both threads are best-effort and self-healing: they log and retry on error and
never take down the API process.
"""

import logging
import os
import threading
import time

import mongo_reader

logger = logging.getLogger("processing.triggers")

WATERMARK_POLL_SECONDS = int(os.getenv("WATERMARK_POLL_SECONDS", "60"))
CHANGE_STREAM_RETRY_SECONDS = int(os.getenv("CHANGE_STREAM_RETRY_SECONDS", "5"))


def _silver_watermark_loop(ch_factory, recompute_all) -> None:
    """Recompute everyone whenever the silver store grows (new DATEADDED)."""
    last = None
    while True:
        try:
            with ch_factory() as ch:
                watermark = ch.silver_watermark()
            if watermark and watermark != last:
                logger.info("Silver watermark %s -> %s; running recompute_all()",
                            last, watermark)
                recompute_all()
                last = watermark
        except Exception as exc:  # noqa: BLE001 — best-effort, never crash
            logger.warning("silver watermark poll failed: %s", exc)
        time.sleep(WATERMARK_POLL_SECONDS)


def _users_change_stream_loop(recompute_user) -> None:
    """Recompute a single user whenever their Mongo profile changes."""
    while True:
        try:
            collection = mongo_reader.users_collection()
            logger.info("Watching '%s' change stream for profile updates…",
                        collection.name)
            with collection.watch(full_document="updateLookup") as stream:
                for change in stream:
                    if change.get("operationType") not in ("insert", "update", "replace"):
                        continue
                    doc = change.get("fullDocument") or {}
                    doc_key = change.get("documentKey") or {}
                    uid = str(doc.get("_id") or doc_key.get("_id")
                              or doc.get("user_id") or "")
                    if not uid:
                        continue
                    logger.info("Profile %s (%s) -> recompute_user()",
                                uid, change.get("operationType"))
                    try:
                        recompute_user(uid)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("recompute_user(%s) failed: %s", uid, exc)
        except Exception as exc:  # noqa: BLE001 — retry (e.g. rs not yet initiated)
            logger.warning("users change stream error: %s; retry in %ds",
                           exc, CHANGE_STREAM_RETRY_SECONDS)
            time.sleep(CHANGE_STREAM_RETRY_SECONDS)


def start(ch_factory, recompute_all, recompute_user) -> None:
    """Launch the two trigger threads as daemons."""
    threading.Thread(
        target=_silver_watermark_loop, args=(ch_factory, recompute_all),
        name="silver-watermark", daemon=True,
    ).start()
    threading.Thread(
        target=_users_change_stream_loop, args=(recompute_user,),
        name="users-change-stream", daemon=True,
    ).start()
    logger.info("Background triggers started (watermark poll every %ds)",
                WATERMARK_POLL_SECONDS)
