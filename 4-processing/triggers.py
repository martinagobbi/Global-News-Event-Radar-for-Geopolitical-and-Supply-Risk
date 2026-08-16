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
# Two missed 15-minute slices plus margin. Beyond this, silver has stopped
# growing and the gold on display is no longer being refreshed.
SILVER_STALE_SECONDS = int(os.getenv("SILVER_STALE_SECONDS", str(45 * 60)))


def _silver_watermark_loop(ch_factory, recompute_all, report_stale=None) -> None:
    """
    Recompute everyone whenever the silver store grows.

    The watermark is max(DATEADDED) in gdelt_events. An equal value means nothing
    new arrived. A HIGHER value means silver grew, so gold is rebuilt.

    A LOWER value means silver shrank — `silver_snapshot.sh trim`/`wipe`/
    `recreate`, or a volume recreated by `docker compose down -v` and refilled
    from the seed. This used to be logged and otherwise ignored, on the reasoning
    that rebuilding gold from a shorter history than it already reflects would be
    a downgrade. That was wrong twice over. Gold describing events silver no
    longer holds is not a richer gold, it is an inconsistent one; and refusing to
    adopt the new value left `last` latched at the old high-water mark forever, so
    `last_advance` never reset and the staleness reporter below fired 45 minutes
    later on a pipeline that was working perfectly. The dashboard then showed
    "technical difficulties" until the container happened to be restarted.

    So a backwards move is now treated as what it is — a real change to silver —
    and triggers the same recompute a forwards move does. Nothing here needs to
    distinguish the two; only the log line differs, because the cause is worth
    knowing.

    If the watermark has not advanced within SILVER_STALE_SECONDS, the upstream
    pipeline has stopped delivering. That is reported rather than passed over in
    silence, because gold simply freezing looks identical to gold being current.
    """
    last = None
    last_advance = time.monotonic()
    reported_stale = False

    while True:
        try:
            with ch_factory() as ch:
                watermark = ch.silver_watermark()
            if watermark and last is not None and watermark < last:
                # Checked BEFORE the grew/unchanged case so the log says which
                # happened. recompute_all() is deliberately the same call: gold
                # mirrors silver, whichever direction silver moved.
                logger.warning("Silver watermark went BACKWARDS (%s -> %s) — "
                               "silver was rebuilt or trimmed behind us; "
                               "adopting the new value and recomputing",
                               last, watermark)
                recompute_all()
                last = watermark
                last_advance = time.monotonic()
                reported_stale = False
            elif watermark and (last is None or watermark > last):
                logger.info("Silver watermark %s -> %s; running recompute_all()",
                            last, watermark)
                recompute_all()
                last = watermark
                last_advance = time.monotonic()
                reported_stale = False

            idle = time.monotonic() - last_advance
            if idle > SILVER_STALE_SECONDS and not reported_stale:
                logger.error("Silver has not advanced for %.0f min — the upstream "
                             "pipeline appears stalled; gold is no longer being "
                             "refreshed", idle / 60)
                if report_stale is not None:
                    try:
                        report_stale(int(idle))
                    except Exception as exc:  # noqa: BLE001 — reporting must not crash the loop
                        logger.warning("could not record stale status: %s", exc)
                reported_stale = True
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


def start(ch_factory, recompute_all, recompute_user, report_stale=None) -> None:
    """Launch the two trigger threads as daemons."""
    threading.Thread(
        target=_silver_watermark_loop, args=(ch_factory, recompute_all, report_stale),
        name="silver-watermark", daemon=True,
    ).start()
    threading.Thread(
        target=_users_change_stream_loop, args=(recompute_user,),
        name="users-change-stream", daemon=True,
    ).start()
    logger.info("Background triggers started (watermark poll every %ds)",
                WATERMARK_POLL_SECONDS)
