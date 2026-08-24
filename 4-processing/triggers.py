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
# How long to wait, after the LAST profile change, before actually recomputing —
# and the hard ceiling on how long a burst can postpone recomputing at all.
#
# Motivation: seeding N test users writes N profiles in quick succession, and
# without this, each write independently triggers recompute_user(), which
# rebuilds the whole catalogue (~90s for 3 users, measured) from scratch every
# time — 3 users cost 3x the catalogue build for work that could share ONE.
# Debouncing collapses a burst into a single recompute, chosen once the burst
# goes quiet.
#
# DEBOUNCE_SECONDS is the quiet-period trigger: reset on every new change, so a
# steady trickle of edits keeps postponing. MAX_WAIT_SECONDS bounds that: a
# burst that never goes quiet for DEBOUNCE_SECONDS still flushes once this much
# time has passed since the FIRST unflushed change, so profile edits are never
# delayed indefinitely.
CHANGE_STREAM_DEBOUNCE_SECONDS = float(os.getenv("CHANGE_STREAM_DEBOUNCE_SECONDS", "2"))
CHANGE_STREAM_DEBOUNCE_MAX_WAIT_SECONDS = float(
    os.getenv("CHANGE_STREAM_DEBOUNCE_MAX_WAIT_SECONDS", "10"))
# Upper bound per poll of the change stream cursor, so the debounce loop wakes
# up often enough to notice "quiet for DEBOUNCE_SECONDS" promptly rather than
# blocking in the driver for an unbounded getMore.
CHANGE_STREAM_MAX_AWAIT_MS = int(os.getenv("CHANGE_STREAM_MAX_AWAIT_MS", "500"))
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


def _flush_pending(pending: set[str], recompute_user, recompute_all) -> None:
    """
    Apply a debounced batch of changed user ids with the cheapest call that
    covers it.

    ONE uid -> recompute_user(uid). Preserves today's behaviour for the common
    case (a single person editing their own preferences): no catalogue rebuilt
    for anyone else.

    MORE THAN ONE -> recompute_all(). This is deliberately the SAME full
    rebuild the watermark trigger uses, not a "recompute just these uids"
    variant — spark_gold.recompute(only_user=...) only accepts one user, and
    building a multi-user variant would duplicate recompute_all's catalogue
    logic for a path that exists purely to save the catalogue build once. It is
    also strictly safe: recompute_all() evaluates every profile, changed or
    not, so unrelated users are simply re-confirmed rather than affected.
    """
    if not pending:
        return
    users = sorted(pending)
    try:
        if len(users) == 1:
            logger.info("Debounced change for %s -> recompute_user()", users[0])
            recompute_user(users[0])
        else:
            logger.info("Debounced changes for %d users (%s) -> recompute_all()",
                        len(users), ", ".join(users))
            recompute_all()
    except Exception as exc:  # noqa: BLE001
        logger.warning("recompute for %s failed: %s", users, exc)


def _users_change_stream_loop(recompute_user, recompute_all) -> None:
    """
    Recompute changed users' gold, debounced.

    A burst of near-simultaneous profile writes — bulk-seeding test accounts is
    the motivating case, see CHANGE_STREAM_DEBOUNCE_SECONDS above — used to
    trigger one independent recompute_user() PER WRITE, each rebuilding the
    whole candidate catalogue from scratch. Debouncing collapses a burst into
    one recompute, chosen once new changes stop arriving (or after
    CHANGE_STREAM_DEBOUNCE_MAX_WAIT_SECONDS, whichever comes first).

    try_next() (not the blocking `for change in stream` this replaced) is what
    makes debouncing possible: it returns None on a quiet cursor instead of
    blocking indefinitely, so the loop can notice "no new change for
    DEBOUNCE_SECONDS" and flush. max_await_time_ms bounds each individual poll
    so that noticing is prompt rather than waiting out a long server-side
    getMore first.
    """
    while True:
        try:
            collection = mongo_reader.users_collection()
            logger.info("Watching '%s' change stream for profile updates…",
                        collection.name)
            with collection.watch(full_document="updateLookup",
                                  max_await_time_ms=CHANGE_STREAM_MAX_AWAIT_MS) as stream:
                pending: set[str] = set()
                first_pending_at = None
                last_change_at = None
                while True:
                    change = stream.try_next()
                    now = time.monotonic()

                    if change is not None:
                        if change.get("operationType") in ("insert", "update", "replace"):
                            doc = change.get("fullDocument") or {}
                            doc_key = change.get("documentKey") or {}
                            uid = str(doc.get("_id") or doc_key.get("_id")
                                      or doc.get("user_id") or "")
                            if uid:
                                if not pending:
                                    first_pending_at = now
                                pending.add(uid)
                                last_change_at = now
                        continue    # check for more without waiting out the debounce

                    # Cursor went quiet for this poll. Decide whether to flush.
                    if pending and (
                        now - last_change_at >= CHANGE_STREAM_DEBOUNCE_SECONDS
                        or now - first_pending_at >= CHANGE_STREAM_DEBOUNCE_MAX_WAIT_SECONDS
                    ):
                        _flush_pending(pending, recompute_user, recompute_all)
                        pending = set()
                        first_pending_at = last_change_at = None
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
        target=_users_change_stream_loop, args=(recompute_user, recompute_all),
        name="users-change-stream", daemon=True,
    ).start()
    logger.info("Background triggers started (watermark poll every %ds)",
                WATERMARK_POLL_SECONDS)
