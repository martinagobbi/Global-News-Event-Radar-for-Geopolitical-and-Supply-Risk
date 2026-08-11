"""
4-processing/retention.py — the only automatic deletion in the pipeline.

Everything else in this system either appends or rebuilds. Silver is
append-only; gold's `articles` is upserted (and swept of unreferenced rows);
nothing ages out. Left alone, both stores grow for as long as the pipeline runs.

This module removes events that have gone quiet for a decade, and everything
hanging off them:

    an event whose MOST RECENT article is older than RETENTION_YEARS
      -> delete the event      from ClickHouse gdelt_events
      -> delete its mentions   from ClickHouse gdelt_mentions
      -> delete its articles   from Oracle articles / user_articles
      -> drop any user's tag   pointing at it, in MongoDB

Why the newest article and not the event date
---------------------------------------------
A long-running story keeps attracting coverage: the event row is stamped once,
but articles arrive for as long as anyone is still writing. Measuring from the
event date would delete a story that is still being reported; measuring from its
newest article means an event survives exactly as long as the world keeps talking
about it, and ages out ten years after the last word.

Schedule
--------
Once a day at midnight, plus a catch-up on startup when a midnight was missed —
a laptop that sleeps overnight would otherwise never clean up at all. The last
run is recorded in RETENTION_STATE_FILE on the shared volume, written atomically,
so the decision survives a restart.

Ten years is a starting point, not a fixed constant: RETENTION_YEARS sets it, so
shortening the window needs no migration and no code change. It is deliberately
long enough that it deletes nothing the project currently holds — the seed spans
2026, so the cutoff lands in 2016 — which makes it a safe default to ship and an
easy one to tighten once the store has real volume behind it.

Deleting a condemned event's mentions by GLOBALEVENTID is equivalent to testing
each mention's own MentionTimeDate: if the MAXIMUM is below the cutoff then every
mention is, because the maximum is the largest. No mention younger than the cutoff
can be removed. The converse is intended — an old article on a still-active event
survives with it, which also keeps the card ordering stable, since that ordering
is keyed on the oldest article a card holds.
"""

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger("processing.retention")

RETENTION_YEARS = float(os.getenv("RETENTION_YEARS", "10"))
CLICKHOUSE_CLUSTER = os.getenv("CLICKHOUSE_CLUSTER", "gnews_cluster")
STATE_DIR = Path(os.getenv("STATE_DIR", "/data/state"))
RETENTION_STATE_FILE = STATE_DIR / "retention.json"
# How often the scheduler re-checks whether midnight has passed. Short enough to
# start the run promptly, long enough to cost nothing.
TICK_SECONDS = int(os.getenv("RETENTION_TICK_SECONDS", "300"))
# Oracle's IN list caps at 1000 entries.
_ORACLE_IN_CHUNK = 900


# ── Durable last-run marker ──────────────────────────────────────────────────

def _load_last_run() -> datetime | None:
    try:
        with RETENTION_STATE_FILE.open(encoding="utf-8") as fh:
            value = json.load(fh).get("last_run_utc")
        return datetime.fromisoformat(value).replace(tzinfo=timezone.utc) if value else None
    except (OSError, ValueError, AttributeError):
        return None


def _save_last_run(when: datetime) -> None:
    """Write atomically, so a crash mid-write cannot leave a truncated marker."""
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = RETENTION_STATE_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump({"last_run_utc": when.replace(tzinfo=None).isoformat()}, fh)
        os.replace(tmp, RETENTION_STATE_FILE)
    except OSError as exc:
        logger.warning("Could not persist the retention marker: %s", exc)


def _due(now: datetime, last_run: datetime | None) -> bool:
    """
    True when a run is owed: never run before, or not run since the most recent
    midnight. This is what makes a missed midnight catch up on the next start
    rather than being skipped until the following day.
    """
    if last_run is None:
        return True
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return last_run < midnight


# ── The three stores ─────────────────────────────────────────────────────────

def find_expired_event_ids(ch, cutoff: str) -> list[int]:
    """
    GLOBALEVENTIDs whose newest article predates `cutoff` (YYYYMMDDHHMMSS).

    An event with no mentions at all has no "most recent article", so it falls
    back to its own DATEADDED — otherwise such rows could never expire.
    """
    sql = """
        SELECT e.GLOBALEVENTID
        FROM (
            SELECT GLOBALEVENTID, max(toString(DATEADDED)) AS added
            FROM gdelt_events FINAL GROUP BY GLOBALEVENTID
        ) e
        LEFT JOIN (
            SELECT GLOBALEVENTID, max(MentionTimeDate) AS newest
            FROM gdelt_mentions FINAL GROUP BY GLOBALEVENTID
        ) m ON e.GLOBALEVENTID = m.GLOBALEVENTID
        WHERE if(m.newest = '' OR m.newest IS NULL, e.added, m.newest) < %(cutoff)s
    """
    rows = ch._get_client().execute(sql, {"cutoff": cutoff})
    return [int(r[0]) for r in rows]


def delete_from_silver(ch, event_ids: list[int]) -> None:
    """
    Remove the events and their mentions from ClickHouse.

    Mentions first: if the job dies between the two statements, an event with no
    mentions is harmless and will be retried, whereas mentions whose event has
    gone would fail the referential-integrity assumption everything else relies on.

    mutations_sync = 2 waits for every replica, so the counts logged afterwards
    are final rather than optimistic — the same setting silver_snapshot.sh trim
    uses.
    """
    client = ch._get_client()
    for table, column in (("gdelt_mentions_local", "GLOBALEVENTID"),
                          ("gdelt_events_local", "GLOBALEVENTID")):
        client.execute(
            f"ALTER TABLE {table} ON CLUSTER {CLICKHOUSE_CLUSTER} "
            f"DELETE WHERE {column} IN %(ids)s SETTINGS mutations_sync = 2",
            {"ids": event_ids},
        )
        logger.info("silver: deleted rows for %d expired events from %s",
                    len(event_ids), table)


def delete_from_gold(event_ids: list[int]) -> tuple[int, int]:
    """
    Remove the expired events' articles from Oracle.

    user_articles FIRST, then articles: the reverse order would briefly leave
    user_articles rows pointing at articles that no longer exist, which is
    exactly the dangling state the serving join assumes cannot happen.
    """
    import oracle_writer

    ids = [str(e) for e in event_ids]
    removed_links = removed_articles = 0
    with oracle_writer._connect() as conn:
        cur = conn.cursor()
        for start in range(0, len(ids), _ORACLE_IN_CHUNK):
            chunk = ids[start:start + _ORACLE_IN_CHUNK]
            binds = {f"e{n}": v for n, v in enumerate(chunk)}
            placeholders = ", ".join(f":{k}" for k in binds)

            cur.execute(
                f"DELETE FROM user_articles WHERE doc_id IN "
                f"(SELECT doc_id FROM articles WHERE global_event_id IN ({placeholders}))",
                binds)
            removed_links += cur.rowcount

            cur.execute(
                f"DELETE FROM articles WHERE global_event_id IN ({placeholders})", binds)
            removed_articles += cur.rowcount
        conn.commit()

    logger.info("gold: deleted %d articles and %d user_articles links",
                removed_articles, removed_links)
    return removed_articles, removed_links


def delete_tags(event_ids: list[int]) -> int:
    """
    Drop every user's tag pointing at an expired event.

    Without this a triaged card becomes a permanent dead reference: the tag stays
    in MongoDB, the article is gone from Oracle, and the triage page silently
    shows one fewer item than the user filed, forever.
    """
    import mongo_reader

    wanted = {str(e) for e in event_ids}
    cleared = 0
    try:
        tags = mongo_reader._get_db()["tags"]
        for doc in list(tags.find({}, {"tags": 1})):
            current = doc.get("tags") or {}
            keep = {k: v for k, v in current.items() if str(k) not in wanted}
            if len(keep) != len(current):
                tags.update_one({"_id": doc["_id"]}, {"$set": {"tags": keep}})
                cleared += len(current) - len(keep)
    except Exception as exc:  # noqa: BLE001 — never let tag cleanup abort the job
        logger.warning("Could not clear tags for expired events: %s", exc)
    if cleared:
        logger.info("tags: cleared %d references to expired events", cleared)
    return cleared


# ── The job ──────────────────────────────────────────────────────────────────

def run_once(ch_factory, now: datetime | None = None) -> dict:
    """Find expired events and remove them from silver, gold and the tag store."""
    now = now or datetime.now(timezone.utc)
    cutoff_dt = now - timedelta(days=RETENTION_YEARS * 365.25)
    cutoff = cutoff_dt.strftime("%Y%m%d%H%M%S")

    with ch_factory() as ch:
        expired = find_expired_event_ids(ch, cutoff)
        if not expired:
            logger.info("retention: nothing older than %s (%.0f years) — no deletions",
                        cutoff_dt.date(), RETENTION_YEARS)
            return {"cutoff": cutoff, "events": 0, "articles": 0, "tags": 0}

        logger.info("retention: %d events have had no article since %s; removing",
                    len(expired), cutoff_dt.date())
        delete_from_silver(ch, expired)

    articles, _ = delete_from_gold(expired)
    tags = delete_tags(expired)
    return {"cutoff": cutoff, "events": len(expired), "articles": articles, "tags": tags}


def _loop(ch_factory) -> None:
    """Run when due, then re-check every TICK_SECONDS. Never exits."""
    while True:
        try:
            now = datetime.now(timezone.utc)
            if _due(now, _load_last_run()):
                run_once(ch_factory, now)
                _save_last_run(now)
        except Exception as exc:  # noqa: BLE001 — best-effort, never kill the thread
            logger.warning("retention run failed: %s", exc)
        time.sleep(TICK_SECONDS)


def start(ch_factory) -> None:
    """Launch the daily retention job as a daemon thread."""
    threading.Thread(target=_loop, args=(ch_factory,),
                     name="retention", daemon=True).start()
    logger.info("Retention job started (%.0f-year cutoff, checked every %ds)",
                RETENTION_YEARS, TICK_SECONDS)
