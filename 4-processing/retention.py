"""
4-processing/retention.py — the pipeline's automatic cleanup job.

Everything else in this system either appends or rebuilds. Silver is
append-only; gold's `articles` is upserted (and swept of unreferenced rows);
nothing ages out. Left alone, both stores grow for as long as the pipeline runs.

This module removes stale data that is older than the configured retention window:

    * events whose last article is older than RETENTION_DAYS, and everything
      hanging off them in silver + gold + Mongo tags;
    * dead-lettered files older than one year, as recorded in the dead-letter log
      kept under DEAD_LETTER_DIR. The CSV log itself is never deleted.

The dead-letter log is intentionally not part of the retention cleanup: it is a
record of what was abandoned, where records over one year old are automatically wiped.

This module removes events that have gone quiet for a year, and everything
hanging off them:

    an event whose MOST RECENT article is older than RETENTION_DAYS
      -> delete the event      from ClickHouse gdelt_events
      -> delete its mentions   from ClickHouse gdelt_mentions
      -> delete its articles   from PostgreSQL articles / user_articles
      -> drop any user's tag   pointing at it, in MongoDB

Why the newest article and not the event date
---------------------------------------------
A long-running story keeps attracting coverage: the event row is stamped once,
but articles arrive for as long as anyone is still writing. Measuring from the
event date would delete a story that is still being reported; measuring from its
newest article means an event survives exactly as long as the world keeps talking
about it, and ages out 185 days after the last word.

Schedule
--------
Once a day at midnight, plus a catch-up on startup when a midnight was missed —
a laptop that sleeps overnight would otherwise never clean up at all. The last
run is recorded in RETENTION_STATE_FILE on the shared volume, written atomically,
so the decision survives a restart.

365 days is a starting point, not a fixed constant: RETENTION_DAYS sets it, so
changing the window needs no migration and no code change. It is still long
enough that it deletes nothing the project currently holds — silver spans
2026-06-27 to 2026-08-16, so a cutoff at 2025-08-16 matches no row (verified:
0 events, 0 mentions) — but unlike the ten-year window it replaced, it is short
enough to actually fire once the store has a year of history behind it.

Deleting a condemned event's mentions by GLOBALEVENTID is equivalent to testing
each mention's own MentionTimeDate: if the MAXIMUM is below the cutoff then every
mention is, because the maximum is the largest. No mention younger than the cutoff
can be removed. The converse is intended — an old article on a still-active event
survives with it, which also keeps the card ordering stable, since that ordering
is keyed on the oldest article a card holds.
"""

import csv
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger("processing.retention")

RETENTION_DAYS = float(os.getenv("RETENTION_DAYS", "185"))
DEAD_LETTER_RETENTION_DAYS = float(os.getenv("DEAD_LETTER_RETENTION_DAYS", "365"))
CLICKHOUSE_CLUSTER = os.getenv("CLICKHOUSE_CLUSTER", "gnews_cluster")
STATE_DIR = Path(os.getenv("STATE_DIR", "/data/state"))
RETENTION_STATE_FILE = STATE_DIR / "retention.json"
DEAD_LETTER_DIR = Path(os.getenv("DEAD_LETTER_DIR", "/data/dead_letter"))
DEAD_LETTER_LOG_FILE = DEAD_LETTER_DIR / "dead_letter_log.csv"
# How often the scheduler re-checks whether midnight has passed. Short enough to
# start the run promptly, long enough to cost nothing.
TICK_SECONDS = int(os.getenv("RETENTION_TICK_SECONDS", "300"))
# Ids per ClickHouse statement. Well under the ~21,800 that max_query_size allows,
# so a batch cannot approach the limit even if ids grow longer.
ID_CHUNK = int(os.getenv("RETENTION_ID_CHUNK", "10000"))


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


# ── Statement sizing ─────────────────────────────────────────────────────────

def _chunked(ids: list[int], size: int | None = None):
    """
    Split an id list into statement-sized batches.

    clickhouse-driver substitutes an `IN %(ids)s` list into the SQL text on the
    client, so the statement grows with the number of ids and is capped by the
    server's max_query_size (256 KB by default). At roughly 12 bytes per id that
    is about 21,800 ids, or six days of this pipeline's output.

    A normal nightly run is far below that — only the events that crossed the
    cutoff that day expire, some 3,500 of them, about 41 KB. That figure is one
    day of this pipeline's output, so it does not change with the window length.
    The cases that overflow are the ones where a single run covers a longer
    stretch: a machine that was off for a week or more (the catch-up runs once and
    clears the whole backlog), or the first run after RETENTION_DAYS is shortened,
    which retires a long stretch of history at once. Batching costs nothing in the
    normal case and removes the cliff in those.

    `size` is resolved at CALL time, not bound as a default argument: a default is
    evaluated once when the function is defined, which would freeze ID_CHUNK at
    import and make the batch size impossible to change afterwards — including
    from a test.
    """
    size = size or ID_CHUNK
    for start in range(0, len(ids), size):
        yield ids[start:start + size]


# ── The three stores ─────────────────────────────────────────────────────────

def find_expired_event_ids(ch, cutoff: str) -> list[int]:
    """
    GLOBALEVENTIDs whose newest article predates `cutoff` (YYYYMMDDHHMMSS).

    An event with no usable article timestamp has no "most recent article", so it
    falls back to its own DATEADDED — otherwise such rows could never expire.

    Deliberately THREE single-table queries combined in Python, with literal id
    lists and no nested table reads. gdelt_events and gdelt_mentions are
    Distributed tables, and ClickHouse denies a distributed subquery inside a
    distributed query by default:

        Code: 288. Double-distributed IN/JOIN subqueries is denied
                   (distributed_product_mode = 'deny')

    Measured on a real two-shard cluster: `WHERE GLOBALEVENTID IN (SELECT ...)`
    fails with exactly that, while the same query with a literal id list succeeds.
    A top-level JOIN of two such subqueries — which this function used to use —
    turns out to be accepted by the current version and returned the right answer
    on two shards, so this is not a bug fix; it is removing a dependence on a
    distinction thin enough to break quietly. Every other ClickHouse query in the
    project already sticks to plain aggregates and literal IN lists.

    The split is exact rather than approximate because both tables shard on
    cityHash64(GLOBALEVENTID): every mention of an event lives on the same shard
    as the event, so each shard's GROUP BY GLOBALEVENTID is already complete and
    the merged aggregates are correct.

        A = events whose newest usable mention time is older than the cutoff
        D = events whose own DATEADDED is older than the cutoff
        H = those of D that have a usable mention time (so do NOT use the fallback)
        expired = A | (D - H)

    The `!= ''` guard is load-bearing. An empty string sorts BELOW any digit
    string, so without it an event whose mention timestamps were all blank would
    look ancient and be deleted regardless of its DATEADDED — silently dropping
    the fallback the previous query performed with `if(m.newest = '', ...)`.
    """
    client = ch._get_client()

    a_rows = client.execute(
        "SELECT GLOBALEVENTID FROM gdelt_mentions FINAL GROUP BY GLOBALEVENTID "
        "HAVING max(MentionTimeDate) != '' AND max(MentionTimeDate) < %(cutoff)s",
        {"cutoff": cutoff},
    )
    expired = {int(r[0]) for r in a_rows}

    d_rows = client.execute(
        "SELECT GLOBALEVENTID FROM gdelt_events FINAL GROUP BY GLOBALEVENTID "
        "HAVING max(toString(DATEADDED)) < %(cutoff)s",
        {"cutoff": cutoff},
    )
    old_by_event_date = [int(r[0]) for r in d_rows]

    # Only the ids in D need the fallback test, so this is bounded by the old
    # tail rather than the whole table — and chunked, because it is a literal IN.
    has_usable_time: set[int] = set()
    for batch in _chunked(old_by_event_date):
        rows = client.execute(
            "SELECT GLOBALEVENTID FROM gdelt_mentions FINAL "
            "WHERE GLOBALEVENTID IN %(ids)s GROUP BY GLOBALEVENTID "
            "HAVING max(MentionTimeDate) != ''",
            {"ids": batch},
        )
        has_usable_time.update(int(r[0]) for r in rows)

    expired.update(e for e in old_by_event_date if e not in has_usable_time)
    return sorted(expired)


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
        batches = 0
        for batch in _chunked(event_ids):
            client.execute(
                f"ALTER TABLE {table} ON CLUSTER {CLICKHOUSE_CLUSTER} "
                f"DELETE WHERE {column} IN %(ids)s SETTINGS mutations_sync = 2",
                {"ids": batch},
            )
            batches += 1
        logger.info("silver: deleted rows for %d expired events from %s (%d batch%s)",
                    len(event_ids), table, batches, "" if batches == 1 else "es")


def delete_from_gold(event_ids: list[int]) -> tuple[int, int]:
    """
    Remove the expired events' articles from the gold store.

    user_articles FIRST, then articles: the reverse order would briefly leave
    user_articles rows pointing at articles that no longer exist, which is
    exactly the dangling state the serving join assumes cannot happen.

    Both statements take the whole id set as a single array parameter. Oracle
    needed the set split into 900-entry chunks because its IN list caps at 1000;
    PostgreSQL has no such limit, so the chunking loop is gone. The ClickHouse
    deletes above are still batched, for an unrelated reason: there the ids are
    substituted into the SQL text and bounded by max_query_size.
    """
    import postgres_writer

    ids = [str(e) for e in event_ids]
    with postgres_writer._connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM user_articles WHERE doc_id IN "
            "(SELECT doc_id FROM articles WHERE global_event_id = ANY(%(ids)s))",
            {"ids": ids})
        removed_links = cur.rowcount

        cur.execute(
            "DELETE FROM articles WHERE global_event_id = ANY(%(ids)s)", {"ids": ids})
        removed_articles = cur.rowcount
        conn.commit()

    logger.info("gold: deleted %d articles and %d user_articles links",
                removed_articles, removed_links)
    return removed_articles, removed_links


def delete_tags(event_ids: list[int]) -> int:
    """
    Drop every user's tag pointing at an expired event.

    Without this a triaged card becomes a permanent dead reference: the tag stays
    in MongoDB, the article is gone from PostgreSQL, and the triage page silently
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

def purge_dead_letter_files(now: datetime | None = None) -> int:
    """Delete stale dead-lettered files and their matching CSV rows, but never the CSV log itself."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=DEAD_LETTER_RETENTION_DAYS)
    if not DEAD_LETTER_DIR.exists():
        return 0

    if not DEAD_LETTER_LOG_FILE.exists():
        return 0

    try:
        with DEAD_LETTER_LOG_FILE.open("r", newline="", encoding="utf-8") as fh:
            rows = list(csv.reader(fh))
    except OSError:
        logger.warning("dead_letter: could not read %s; skipping stale purge", DEAD_LETTER_LOG_FILE)
        return 0

    if not rows:
        return 0
    header = rows[0]
    if header != ["file_name", "dead_letter_day", "dead_letter_time"]:
        return 0

    kept_rows = [header]
    removed = 0
    for row in rows[1:]:
        if len(row) < 3:
            continue
        file_name = row[0].strip()
        if not file_name or file_name == DEAD_LETTER_LOG_FILE.name:
            continue
        try:
            stamped = datetime.strptime(f"{row[1].strip()} {row[2].strip()}", "%Y-%m-%d %H:%M:%S")
        except ValueError:
            kept_rows.append(row)
            continue
        if stamped.tzinfo is None:
            stamped = stamped.replace(tzinfo=timezone.utc)

        if stamped < cutoff:
            stale_deleted = False
            for match in DEAD_LETTER_DIR.rglob(file_name):
                if match.is_file() and match != DEAD_LETTER_LOG_FILE:
                    try:
                        match.unlink()
                        stale_deleted = True
                        removed += 1
                    except OSError as exc:
                        logger.warning("dead_letter: could not delete stale file %s: %s", match, exc)
            if stale_deleted:
                continue

        kept_rows.append(row)

    if len(kept_rows) != len(rows):
        with DEAD_LETTER_LOG_FILE.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerows(kept_rows)

    if removed:
        logger.info("dead_letter: removed %d stale file(s) and their log row(s) older than %s",
                    removed, cutoff.date())
    else:
        logger.info("dead_letter: nothing older than %s — no deletions", cutoff.date())
    return removed


def run_once(ch_factory, now: datetime | None = None) -> dict:
    """Find expired events and remove them from silver, gold, and stale dead-letter files."""
    now = now or datetime.now(timezone.utc)
    cutoff_dt = now - timedelta(days=RETENTION_DAYS)
    cutoff = cutoff_dt.strftime("%Y%m%d%H%M%S")

    with ch_factory() as ch:
        expired = find_expired_event_ids(ch, cutoff)
        if not expired:
            logger.info("retention: nothing older than %s (%.0f days) — no deletions",
                        cutoff_dt.date(), RETENTION_DAYS)
            dead_letter_removed = purge_dead_letter_files(now)
            return {"cutoff": cutoff, "events": 0, "articles": 0, "tags": 0,
                    "dead_letters_removed": dead_letter_removed}

        logger.info("retention: %d events have had no article since %s; removing",
                    len(expired), cutoff_dt.date())
        delete_from_silver(ch, expired)

    articles, _ = delete_from_gold(expired)
    tags = delete_tags(expired)
    dead_letter_removed = purge_dead_letter_files(now)
    return {"cutoff": cutoff, "events": len(expired), "articles": articles,
            "tags": tags, "dead_letters_removed": dead_letter_removed}


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
    logger.info("Retention job started (%.0f-day cutoff, checked every %ds)",
                RETENTION_DAYS, TICK_SECONDS)
