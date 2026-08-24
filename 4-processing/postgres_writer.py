"""
4-processing/postgres_writer.py
-------------------------------

Writes the gold layer into PostgreSQL — the store the serving layer reads.
Connection defaults match 5-serving/backend/postgres_store.py. The three tables
(created once by postgres-init/01_schema.sql):

    articles(doc_id BYTEA PK, document_identifier, mention_identifier,
             global_event_id, in_raw_text, confidence, mention_doc_tone, country,
             risk_category, goldstein, cameo_code, cameo_label, mention_source_name,
             latitude, longitude, event_date, date_added, age_days, mention_time)
    user_articles(user_id, doc_id)   PK (user_id, doc_id)
    pipeline_status(status, timestamp_of_last_update)

doc_id = SHA-256(document_identifier): a fixed 32-byte key, because the URL is far
too long to index as a primary key (see _doc_id). Everything upstream of this
module still speaks URLs; the hashing happens only here, at the store boundary.

Articles are UPSERTed (INSERT ... ON CONFLICT on doc_id). A user's rows are
replaced (delete-then-insert). pipeline_status is replaced with a single row.

The connection is described by a single POSTGRES_DSN, which in intended mode
lists all three cluster members with target_session_attrs=read-write, so libpq
finds whichever node Patroni has made leader. Failover therefore needs no
configuration change here; in testing mode the same variable names one host.
"""

import hashlib
import logging
import os
from datetime import datetime, timezone

import psycopg

from contextlib import contextmanager # For the advisory lock to prevent collision

logger = logging.getLogger("processing.postgres")

_HOST     = os.getenv("POSTGRES_HOST", "pipeline_postgres")
_PORT     = os.getenv("POSTGRES_PORT", "5432")
_DB       = os.getenv("POSTGRES_DB", "radar")
_USER     = os.getenv("POSTGRES_USER", "radar")
_PASSWORD = os.getenv("POSTGRES_PASSWORD", "radar")
# A full conninfo wins when given (that is how intended mode passes the member
# list); otherwise one is assembled from the parts, which is what a single-node
# testing deployment needs.
_DSN = os.getenv("POSTGRES_DSN") or \
    f"postgresql://{_USER}:{_PASSWORD}@{_HOST}:{_PORT}/{_DB}"


def _connect():
    return psycopg.connect(_DSN)


def _doc_id(document_identifier: str) -> bytes:
    """
    The `articles` primary key: SHA-256 of the article URL, as 32 raw bytes.

    document_identifier is the URL (VARCHAR(2000)). Using it directly as the PK
    can exceed PostgreSQL's btree index-entry limit of roughly 2704 bytes — 2000
    characters of UTF-8 can reach 8000 — and the INSERT would be rejected with
    "index row size ... exceeds btree version 4 maximum". A digest is a fixed 32
    bytes whatever the URL's length, and is deterministic, so it is a safe key.
    The full URL is still stored beside it as ordinary data, and
    5-serving/backend/postgres_store.py joins user_articles -> articles on doc_id.
    """
    return hashlib.sha256(document_identifier.encode("utf-8")).digest()


# The updatable columns, named once. Oracle's MERGE had to list every column
# twice — once in the UPDATE clause and once in the INSERT — whereas ON CONFLICT
# can refer to the row that failed to insert as EXCLUDED, so the two lists cannot
# drift apart.
_ARTICLE_COLUMNS = [
    "doc_id", "document_identifier", "mention_identifier", "global_event_id",
    "in_raw_text", "confidence", "mention_doc_tone", "country", "risk_category",
    "goldstein", "cameo_code", "cameo_label", "mention_source_name", "latitude", "longitude",
    "event_date", "date_added", "age_days", "mention_time",
]

_UPSERT_ARTICLES = (
    "INSERT INTO articles ({cols}) VALUES ({vals}) "
    "ON CONFLICT (doc_id, global_event_id) DO UPDATE SET {sets}"
).format(
    cols=", ".join(_ARTICLE_COLUMNS),
    vals=", ".join(f"%({c})s" for c in _ARTICLE_COLUMNS),
    # The conflict target is the whole key, so neither part is reassigned.
    sets=", ".join(f"{c} = EXCLUDED.{c}" for c in _ARTICLE_COLUMNS
                   if c not in ("doc_id", "global_event_id")),
)


# Columns added after the original schema shipped. postgres-init/01_schema.sql
# runs ONCE, at first database creation, so an installation whose volume predates
# a column would never get it — and the project has no migration mechanism. Each
# entry is applied idempotently before the first write.
_ADDED_COLUMNS = [
    # (column, DDL type) — mention_time is the article's own MentionTimeDate from
    # silver, which the card ordering needs; event_date is per-EVENT and so is the
    # same for every article on a card.
    ("mention_time", "TIMESTAMP"),
    ("date_added", "TIMESTAMP"),
    # Added when `actor` was dropped: the article publisher, shown on the card.
    ("mention_source_name", "VARCHAR(500)"),
]

# Same idea, for pipeline_status. postgres-init/01_schema.sql declares this column
# too, but that file runs ONLY at first database initialisation, so a volume
# created before the column existed would never gain it.
_ADDED_STATUS_COLUMNS = [
    ("silver_watermark", "VARCHAR(14)"),
]


# ── Type WIDENING for databases created before gold was aligned to silver ────
# postgres-init/01_schema.sql declares these types, but it runs ONLY at first
# database initialisation, so an existing volume keeps whatever it was created
# with — and the old types were STRICTER than silver:
#
#   SMALLINT   16-bit, against silver's Nullable(Int32). `age_days` is the live
#              risk: datediff(today, event_date) overflows it for an event dated
#              far enough in the past. Measured: 99999 -> "smallint out of range".
#   VARCHAR(n) a length cap, against silver's unbounded String. VARCHAR(50) on
#              global_event_id in particular constrained an identifier this
#              pipeline promises never to interpret.
#
# Every conversion here WIDENS, so it cannot fail on existing data and needs no
# USING clause. Re-running is free: PostgreSQL makes ALTER ... TYPE a no-op when
# the column already has the target type. TEXT and VARCHAR share one storage
# representation, so the text conversions do not even rewrite the table.
_WIDENED_COLUMNS = [
    ("articles", "document_identifier", "TEXT"),
    ("articles", "mention_identifier",  "TEXT"),
    ("articles", "global_event_id",     "TEXT"),
    ("articles", "country",             "TEXT"),
    ("articles", "risk_category",       "TEXT"),
    ("articles", "cameo_code",          "TEXT"),
    ("articles", "cameo_label",         "TEXT"),
    ("articles", "mention_source_name", "TEXT"),
    ("articles", "in_raw_text",         "INTEGER"),
    ("articles", "confidence",          "INTEGER"),
    ("articles", "age_days",            "INTEGER"),
    ("user_articles", "user_id",         "TEXT"),
    ("user_articles", "global_event_id", "TEXT"),
]

_schema_checked = False

# Distinguishes "leave the stored watermark alone" from "set it to NULL". Public
# because callers pass it explicitly when they tried to read the watermark and
# failed — see main.recompute_all(). See write_pipeline_status().
KEEP = object()


def ensure_schema() -> None:
    """
    Add any post-launch columns that this database is missing.

    Idempotent by construction: PostgreSQL supports ADD COLUMN IF NOT EXISTS, so
    unlike the Oracle version this needs no exception handling to recognise an
    already-current database. Runs lazily before the first write rather than at
    import or container start, so the database does not have to be reachable the
    instant the processing layer boots.
    """
    global _schema_checked
    if _schema_checked:
        return
    try:
        with _connect() as conn:
            cur = conn.cursor()
            for column, ddl_type in _ADDED_COLUMNS:
                cur.execute(
                    f"ALTER TABLE articles ADD COLUMN IF NOT EXISTS {column} {ddl_type}")
            for column, ddl_type in _ADDED_STATUS_COLUMNS:
                cur.execute(
                    f"ALTER TABLE pipeline_status "
                    f"ADD COLUMN IF NOT EXISTS {column} {ddl_type}")
            # Widen before anything else touches these tables: a publish that
            # arrives with a null or an oversized value must not meet the old,
            # narrower type.
            for table, column, ddl_type in _WIDENED_COLUMNS:
                try:
                    cur.execute(
                        f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {ddl_type}")
                except Exception as exc:      # noqa: BLE001 — column may not exist yet
                    logger.debug("widen %s.%s -> %s skipped: %s",
                                 table, column, ddl_type, exc)
                    conn.rollback()
            _migrate_to_pair_key(cur)
            conn.commit()
        _schema_checked = True
    except Exception as exc:  # noqa: BLE001 — retried on the next write
        logger.warning("Could not verify the articles schema (%s); will retry", exc)


def _migrate_to_pair_key(cur) -> None:
    """
    Rebuild the gold tables if they still carry the OLD, URL-only primary key.

    The key changed from `doc_id` to `(doc_id, global_event_id)` — see
    postgres-init/01_schema.sql for the measurements behind that. A database
    created before the change physically cannot hold the second row of a
    multi-event article, so the tables have to be replaced rather than altered:
    `user_articles` is also gaining a column that is part of its key.

    DROP and CREATE is safe here, and deliberately not a migration script,
    because gold is DERIVED: every row is rebuilt from silver by the next
    recompute, which the caller runs anyway. Nothing a user created lives here —
    profiles and tags are in MongoDB and are untouched.

    Idempotent: once the new key is in place this finds it and does nothing.
    """
    cur.execute("""
        SELECT count(*) FROM information_schema.key_column_usage
        WHERE table_name = 'articles' AND constraint_name = 'pk_articles'
          AND column_name = 'global_event_id'
    """)
    if cur.fetchone()[0]:
        return                                    # already the pair key

    logger.warning("gold tables use the old URL-only key; rebuilding them on "
                   "(doc_id, global_event_id) — they refill from silver on the "
                   "next recompute")
    cur.execute("DROP TABLE IF EXISTS user_articles")
    cur.execute("DROP TABLE IF EXISTS articles")
    cur.execute("""
        CREATE TABLE articles (
          doc_id              BYTEA         NOT NULL,
          document_identifier VARCHAR(2000) NOT NULL,
          mention_identifier  VARCHAR(2000),
          global_event_id     VARCHAR(50)   NOT NULL,
          in_raw_text         SMALLINT,
          confidence          SMALLINT,
          mention_doc_tone    DOUBLE PRECISION,
          country             VARCHAR(200),
          risk_category       VARCHAR(500),
          goldstein           DOUBLE PRECISION,
          cameo_code          VARCHAR(10),
          cameo_label         VARCHAR(200),
          mention_source_name VARCHAR(500),
          latitude            DOUBLE PRECISION,
          longitude           DOUBLE PRECISION,
          mention_time        TIMESTAMP,
          date_added          TIMESTAMP,
          event_date          TIMESTAMP,
          age_days            SMALLINT,
          CONSTRAINT pk_articles PRIMARY KEY (doc_id, global_event_id))
    """)
    cur.execute("CREATE INDEX ix_articles_event ON articles (global_event_id)")
    cur.execute("""
        CREATE TABLE user_articles (
          user_id         VARCHAR(200) NOT NULL,
          doc_id          BYTEA        NOT NULL,
          global_event_id VARCHAR(50)  NOT NULL,
          CONSTRAINT pk_user_articles PRIMARY KEY (user_id, doc_id, global_event_id))
    """)
    logger.warning("gold tables rebuilt on the pair key")


def _naive_utc(ts: datetime) -> datetime:
    """
    Normalise to a naive UTC datetime for a TIMESTAMP (without time zone) column.

    An aware datetime handed to such a column is converted using the session's
    TimeZone and the offset then discarded, so the stored value would depend on
    the server's configuration. Converting here makes it UTC regardless. The
    datetimes built from GDELT timestamps upstream are already naive and pass
    through untouched.
    """
    return ts.astimezone(timezone.utc).replace(tzinfo=None) if ts.tzinfo else ts


def write_articles(rows: list[dict]) -> int:
    """Upsert article rows into the `articles` table (keyed on doc_id)."""
    if not rows:
        return 0
    ensure_schema()
    # Derive the 32-byte key from the URL; callers only ever pass the URL.
    rows = [{**r, "doc_id": _doc_id(r["document_identifier"])} for r in rows]
    with _connect() as conn:
        cur = conn.cursor()
        # No setinputsizes equivalent is needed: psycopg adapts bytes to BYTEA and
        # None to NULL, and PostgreSQL infers each parameter's type from the
        # target column rather than from the first row's values.
        cur.executemany(_UPSERT_ARTICLES, rows)
        affected = cur.rowcount
        conn.commit()
    if affected != len(rows):
        logger.warning("articles: sent %d rows but %d were affected", len(rows), affected)
    logger.info("Upserted %d rows into articles (%d affected)", len(rows), affected)
    return affected


def write_user_articles(user_id: str, pairs: list[tuple[str, str]]) -> int:
    """
    Replace a user's rows in user_articles with the given (url, event) pairs.

    Takes PAIRS, not URLs. One article URL is routinely a mention of several
    events — measured on the seed, 51.8% of URLs are — and the user receives it
    once per event, because each one is a different card. De-duplicating on the
    URL alone would keep one arbitrary event and silently drop the rest.
    """
    # De-duplicate on the whole key, which is what the (user_id, doc_id,
    # global_event_id) primary key requires: a re-ingested slice can repeat the
    # same pair, but two different events sharing a URL are NOT duplicates.
    # Order-preserving.
    pairs = list(dict.fromkeys((str(u), str(e)) for u, e in pairs))
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM user_articles WHERE user_id = %(u)s", {"u": user_id})
        inserted = 0
        if pairs:
            cur.executemany(
                "INSERT INTO user_articles (user_id, doc_id, global_event_id) "
                "VALUES (%(u)s, %(d)s, %(e)s)",
                [{"u": user_id, "d": _doc_id(u), "e": e} for u, e in pairs],
            )
            inserted = cur.rowcount
        conn.commit()
    if inserted != len(pairs):
        logger.warning("user_articles for %s: sent %d but %d were inserted",
                       user_id, len(pairs), inserted)
    logger.info("Wrote %d user_articles for user %s", inserted, user_id)
    return inserted


def delete_orphan_articles(protected_event_ids=()) -> int:
    """
    Remove rows from `articles` that no `user_articles` row references AND whose
    event nobody is actively tracking.

    `articles` is written as an upsert and never deleted from, so it only ever
    grows. `user_articles`, by contrast, is rebuilt per user, so an article stops
    being referenced the moment a user narrows their preferences or the silver it
    came from is trimmed away. Those rows are unreachable — serving joins
    user_articles -> articles — but they accumulate indefinitely.

    `protected_event_ids` is what stops that cleanup eating tracked cards. The
    serving layer reads needs-action and monitoring events straight from
    `articles`, without joining `user_articles`, precisely so a card the user has
    committed to following survives them dropping the territory that first brought
    it in. Such a row IS unreferenced, and without this guard the sweep would
    delete it — the tag would survive in MongoDB pointing at an article that no
    longer exists.

    ARCHIVED events are deliberately NOT passed in (see
    mongo_reader.PROTECTED_TAGS). Archiving means the event does not matter, so
    once it also stops matching the user's preferences the row is swept like any
    other orphan and the card leaves the Archive page. The tag is left in place,
    so re-adding the territory later brings the entry back.

    Deleting the rest is precise and destroys nothing else: rows still referenced
    by ANY user are kept by the NOT EXISTS, `user_articles` is untouched, and so
    are MongoDB and the silver layer. This is why the cleanup does not require
    recreating the gold volume.

    NOT EXISTS rather than NOT IN: an anti-join is what the planner optimises for
    here, and NOT IN would return nothing at all if any doc_id were ever NULL.
    """
    ids = [str(e) for e in protected_event_ids if str(e).strip()]
    sql = (
        "DELETE FROM articles a WHERE NOT EXISTS "
        "(SELECT 1 FROM user_articles ua WHERE ua.doc_id = a.doc_id "
        "   AND ua.global_event_id = a.global_event_id)"
    )
    params: dict = {}
    if ids:
        # PostgreSQL binds a whole list as one array parameter, so the OR-ed
        # 900-entry chunks the Oracle version needed (its IN list caps at 1000)
        # are unnecessary: = ANY takes the entire set in a single parameter.
        sql += " AND NOT (a.global_event_id = ANY(%(ids)s))"
        params["ids"] = ids

    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        removed = cur.rowcount
        conn.commit()
    if removed:
        logger.info("Removed %d orphaned rows from articles "
                    "(%d tracked events protected)", removed, len(ids))
    return removed


def mark_pipeline_stale() -> None:
    """
    Flag the gold as stale WITHOUT moving timestamp_of_last_update.

    write_pipeline_status() defaults that column to now, which is right when a
    recompute has just happened and wrong here: the whole point of this call is
    that nothing has been refreshed, so advancing the timestamp would make stale
    data look freshly built to the serving tier.
    """
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE pipeline_status SET status = 'ERROR'")
        conn.commit()
    logger.warning("Marked pipeline_status=ERROR (silver stopped advancing)")


def read_silver_watermark() -> str | None:
    """
    The slice id the gold layer was last built from, or None if never recorded.

    This is the BASELINE an incremental recompute adds to: it answers "what was
    already accounted for", so the next run only has to consider mentions newer
    than it. Distinct from the live ClickHouse watermark, which answers "what is
    in silver now" — the gap between the two is exactly the work to do.
    """
    try:
        with _connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT silver_watermark FROM pipeline_status LIMIT 1")
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    except Exception as exc:  # noqa: BLE001 — caller falls back to a full run
        logger.warning("could not read the stored silver watermark: %s", exc)
        return None


def write_pipeline_status(status: str, ts: datetime | None = None,
                          watermark: str | None | object = KEEP) -> None:
    """
    Replace pipeline_status with a single (status, timestamp, watermark) row.

    `watermark` defaults to the KEEP sentinel, meaning "carry the stored value
    forward". That is not the same as None, and the distinction matters: this
    table holds one row and is rewritten rather than updated, so a plain None
    default would silently erase the watermark on every status write made by a
    caller that does not happen to know it — which is most of them.
    """
    with _connect() as conn:
        cur = conn.cursor()
        if watermark is KEEP:
            cur.execute("SELECT silver_watermark FROM pipeline_status LIMIT 1")
            row = cur.fetchone()
            watermark = row[0] if row else None
        cur.execute("DELETE FROM pipeline_status")
        cur.execute(
            "INSERT INTO pipeline_status "
            "(status, timestamp_of_last_update, silver_watermark) "
            "VALUES (%(s)s, %(t)s, %(w)s)",
            {"s": status, "t": _naive_utc(ts or datetime.now(timezone.utc)),
             "w": watermark},
        )
        conn.commit()
    logger.info("Wrote pipeline_status=%s (silver watermark %s) to PostgreSQL",
                status, watermark or "unknown")

@contextmanager
def advisory_lock(lock_id: int = 8273645):
    """
    Acquires an exclusive Postgres advisory lock.
    Forces overlapping recompute tasks to run sequentially to prevent collisions.
    """
    # Open a connection strictly to hold the lock using the existing helper
    with _connect() as conn:
        cur = conn.cursor()
        try:
            # This blocks if another job already holds the lock
            cur.execute("SELECT pg_advisory_lock(%s)", (lock_id,))
            yield
        finally:
            # Safely release the lock once the job completes or crashes
            cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))