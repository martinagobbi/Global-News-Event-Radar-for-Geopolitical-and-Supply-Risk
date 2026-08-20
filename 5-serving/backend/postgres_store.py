"""
postgres_store.py
=================
All PostgreSQL read access for the serving layer.

The processing layer writes to PostgreSQL; the serving layer only reads.
Connection is configured via environment variables:
    POSTGRES_DSN      — full libpq conninfo; wins over the parts below
    POSTGRES_HOST     — e.g. "pipeline_postgres"
    POSTGRES_PORT     — default 5432
    POSTGRES_DB       — database name, e.g. "radar"
    POSTGRES_USER     — DB user
    POSTGRES_PASSWORD — DB password
    POSTGRES_TIMEOUT  — connect and statement timeout in seconds (default 10)
    POSTGRES_RETRIES  — number of retry attempts on transient errors (default 3)

In intended mode POSTGRES_DSN lists all three cluster members with
target_session_attrs=read-write, so libpq itself finds whichever node Patroni has
made leader. Combined with the retry loop below, a failover appears here as a
handful of retried connections rather than an outage.

Schema (created by postgres-init/01_schema.sql, written by processing, read here).
doc_id = SHA-256(document_identifier), a fixed 32-byte key: the URL itself is too
long to index as a primary key (a btree entry must fit in roughly 2704 bytes). The
URL is still carried as ordinary data, and the user_articles -> articles join runs
on doc_id:
-------------------------------------------------------
TABLE articles (
    doc_id               BYTEA,
    document_identifier  VARCHAR(2000),
    mention_identifier   VARCHAR(2000),
    global_event_id      VARCHAR(50),
    in_raw_text          SMALLINT,
    confidence           SMALLINT,
    mention_doc_tone     DOUBLE PRECISION,
    country              VARCHAR(200),
    risk_category        VARCHAR(500),
    goldstein            DOUBLE PRECISION,
    cameo_code           VARCHAR(10),
    cameo_label          VARCHAR(200),
    actor                VARCHAR(500),
    latitude             DOUBLE PRECISION,
    longitude            DOUBLE PRECISION,
    event_date           TIMESTAMP,
    age_days             SMALLINT,
    mention_time         TIMESTAMP, -- the ARTICLE's own timestamp; event_date is
                                    -- per-EVENT and so identical across a card
    PRIMARY KEY (doc_id, global_event_id)   -- one row per (article, event) pair
)

TABLE user_articles (
    user_id              VARCHAR(200),
    doc_id               BYTEA,
    global_event_id      VARCHAR(50),
    PRIMARY KEY (user_id, doc_id, global_event_id)
)

TABLE pipeline_status (
    status                   VARCHAR(10),
    timestamp_of_last_update TIMESTAMP
)
-------------------------------------------------------
"""

from __future__ import annotations

import logging
import time
import os
from datetime import datetime, timezone

import psycopg


logger = logging.getLogger(__name__)

_HOST     = os.getenv("POSTGRES_HOST", "localhost")
_PORT     = os.getenv("POSTGRES_PORT", "5432")
_DB       = os.getenv("POSTGRES_DB", "radar")
_USER     = os.getenv("POSTGRES_USER", "radar")
_PASSWORD = os.getenv("POSTGRES_PASSWORD", "radar")
_TIMEOUT  = int(os.getenv("POSTGRES_TIMEOUT", "10"))
_RETRIES  = int(os.getenv("POSTGRES_RETRIES", "3"))
# How old the gold layer may be before the dashboard calls it stale. GDELT
# releases every 15 minutes, so an hour is four missed releases: comfortably
# beyond normal jitter, and well short of a user not noticing.
PIPELINE_STALE_SECONDS = int(os.getenv("PIPELINE_STALE_SECONDS", str(60 * 60)))

_DSN = os.getenv("POSTGRES_DSN") or \
    f"postgresql://{_USER}:{_PASSWORD}@{_HOST}:{_PORT}/{_DB}"

# SQLSTATEs worth retrying: the connection classes (08xxx) and the operator-
# intervention classes (57Pxx), which are exactly what a Patroni failover looks
# like from a client — the old leader shuts down (57P01), and the promoting node
# briefly refuses connections while it starts up (57P03). Anything else (bad SQL,
# wrong credentials, constraint violations) is permanent and re-raised at once.
_RETRYABLE_SQLSTATES = {
    "08000",  # connection_exception
    "08001",  # sqlclient_unable_to_establish_sqlconnection
    "08003",  # connection_does_not_exist
    "08004",  # sqlserver_rejected_establishment_of_sqlconnection
    "08006",  # connection_failure
    "08007",  # transaction_resolution_unknown
    "57P01",  # admin_shutdown
    "57P02",  # crash_shutdown
    "57P03",  # cannot_connect_now — the server is still starting up
}


# ── Connection ─────────────────────────────────────────────────────────────

def _connect():
    # connect_timeout bounds establishing the connection; statement_timeout bounds
    # each query once connected. The Oracle version could only express the former,
    # so a query that hung after connecting had no ceiling at all.
    return psycopg.connect(
        _DSN,
        connect_timeout=_TIMEOUT,
        options=f"-c statement_timeout={_TIMEOUT * 1000}",
    )


# ── Retry helper ───────────────────────────────────────────────────────────

def _with_retry(fn, retries: int = _RETRIES, backoff: float = 1.0):
    """
    Run fn() inside a retry loop with exponential backoff.
    Only retries on transient PostgreSQL errors (connection loss, failover,
    server still starting). Re-raises immediately on permanent errors (bad SQL,
    wrong credentials). After all retries are exhausted, re-raises the last
    exception so callers can decide on a fallback (typically returning [] or {}).
    """
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return fn()
        except psycopg.Error as e:
            sqlstate = getattr(e, "sqlstate", None)
            # A failed connection attempt often carries no SQLSTATE at all (the
            # server was never reached), and that is precisely the case worth
            # retrying — hence OperationalError counts as transient regardless.
            transient = sqlstate in _RETRYABLE_SQLSTATES or (
                sqlstate is None and isinstance(e, psycopg.OperationalError))
            if not transient:
                raise   # permanent error — don't retry
            last_exc = e
            wait = backoff * (2 ** attempt)
            logger.warning(
                "PostgreSQL transient error %s (attempt %d/%d), retrying in %.1fs: %s",
                sqlstate or "connection", attempt + 1, retries, wait, e,
            )
            time.sleep(wait)
        except Exception as e:
            # Non-database exception (e.g. a socket error before the server replies)
            last_exc = e
            wait = backoff * (2 ** attempt)
            logger.warning(
                "PostgreSQL connection error (attempt %d/%d), retrying in %.1fs: %s",
                attempt + 1, retries, wait, e,
            )
            time.sleep(wait)
    raise last_exc


# ── Article-level logic (pure Python, no DB) ───────────────────────────────

def _apply_inrawtext_filter(articles: list[dict]) -> tuple[list[dict], bool]:
    """
    If at least one article has in_raw_text=1, keep only those.
    Returns (filtered_articles, was_filtered).
    """
    raw = [a for a in articles if a["in_raw_text"] == 1]
    if raw:
        return raw, len(raw) < len(articles)
    return articles, False


def _dedupe_by_title(articles: list[dict]) -> list[dict]:
    """
    Drop repeats of the same headline within an event. Syndicated stories are
    republished under different URLs, so the same title can arrive as several
    distinct articles; a card should list it once. Compared case/space-
    insensitively. Order-preserving, so the sort below still decides ranking.
    (Processing also de-duplicates when building the gold; this covers rows
    already written by an earlier build.)
    """
    seen: set[str] = set()
    out: list[dict] = []
    for a in articles:
        key = " ".join(str(a.get("mention_identifier", "")).lower().split())
        if key and key in seen:
            continue
        seen.add(key)
        out.append(a)
    return out


def _sort_and_cap(articles: list[dict], limit: int = 20) -> list[dict]:
    """Confidence DESC, abs(MentionDocTone) ASC, capped at limit."""
    articles.sort(key=lambda a: (-a["confidence"], abs(a["mention_doc_tone"])))
    return _dedupe_by_title(articles)[:limit]


def _oldest_article_time(articles: list[dict]):
    """
    The timestamp of the earliest article on a card — when the story began.

    Computed over the card's WHOLE article list, not the few shown before it is
    opened, so the ordering does not shift as the preview length changes. Returns
    None when no article has a timestamp: rows written before `mention_time`
    existed have NULL there until the next recompute refills them from silver.
    """
    times = [a["mention_time"] for a in articles if a.get("mention_time")]
    return min(times) if times else None


def _sort_cards_newest_first(cards: list[dict]) -> list[dict]:
    """
    Order cards by the timestamp of their oldest article, most recent first — so
    the story that STARTED most recently leads.

    Cards with no timestamp at all sort last rather than raising on a None
    comparison; among themselves they keep the order the database returned.
    """
    return sorted(
        cards,
        key=lambda c: (c["oldest_article_time"] is not None, c["oldest_article_time"]),
        reverse=True,
    )


def _confidence_value(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def _build_event_card(global_event_id: str, raw_articles: list[dict]) -> dict:
    filtered, inrawtext_filtered = _apply_inrawtext_filter(raw_articles)
    articles = _sort_and_cap(filtered)

    title   = articles[0]["mention_identifier"] if articles else f"Event {global_event_id}"
    top_url = articles[0]["url"] if articles else None
    meta    = raw_articles[0]
    max_confidence = max(
        (_confidence_value(a.get("confidence")) for a in raw_articles),
        default=None,
    )

    return {
        "global_event_id":    global_event_id,
        "card_title":         title,
        "country":            meta.get("country", ""),
        "latitude":           meta.get("latitude"),
        "longitude":          meta.get("longitude"),
        "cameo_code":         meta.get("cameo_code", ""),
        "cameo_label":        meta.get("cameo_label", ""),
        "actor":              meta.get("actor", ""),
        "risk_category":      meta.get("risk_category", ""),
        "goldstein":          meta.get("goldstein"),
        "event_date":         str(meta.get("event_date", "")),
        "date_added":         str(meta.get("date_added", "")),
        "max_confidence":     max_confidence,
        "age_days":           meta.get("age_days"),
        "top_article_url":    top_url,
        "inrawtext_filtered": inrawtext_filtered,
        # When this story began, used to order the cards. Computed over the full
        # article list, not just the ones visible before the card is opened.
        "oldest_article_time": _oldest_article_time(articles),
        "articles":           articles,
    }


def _fetch_rows(sql: str, params: dict | None = None) -> list[dict]:
    """Execute a SELECT and return rows as list of dicts. Retries on transient errors."""
    def _run():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                cols = [d[0].lower() for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    return _with_retry(_run)


# ── Public query functions ─────────────────────────────────────────────────

_EVENTS_SQL = """
    SELECT
        a.global_event_id,
        a.document_identifier,
        a.mention_identifier,
        a.in_raw_text,
        a.confidence,
        a.mention_doc_tone,
        a.country,
        a.risk_category,
        a.goldstein,
        a.cameo_code,
        a.cameo_label,
        a.actor,
        a.latitude,
        a.longitude,
        a.event_date,
        a.date_added,
        a.age_days,
        a.mention_time
    FROM user_articles ua
    -- The WHOLE key. `articles` is keyed on (doc_id, global_event_id) because one
    -- URL is routinely a mention of several events, and each pair is a separate
    -- card. Joining on doc_id alone would fan every such article out across every
    -- event it belongs to, showing it on cards this user never matched.
    JOIN articles a ON ua.doc_id = a.doc_id
                   AND ua.global_event_id = a.global_event_id
    WHERE ua.user_id = %(user_id)s
      AND a.age_days <= %(max_age_days)s
    ORDER BY a.global_event_id, a.confidence DESC, ABS(a.mention_doc_tone) ASC
"""

_SINGLE_EVENT_SQL = """
    SELECT
        a.global_event_id,
        a.document_identifier,
        a.mention_identifier,
        a.in_raw_text,
        a.confidence,
        a.mention_doc_tone,
        a.country,
        a.risk_category,
        a.goldstein,
        a.cameo_code,
        a.cameo_label,
        a.actor,
        a.latitude,
        a.longitude,
        a.event_date,
        a.date_added,
        a.age_days,
        a.mention_time
    FROM user_articles ua
    -- The WHOLE key. `articles` is keyed on (doc_id, global_event_id) because one
    -- URL is routinely a mention of several events, and each pair is a separate
    -- card. Joining on doc_id alone would fan every such article out across every
    -- event it belongs to, showing it on cards this user never matched.
    JOIN articles a ON ua.doc_id = a.doc_id
                   AND ua.global_event_id = a.global_event_id
    WHERE ua.user_id = %(user_id)s
      AND a.global_event_id = %(global_event_id)s
"""


def get_events_for_user(user_id: str, max_age_days: int = 90) -> list[dict]:
    """
    Return all event cards for a user.
    Applies InRawText filter, sorts by Confidence/Tone, caps at 20 articles per event.
    Returns [] on database error (dashboard shows "no events" rather than crashing).
    """
    try:
        rows = _fetch_rows(
            _EVENTS_SQL, {"user_id": user_id, "max_age_days": max_age_days})
    except Exception as e:
        logger.error("get_events_for_user failed for %s: %s", user_id, e)
        return []

    groups: dict[str, list[dict]] = {}
    for row in rows:
        eid = str(row["global_event_id"])
        row["url"] = row["document_identifier"]
        groups.setdefault(eid, []).append(row)

    return _sort_cards_newest_first(
        [_build_event_card(eid, arts) for eid, arts in groups.items()])


def get_event_articles(user_id: str, global_event_id: str) -> dict:
    """
    Return a single event card with all its articles.
    Returns {} on database error or if the event is not found.
    """
    try:
        rows = _fetch_rows(
            _SINGLE_EVENT_SQL,
            {"user_id": user_id, "global_event_id": global_event_id},
        )
    except Exception as e:
        logger.error(
            "get_event_articles failed for user=%s event=%s: %s",
            user_id, global_event_id, e,
        )
        return {}

    if not rows:
        return {}

    for row in rows:
        row["url"] = row["document_identifier"]

    return _build_event_card(global_event_id, rows)


_BY_IDS_SQL = """
    SELECT
        a.global_event_id, a.document_identifier, a.mention_identifier,
        a.in_raw_text, a.confidence, a.mention_doc_tone, a.country,
        a.risk_category, a.goldstein, a.cameo_code, a.cameo_label,
        a.actor, a.latitude, a.longitude, a.event_date, a.date_added, a.age_days,
        a.mention_time
    FROM articles a
    WHERE a.global_event_id = ANY(%(ids)s)
    ORDER BY a.global_event_id, a.confidence DESC, ABS(a.mention_doc_tone) ASC
"""


def get_events_by_ids(global_event_ids: list[str]) -> list[dict]:
    """
    Event cards for the given GLOBALEVENTIDs, read straight from `articles`.

    Deliberately does NOT join user_articles. A user's triaged events (needs
    action / monitoring / archive) must survive changes to their perimeter: when
    a territory is removed, processing rewrites user_articles and those documents
    disappear from it — but the article rows themselves are only ever upserted,
    never deleted, so the triaged cards remain readable here.

    The id set binds as a single array parameter. Oracle had no list binding and
    needed one generated placeholder per id, which also capped the set at 1000.

    Returns [] on database error.
    """
    ids = [str(i) for i in global_event_ids if str(i).strip()]
    if not ids:
        return []
    try:
        rows = _fetch_rows(_BY_IDS_SQL, {"ids": ids})
    except Exception as e:
        logger.error("get_events_by_ids failed: %s", e)
        return []

    groups: dict[str, list[dict]] = {}
    for row in rows:
        eid = str(row["global_event_id"])
        row["url"] = row["document_identifier"]
        groups.setdefault(eid, []).append(row)
    return _sort_cards_newest_first(
        [_build_event_card(eid, arts) for eid, arts in groups.items()])


def get_events_version(user_id: str) -> str | None:
    """
    An order-independent fingerprint of a user's gold set: it changes if and only
    if the set of doc_ids in user_articles for this user changes.

    COUNT plus an md5 over the user's doc_ids concatenated in doc_id order. The
    explicit ORDER BY inside string_agg is what makes it order-independent, and
    the (user_id, doc_id) primary key already returns the rows in that order, so
    no extra sort is performed and this stays cheap enough to poll. (Oracle used
    COUNT + SUM(ORA_HASH(doc_id)); a sum is order-independent for free, but
    ORA_HASH has no PostgreSQL equivalent.)

    Returns None on database error — the dashboard treats None as "unknown" and
    skips the comparison.
    """
    sql = ("SELECT COUNT(*), "
           "COALESCE(md5(string_agg(encode(doc_id, 'hex') || global_event_id, "
           "                        '' ORDER BY doc_id, global_event_id)), '') "
           "FROM user_articles WHERE user_id = %(user_id)s")
    try:
        def _run():
            with _connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, {"user_id": user_id})
                    return cur.fetchone()
        row = _with_retry(_run)
        return f"{row[0]}:{row[1]}" if row else "0:"
    except Exception as e:
        logger.error("get_events_version failed for %s: %s", user_id, e)
        return None


def get_pipeline_status() -> dict:
    """
    Read the pipeline status from PostgreSQL.

    Two distinct failure modes are surfaced separately to the frontend:
      - The processing pipeline itself reports status=ERROR (the gold layer
        is known to be stale). This is the normal "technical difficulties"
        banner with the last known timestamp.
      - The backend cannot reach PostgreSQL at all after exhausting retries.
        This is a different, more urgent situation (503-POSTGRES) — we don't
        know if the data is stale, we simply can't read anything right now.

    On connection failure, returns an explicit error payload (does not
    silently fall back to "OK") so the frontend can tell the two apart.
    """
    sql = ("SELECT status, timestamp_of_last_update, silver_watermark "
           "FROM pipeline_status LIMIT 1")
    try:
        def _run():
            with _connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    return cur.fetchone()
        row = _with_retry(_run)
        if row:
            status, updated, watermark = row[0], row[1], row[2]
            # Freshness is judged here, not taken on trust. The processing layer
            # writes ERROR when it NOTICES silver has stopped advancing, but if
            # that layer is itself down it writes nothing at all, and the last row
            # it wrote says OK for as long as the database keeps answering.
            # Comparing the timestamp to the clock catches the case nobody is
            # left to report. The column is TIMESTAMP without time zone and the
            # writer stores UTC, so it is read back naive and labelled UTC here.
            if status == "OK" and updated is not None:
                age = (datetime.now(timezone.utc)
                       - updated.replace(tzinfo=timezone.utc)).total_seconds()
                if age > PIPELINE_STALE_SECONDS:
                    logger.warning("pipeline_status says OK but the gold layer is "
                                   "%.0f min old", age / 60)
                    return {
                        "status": "ERROR",
                        "timestamp_of_last_update": str(updated),
                        "silver_watermark": watermark,
                        "code": "STALE-PIPELINE",
                        "message": (
                            f"The briefing has not been updated for "
                            f"{age / 3600:.1f} hours. New articles are not "
                            "currently arriving."
                        ),
                    }
            return {
                "status": status,
                "timestamp_of_last_update": str(updated) if updated else None,
                "silver_watermark": watermark,
            }
        # Table reachable but empty — processing hasn't run yet, not an error.
        return {"status": "OK", "timestamp_of_last_update": None,
                "silver_watermark": None}
    except Exception as e:
        logger.error("get_pipeline_status failed (PostgreSQL unreachable): %s", e)
        return {
            "status": "ERROR",
            "timestamp_of_last_update": None,
            "code": "503-POSTGRES",
            "message": (
                "The backend could not reach the PostgreSQL database after "
                "multiple attempts. Event data may be temporarily unavailable."
            ),
        }
