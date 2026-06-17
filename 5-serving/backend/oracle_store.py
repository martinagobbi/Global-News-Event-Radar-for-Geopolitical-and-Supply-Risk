"""
oracle_store.py
===============
All Oracle read access for the serving layer.

The processing layer writes to Oracle; the serving layer only reads.
Connection is configured via environment variables:
    ORACLE_HOST      — e.g. "oracle-db"
    ORACLE_PORT      — default 1521
    ORACLE_SERVICE   — Oracle service name, e.g. "GDELT"
    ORACLE_USER      — DB user
    ORACLE_PASSWORD  — DB password
    ORACLE_TIMEOUT   — query timeout in seconds (default 10)
    ORACLE_RETRIES   — number of retry attempts on transient errors (default 3)

Oracle schema (written by processing, read here):
-------------------------------------------------------
TABLE articles (
    document_identifier  VARCHAR2(2000) PRIMARY KEY,
    mention_identifier   VARCHAR2(2000),
    global_event_id      VARCHAR2(50),
    in_raw_text          NUMBER(1),
    confidence           NUMBER(3),
    mention_doc_tone     FLOAT,
    country              VARCHAR2(200),
    risk_category        VARCHAR2(500),
    goldstein            FLOAT,
    risk_score           NUMBER(3),
    cameo_code           VARCHAR2(10),
    cameo_label          VARCHAR2(200),
    actor                VARCHAR2(500),
    latitude             FLOAT,
    longitude            FLOAT,
    event_date           DATE,
    age_days             NUMBER(4)
)

TABLE user_articles (
    user_id              VARCHAR2(200),
    document_identifier  VARCHAR2(2000),
    PRIMARY KEY (user_id, document_identifier)
)

TABLE pipeline_status (
    status                   VARCHAR2(10),
    timestamp_of_last_update TIMESTAMP
)
-------------------------------------------------------
"""

from __future__ import annotations

import logging
import time
import os

import oracledb


logger = logging.getLogger(__name__)

_HOST     = os.getenv("ORACLE_HOST", "localhost")
_PORT     = int(os.getenv("ORACLE_PORT", "1521"))
_SERVICE  = os.getenv("ORACLE_SERVICE", "GDELT")
_USER     = os.getenv("ORACLE_USER", "radar")
_PASSWORD = os.getenv("ORACLE_PASSWORD", "radar")
_TIMEOUT  = int(os.getenv("ORACLE_TIMEOUT", "10"))
_RETRIES  = int(os.getenv("ORACLE_RETRIES", "3"))

_DSN = f"{_HOST}:{_PORT}/{_SERVICE}"

# Oracle error codes that are worth retrying (transient network/resource errors)
_RETRYABLE_CODES = {
    12170,  # TNS: connect timeout
    12541,  # TNS: no listener
    12543,  # TNS: destination host unreachable
    12571,  # TNS: packet writer failure
    3113,   # end-of-file on communication channel
    3114,   # not connected to Oracle
    1033,   # Oracle initialization or shutdown in progress
    1089,   # immediate shutdown in progress
}


# ── Connection ─────────────────────────────────────────────────────────────

def _connect():
    return oracledb.connect(
        user=_USER,
        password=_PASSWORD,
        dsn=_DSN,
        tcp_connect_timeout=_TIMEOUT,
    )


# ── Retry helper ───────────────────────────────────────────────────────────

def _with_retry(fn, retries: int = _RETRIES, backoff: float = 1.0):
    """
    Run fn() inside a retry loop with exponential backoff.
    Only retries on transient Oracle errors (connection loss, timeout, etc.).
    Re-raises immediately on permanent errors (bad SQL, wrong credentials).
    After all retries are exhausted, re-raises the last exception so callers
    can decide on a fallback (typically returning [] or {}).
    """
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return fn()
        except oracledb.DatabaseError as e:
            error_obj, = e.args
            code = getattr(error_obj, "code", None)
            if code not in _RETRYABLE_CODES:
                raise   # permanent error — don't retry
            last_exc = e
            wait = backoff * (2 ** attempt)
            logger.warning(
                "Oracle transient error ORA-%05d (attempt %d/%d), retrying in %.1fs: %s",
                code, attempt + 1, retries, wait, error_obj.message,
            )
            time.sleep(wait)
        except Exception as e:
            # Non-Oracle exception (e.g. network socket error before DB responds)
            last_exc = e
            wait = backoff * (2 ** attempt)
            logger.warning(
                "Oracle connection error (attempt %d/%d), retrying in %.1fs: %s",
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


def _sort_and_cap(articles: list[dict], limit: int = 20) -> list[dict]:
    """Confidence DESC, abs(MentionDocTone) ASC, capped at limit."""
    articles.sort(key=lambda a: (-a["confidence"], abs(a["mention_doc_tone"])))
    return articles[:limit]


def _build_event_card(global_event_id: str, raw_articles: list[dict]) -> dict:
    filtered, inrawtext_filtered = _apply_inrawtext_filter(raw_articles)
    articles = _sort_and_cap(filtered)

    title   = articles[0]["mention_identifier"] if articles else f"Event {global_event_id}"
    top_url = articles[0]["url"] if articles else None
    meta    = raw_articles[0]

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
        "risk_score":         meta.get("risk_score"),
        "event_date":         str(meta.get("event_date", "")),
        "age_days":           meta.get("age_days"),
        "top_article_url":    top_url,
        "inrawtext_filtered": inrawtext_filtered,
        "articles":           articles,
    }


def _fetch_rows(sql: str, **params) -> list[dict]:
    """Execute a SELECT and return rows as list of dicts. Retries on transient errors."""
    def _run():
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, **params)
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
        a.risk_score,
        a.cameo_code,
        a.cameo_label,
        a.actor,
        a.latitude,
        a.longitude,
        a.event_date,
        a.age_days
    FROM user_articles ua
    JOIN articles a ON ua.document_identifier = a.document_identifier
    WHERE ua.user_id = :user_id
      AND a.age_days <= :max_age_days
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
        a.risk_score,
        a.cameo_code,
        a.cameo_label,
        a.actor,
        a.latitude,
        a.longitude,
        a.event_date,
        a.age_days
    FROM user_articles ua
    JOIN articles a ON ua.document_identifier = a.document_identifier
    WHERE ua.user_id = :user_id
      AND a.global_event_id = :global_event_id
"""


def get_events_for_user(user_id: str, max_age_days: int = 90) -> list[dict]:
    """
    Return all event cards for a user.
    Applies InRawText filter, sorts by Confidence/Tone, caps at 20 articles per event.
    Returns [] on Oracle error (dashboard shows "no events" rather than crashing).
    """
    try:
        rows = _fetch_rows(_EVENTS_SQL, user_id=user_id, max_age_days=max_age_days)
    except Exception as e:
        logger.error("get_events_for_user failed for %s: %s", user_id, e)
        return []

    groups: dict[str, list[dict]] = {}
    for row in rows:
        eid = str(row["global_event_id"])
        row["url"] = row["document_identifier"]
        groups.setdefault(eid, []).append(row)

    return [_build_event_card(eid, arts) for eid, arts in groups.items()]


def get_event_articles(user_id: str, global_event_id: str) -> dict:
    """
    Return a single event card with all its articles.
    Returns {} on Oracle error or if the event is not found.
    """
    try:
        rows = _fetch_rows(
            _SINGLE_EVENT_SQL,
            user_id=user_id,
            global_event_id=global_event_id,
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


def get_pipeline_status() -> dict:
    """
    Read the pipeline status from Oracle.
    Returns {"status": "OK", "timestamp_of_last_update": None} on any error
    so the dashboard doesn't show a spurious error banner when Oracle is
    temporarily unreachable (the banner would be misleading — we don't
    actually know whether the data is stale or not).
    """
    sql = "SELECT status, timestamp_of_last_update FROM pipeline_status FETCH FIRST 1 ROWS ONLY"
    try:
        def _run():
            with _connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    return cur.fetchone()
        row = _with_retry(_run)
        if row:
            return {
                "status": row[0],
                "timestamp_of_last_update": str(row[1]) if row[1] else None,
            }
    except Exception as e:
        logger.error("get_pipeline_status failed: %s", e)

    return {"status": "OK", "timestamp_of_last_update": None}