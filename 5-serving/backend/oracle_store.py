"""
oracle_store.py
===============
All Oracle read access for the serving layer.

The processing layer writes to Oracle; the serving layer only reads.
Connection is configured via environment variables:
    ORACLE_HOST      — e.g. "oracle-db"
    ORACLE_PORT      — default 1521
    ORACLE_SERVICE   — Oracle service (PDB) name, e.g. "FREEPDB1"
    ORACLE_USER      — DB user
    ORACLE_PASSWORD  — DB password
    ORACLE_TIMEOUT   — query timeout in seconds (default 10)
    ORACLE_RETRIES   — number of retry attempts on transient errors (default 3)

Oracle schema (created by oracle-init/01_schema.sql, written by processing, read
here). doc_id = SHA-256(document_identifier), a fixed 32-byte key: the URL itself
is too long to index as a primary key (ORA-01450). The URL is still carried as
ordinary data, and the user_articles -> articles join runs on doc_id:
-------------------------------------------------------
TABLE articles (
    doc_id               RAW(32) PRIMARY KEY,
    document_identifier  VARCHAR2(2000),
    mention_identifier   VARCHAR2(2000),
    global_event_id      VARCHAR2(50),
    in_raw_text          NUMBER(1),
    confidence           NUMBER(3),
    mention_doc_tone     FLOAT,
    country              VARCHAR2(200),
    risk_category        VARCHAR2(500),
    goldstein            FLOAT,
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
    doc_id               RAW(32),
    PRIMARY KEY (user_id, doc_id)
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
_SERVICE  = os.getenv("ORACLE_SERVICE", "FREEPDB1")
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
        a.cameo_code,
        a.cameo_label,
        a.actor,
        a.latitude,
        a.longitude,
        a.event_date,
        a.age_days
    FROM user_articles ua
    JOIN articles a ON ua.doc_id = a.doc_id
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
        a.cameo_code,
        a.cameo_label,
        a.actor,
        a.latitude,
        a.longitude,
        a.event_date,
        a.age_days
    FROM user_articles ua
    JOIN articles a ON ua.doc_id = a.doc_id
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


def get_events_by_ids(global_event_ids: list[str]) -> list[dict]:
    """
    Event cards for the given GLOBALEVENTIDs, read straight from `articles`.

    Deliberately does NOT join user_articles. A user's triaged events (needs
    action / monitoring / archive) must survive changes to their perimeter: when
    a territory is removed, processing rewrites user_articles and those documents
    disappear from it — but the article rows themselves are only ever upserted,
    never deleted, so the triaged cards remain readable here.

    Returns [] on Oracle error.
    """
    ids = [str(i) for i in global_event_ids if str(i).strip()]
    if not ids:
        return []
    # Oracle has no list binding: generate one placeholder per id.
    binds = {f"id{n}": v for n, v in enumerate(ids)}
    placeholders = ", ".join(f":{k}" for k in binds)
    sql = f"""
        SELECT
            a.global_event_id, a.document_identifier, a.mention_identifier,
            a.in_raw_text, a.confidence, a.mention_doc_tone, a.country,
            a.risk_category, a.goldstein, a.cameo_code, a.cameo_label,
            a.actor, a.latitude, a.longitude, a.event_date, a.age_days
        FROM articles a
        WHERE a.global_event_id IN ({placeholders})
        ORDER BY a.global_event_id, a.confidence DESC, ABS(a.mention_doc_tone) ASC
    """
    try:
        rows = _fetch_rows(sql, **binds)
    except Exception as e:
        logger.error("get_events_by_ids failed: %s", e)
        return []

    groups: dict[str, list[dict]] = {}
    for row in rows:
        eid = str(row["global_event_id"])
        row["url"] = row["document_identifier"]
        groups.setdefault(eid, []).append(row)
    return [_build_event_card(eid, arts) for eid, arts in groups.items()]


def get_events_version(user_id: str) -> str | None:
    """
    An order-independent fingerprint of a user's gold set: it changes if and only
    if the set of doc_ids in user_articles for this user changes. COUNT + SUM of a
    per-row hash, index-backed on user_id, so it's cheap to poll. Returns None on
    Oracle error — the dashboard treats None as "unknown" and skips the compare.
    """
    sql = ("SELECT COUNT(*), NVL(SUM(ORA_HASH(doc_id)), 0) "
           "FROM user_articles WHERE user_id = :user_id")
    try:
        def _run():
            with _connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, user_id=user_id)
                    return cur.fetchone()
        row = _with_retry(_run)
        return f"{row[0]}:{row[1]}" if row else "0:0"
    except Exception as e:
        logger.error("get_events_version failed for %s: %s", user_id, e)
        return None


def get_pipeline_status() -> dict:
    """
    Read the pipeline status from Oracle.

    Two distinct failure modes are surfaced separately to the frontend:
      - The processing pipeline itself reports status=ERROR (data in Oracle
        is known to be stale). This is the normal "technical difficulties"
        banner with the last known timestamp.
      - The backend cannot reach Oracle at all after exhausting retries.
        This is a different, more urgent situation (503-ORACLE) — we don't
        know if the data is stale, we simply can't read anything right now.

    On connection failure, returns an explicit error payload (does not
    silently fall back to "OK") so the frontend can tell the two apart.
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
        # Table reachable but empty — processing hasn't run yet, not an error.
        return {"status": "OK", "timestamp_of_last_update": None}
    except Exception as e:
        logger.error("get_pipeline_status failed (Oracle unreachable): %s", e)
        return {
            "status": "ERROR",
            "timestamp_of_last_update": None,
            "code": "503-ORACLE",
            "message": (
                "The backend could not reach the Oracle database after "
                "multiple attempts. Event data may be temporarily unavailable."
            ),
        }