"""
4-processing/oracle_writer.py
-----------------------------

Writes the gold layer into Oracle — the store the serving layer reads. Connection
defaults match 5-serving/backend/oracle_store.py (pipeline_oracle:1521/FREEPDB1,
user radar). The three tables (created once by oracle-init/01_schema.sql):

    articles(doc_id RAW(32) PK, document_identifier, mention_identifier,
             global_event_id, in_raw_text, confidence, mention_doc_tone, country,
             risk_category, goldstein, cameo_code, cameo_label, actor,
             latitude, longitude, event_date, age_days)
    user_articles(user_id, doc_id)   PK (user_id, doc_id)
    pipeline_status(status, timestamp_of_last_update)

doc_id = SHA-256(document_identifier): a fixed 32-byte key, because the URL is far
too long to index as a primary key (see _doc_id). Everything upstream of this
module still speaks URLs; the hashing happens only here, at the Oracle boundary.

Articles are UPSERTed (MERGE on doc_id). A user's rows are replaced
(delete-then-insert). pipeline_status is replaced with a single row.
"""

import hashlib
import logging
import os
from datetime import datetime, timezone

import oracledb

logger = logging.getLogger("processing.oracle")

_HOST     = os.getenv("ORACLE_HOST", "pipeline_oracle")
_PORT     = int(os.getenv("ORACLE_PORT", "1521"))
_SERVICE  = os.getenv("ORACLE_SERVICE", "FREEPDB1")
_USER     = os.getenv("ORACLE_USER", "radar")
_PASSWORD = os.getenv("ORACLE_PASSWORD", "radar")
_DSN = f"{_HOST}:{_PORT}/{_SERVICE}"


def _connect():
    return oracledb.connect(user=_USER, password=_PASSWORD, dsn=_DSN)


def _doc_id(document_identifier: str) -> bytes:
    """
    The `articles` primary key: SHA-256 of the article URL, as 32 raw bytes.

    document_identifier is the URL (VARCHAR2(2000)). Using it directly as the PK
    can exceed Oracle's maximum index key length (~6398 bytes on an 8K block —
    2000 chars of AL32UTF8 can reach 8000) and raise ORA-01450. A digest is a
    fixed 32 bytes whatever the URL's length, and is deterministic, so it is a
    safe key. The full URL is still stored beside it as ordinary data, and
    5-serving/backend/oracle_store.py joins user_articles -> articles on doc_id.
    """
    return hashlib.sha256(document_identifier.encode("utf-8")).digest()


_MERGE_ARTICLES = """
MERGE INTO articles t
USING (SELECT :doc_id AS doc_id FROM dual) s
ON (t.doc_id = s.doc_id)
WHEN MATCHED THEN UPDATE SET
    document_identifier = :document_identifier,
    mention_identifier = :mention_identifier, global_event_id = :global_event_id,
    in_raw_text = :in_raw_text, confidence = :confidence, mention_doc_tone = :mention_doc_tone,
    country = :country, risk_category = :risk_category, goldstein = :goldstein,
    cameo_code = :cameo_code, cameo_label = :cameo_label,
    actor = :actor, latitude = :latitude, longitude = :longitude,
    event_date = :event_date, age_days = :age_days
WHEN NOT MATCHED THEN INSERT
    (doc_id, document_identifier, mention_identifier, global_event_id, in_raw_text,
     confidence, mention_doc_tone, country, risk_category, goldstein, cameo_code,
     cameo_label, actor, latitude, longitude, event_date, age_days)
VALUES
    (:doc_id, :document_identifier, :mention_identifier, :global_event_id, :in_raw_text,
     :confidence, :mention_doc_tone, :country, :risk_category, :goldstein, :cameo_code,
     :cameo_label, :actor, :latitude, :longitude, :event_date, :age_days)
"""


def write_articles(rows: list[dict]) -> int:
    """Upsert article rows into the Oracle `articles` table (keyed on doc_id)."""
    if not rows:
        return 0
    # Derive the 32-byte key from the URL; callers only ever pass the URL.
    rows = [{**r, "doc_id": _doc_id(r["document_identifier"])} for r in rows]
    with _connect() as conn:
        cur = conn.cursor()
        # Declare types explicitly so executemany doesn't mis-infer from a row
        # whose nullable numeric/date columns happen to be NULL.
        cur.setinputsizes(
            doc_id=oracledb.DB_TYPE_RAW,
            mention_doc_tone=oracledb.DB_TYPE_NUMBER,
            goldstein=oracledb.DB_TYPE_NUMBER,
            latitude=oracledb.DB_TYPE_NUMBER,
            longitude=oracledb.DB_TYPE_NUMBER,
            age_days=oracledb.DB_TYPE_NUMBER,
            event_date=oracledb.DB_TYPE_DATE,
        )
        cur.executemany(_MERGE_ARTICLES, rows)
        affected = cur.rowcount
        conn.commit()
    if affected != len(rows):
        logger.warning("Oracle articles: sent %d rows but %d were affected", len(rows), affected)
    logger.info("Upserted %d rows into Oracle articles (%d affected)", len(rows), affected)
    return affected


def write_user_articles(user_id: str, document_identifiers: list[str]) -> int:
    """Replace a user's rows in user_articles with the given document set."""
    # De-duplicate: a URL can be mentioned for several events (and a re-ingested
    # slice can duplicate mentions), which would otherwise violate the
    # (user_id, doc_id) primary key. Order-preserving.
    document_identifiers = list(dict.fromkeys(document_identifiers))
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM user_articles WHERE user_id = :u", u=user_id)
        inserted = 0
        if document_identifiers:
            cur.setinputsizes(d=oracledb.DB_TYPE_RAW)
            cur.executemany(
                "INSERT INTO user_articles (user_id, doc_id) VALUES (:u, :d)",
                [{"u": user_id, "d": _doc_id(d)} for d in document_identifiers],
            )
            inserted = cur.rowcount
        conn.commit()
    if inserted != len(document_identifiers):
        logger.warning("user_articles for %s: sent %d but %d were inserted",
                       user_id, len(document_identifiers), inserted)
    logger.info("Wrote %d user_articles for user %s", inserted, user_id)
    return inserted


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


def write_pipeline_status(status: str, ts: datetime | None = None) -> None:
    """Replace pipeline_status with a single (status, timestamp) row."""
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM pipeline_status")
        cur.execute(
            "INSERT INTO pipeline_status (status, timestamp_of_last_update) VALUES (:s, :t)",
            s=status, t=ts or datetime.now(timezone.utc),
        )
        conn.commit()
    logger.info("Wrote pipeline_status=%s to Oracle", status)
