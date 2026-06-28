"""
4-processing/oracle_writer.py
-----------------------------

Writes the gold layer into Oracle — the store the serving layer reads. Connection
defaults match 5-serving/backend/oracle_store.py (pipeline_oracle:1521/GDELT,
user radar). The three tables (assumed to already exist):

    articles(document_identifier PK, mention_identifier, global_event_id,
             in_raw_text, confidence, mention_doc_tone, country, risk_category,
             goldstein, cameo_code, cameo_label, actor,
             latitude, longitude, event_date, age_days)
    user_articles(user_id, document_identifier)   PK (user_id, document_identifier)
    pipeline_status(status, timestamp_of_last_update)

Articles are UPSERTed (MERGE on document_identifier). A user's rows are replaced
(delete-then-insert). pipeline_status is replaced with a single row.
"""

import logging
import os
from datetime import datetime, timezone

import oracledb

logger = logging.getLogger("processing.oracle")

_HOST     = os.getenv("ORACLE_HOST", "pipeline_oracle")
_PORT     = int(os.getenv("ORACLE_PORT", "1521"))
_SERVICE  = os.getenv("ORACLE_SERVICE", "GDELT")
_USER     = os.getenv("ORACLE_USER", "radar")
_PASSWORD = os.getenv("ORACLE_PASSWORD", "radar")
_DSN = f"{_HOST}:{_PORT}/{_SERVICE}"


def _connect():
    return oracledb.connect(user=_USER, password=_PASSWORD, dsn=_DSN)


_MERGE_ARTICLES = """
MERGE INTO articles t
USING (SELECT :document_identifier AS document_identifier FROM dual) s
ON (t.document_identifier = s.document_identifier)
WHEN MATCHED THEN UPDATE SET
    mention_identifier = :mention_identifier, global_event_id = :global_event_id,
    in_raw_text = :in_raw_text, confidence = :confidence, mention_doc_tone = :mention_doc_tone,
    country = :country, risk_category = :risk_category, goldstein = :goldstein,
    cameo_code = :cameo_code, cameo_label = :cameo_label,
    actor = :actor, latitude = :latitude, longitude = :longitude,
    event_date = :event_date, age_days = :age_days
WHEN NOT MATCHED THEN INSERT
    (document_identifier, mention_identifier, global_event_id, in_raw_text, confidence,
     mention_doc_tone, country, risk_category, goldstein, cameo_code,
     cameo_label, actor, latitude, longitude, event_date, age_days)
VALUES
    (:document_identifier, :mention_identifier, :global_event_id, :in_raw_text, :confidence,
     :mention_doc_tone, :country, :risk_category, :goldstein, :cameo_code,
     :cameo_label, :actor, :latitude, :longitude, :event_date, :age_days)
"""


def write_articles(rows: list[dict]) -> int:
    """Upsert article rows into the Oracle `articles` table."""
    if not rows:
        return 0
    with _connect() as conn:
        cur = conn.cursor()
        # Declare types explicitly so executemany doesn't mis-infer from a row
        # whose nullable numeric/date columns happen to be NULL.
        cur.setinputsizes(
            mention_doc_tone=oracledb.DB_TYPE_NUMBER,
            goldstein=oracledb.DB_TYPE_NUMBER,
            latitude=oracledb.DB_TYPE_NUMBER,
            longitude=oracledb.DB_TYPE_NUMBER,
            age_days=oracledb.DB_TYPE_NUMBER,
            event_date=oracledb.DB_TYPE_DATE,
        )
        cur.executemany(_MERGE_ARTICLES, rows)
        conn.commit()
    logger.info("Upserted %d rows into Oracle articles", len(rows))
    return len(rows)


def write_user_articles(user_id: str, document_identifiers: list[str]) -> int:
    """Replace a user's rows in user_articles with the given document set."""
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM user_articles WHERE user_id = :u", u=user_id)
        if document_identifiers:
            cur.executemany(
                "INSERT INTO user_articles (user_id, document_identifier) VALUES (:u, :d)",
                [{"u": user_id, "d": d} for d in document_identifiers],
            )
        conn.commit()
    logger.info("Wrote %d user_articles for user %s", len(document_identifiers), user_id)
    return len(document_identifiers)


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
