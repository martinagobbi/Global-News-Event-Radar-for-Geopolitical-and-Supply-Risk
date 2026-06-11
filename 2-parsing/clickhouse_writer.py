"""
src/storage/clickhouse_writer.py

Table schema:
    silver_events — each row corresponds to one silver event produced
                    by the parser (to_silver_event).

Dependencies:
    clickhouse-driver>=0.2.9  (see requirements.txt)

Typical production usage:
    writer = ClickHouseWriter()
    writer.ensure_table()
    writer.write_batch(silver_events)

Development / test usage:
    writer = ClickHouseWriter(host="localhost", database="default")
    # or use ClickHouseWriter.from_env() to read from environment variables
"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# DDL for the silver_events table
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS silver_events (
    event_id        String,
    date            String,
    event_code      String,
    event_root      String,
    actor1          String,
    actor2          String,
    country_code    String,
    fips_country    String,
    lat             Float64,
    lon             Float64,
    goldstein       Float64,
    avg_tone        Float64,
    num_articles    Int32,
    risk_score      Float64,
    source_url      String,
    source          String,
    inserted_at     DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (date, country_code, event_code)
PARTITION BY substring(date, 1, 6)
SETTINGS index_granularity = 8192
"""

# INSERT statement — uses parameters to prevent SQL injection
_INSERT_SQL = """
INSERT INTO silver_events (
    event_id, date, event_code, event_root,
    actor1, actor2, country_code, fips_country,
    lat, lon, goldstein, avg_tone, num_articles,
    risk_score, source_url, source
) VALUES
"""

# DDL for the silver_mentions table (mentions enriched with Newspaper3k)
_CREATE_MENTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS silver_mentions (
    event_id          String,
    event_time        String,
    mention_time      String,
    mention_type      String,
    source_name       String,
    mention_url       String,
    confidence        Float64,
    doc_tone          Float64,
    article_title     String,
    article_keywords  String,
    enriched          UInt8,
    inserted_at       DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (event_id, mention_time)
SETTINGS index_granularity = 8192
"""

_INSERT_MENTIONS_SQL = """
INSERT INTO silver_mentions (
    event_id, event_time, mention_time, mention_type, source_name,
    mention_url, confidence, doc_tone,
    article_title, article_keywords, enriched
) VALUES
"""


class ClickHouseWriter:
    """
    Wrapper around clickhouse-driver for writing to the silver layer.

    Parameters
    ----------
    host     : str  — ClickHouse hostname (default "localhost")
    port     : int  — ClickHouse native protocol port (default 9000)
    database : str  — database name (default "default")
    user     : str  — ClickHouse user (default "default")
    password : str  — ClickHouse password (default "")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9000,
        database: str = "default",
        user: str = "default",
        password: str = "",
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._client = None  # lazy initialisation

    @classmethod
    def from_env(cls) -> "ClickHouseWriter":
        """
        Build a ClickHouseWriter by reading configuration from environment variables:
            CLICKHOUSE_HOST      (default: localhost)
            CLICKHOUSE_PORT      (default: 9000)
            CLICKHOUSE_DATABASE  (default: default)
            CLICKHOUSE_USER      (default: default)
            CLICKHOUSE_PASSWORD  (default: "")
        """
        return cls(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
            database=os.getenv("CLICKHOUSE_DATABASE", "default"),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )

    def _get_client(self):
        """Return the ClickHouse client, creating it on first access (lazy init)."""
        if self._client is None:
            try:
                from clickhouse_driver import Client  # lazy import
            except ImportError as exc:
                raise ImportError(
                    "clickhouse-driver not found. "
                    "Install with: pip install clickhouse-driver"
                ) from exc

            self._client = Client(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                settings={"use_numpy": False},
            )
            logger.info(
                "ClickHouse client connected to %s:%d/%s",
                self.host, self.port, self.database,
            )
        return self._client

    def ensure_table(self) -> None:
        """
        Create the silver_events and silver_mentions tables if they do not
        exist (idempotent). Call once at service startup.
        """
        client = self._get_client()
        client.execute(_CREATE_TABLE_SQL)
        client.execute(_CREATE_MENTIONS_TABLE_SQL)
        logger.info("Tables silver_events and silver_mentions are ready on ClickHouse")

    def write_event(self, event: dict) -> None:
        """
        Write a single silver event to ClickHouse.
        For high-volume writes prefer write_batch().
        """
        self.write_batch([event])

    def write_batch(self, events: list[dict]) -> int:
        """
        Write a list of silver events to ClickHouse in a single operation.

        Parameters
        ----------
        events : list[dict] — output of to_silver_event() from the parser

        Returns
        -------
        int — number of events written
        """
        if not events:
            logger.debug("write_batch: empty list, nothing to write")
            return 0

        rows = [_event_to_row(e) for e in events]
        client = self._get_client()

        client.execute(
            _INSERT_SQL,
            rows,
            types_check=True,
        )
        logger.info("Wrote %d silver events to ClickHouse", len(rows))
        return len(rows)

    def write_mentions_batch(self, mentions: list[dict]) -> int:
        """
        Write a list of enriched silver mentions to ClickHouse.

        Parameters
        ----------
        mentions : list[dict] — output of to_silver_mention() after
                                 enrichment.enrich_mentions_parallel()

        Returns
        -------
        int — number of mentions written
        """
        if not mentions:
            logger.debug("write_mentions_batch: empty list, nothing to write")
            return 0

        rows = [_mention_to_row(m) for m in mentions]
        client = self._get_client()
        client.execute(
            _INSERT_MENTIONS_SQL,
            rows,
            types_check=True,
        )
        logger.info("Wrote %d silver mentions to ClickHouse", len(rows))
        return len(rows)

    def query_silver(
        self,
        country_code: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        min_risk_score: float = 0.0,
        limit: int = 1000,
    ) -> list[dict]:
        """
        Query the silver layer with optional filters.
        Useful for the processor and for debugging.

        Parameters
        ----------
        country_code   : str   — CAMEO country code (e.g. "US")
        date_from      : str   — start date YYYYMMDD (e.g. "20240101")
        date_to        : str   — end date YYYYMMDD
        min_risk_score : float — minimum risk score threshold
        limit          : int   — maximum number of rows returned

        Returns
        -------
        list[dict] — list of events in the silver schema
        """
        conditions = ["risk_score >= %(min_risk)s"]
        params: dict = {"min_risk": min_risk_score, "lim": limit}

        if country_code:
            conditions.append("country_code = %(country)s")
            params["country"] = country_code.upper()
        if date_from:
            conditions.append("date >= %(date_from)s")
            params["date_from"] = date_from
        if date_to:
            conditions.append("date <= %(date_to)s")
            params["date_to"] = date_to

        where = " AND ".join(conditions)
        sql = f"""
            SELECT
                event_id, date, event_code, event_root,
                actor1, actor2, country_code, fips_country,
                lat, lon, goldstein, avg_tone, num_articles,
                risk_score, source_url, source
            FROM silver_events
            WHERE {where}
            ORDER BY risk_score DESC, date DESC
            LIMIT %(lim)s
        """
        client = self._get_client()
        rows = client.execute(sql, params, with_column_types=True)

        if not rows or len(rows) < 2:
            return []

        data_rows, columns = rows
        col_names = [c[0] for c in columns]
        return [dict(zip(col_names, row)) for row in data_rows]

    def close(self) -> None:
        """Close the database connection."""
        if self._client is not None:
            self._client.disconnect()
            self._client = None
            logger.info("ClickHouse connection closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ═══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _event_to_row(event: dict) -> tuple:
    """
    Convert a silver event dict into the ordered tuple expected by
    clickhouse-driver for the INSERT statement.
    Column order must match exactly the columns listed in _INSERT_SQL.
    """
    return (
        str(event.get("event_id", "")),
        str(event.get("date", "")),
        str(event.get("event_code", "")),
        str(event.get("event_root", "")),
        str(event.get("actor1", "")),
        str(event.get("actor2", "")),
        str(event.get("country_code", "")),
        str(event.get("fips_country", "")),
        float(event.get("lat", 0.0)),
        float(event.get("lon", 0.0)),
        float(event.get("goldstein", 0.0)),
        float(event.get("avg_tone", 0.0)),
        int(event.get("num_articles", 0)),
        float(event.get("risk_score", 0.0)),
        str(event.get("source_url", "")),
        str(event.get("source", "gdelt_events")),
    )


def _mention_to_row(mention: dict) -> tuple:
    """
    Convert a silver mention dict into the ordered tuple expected by
    clickhouse-driver for the INSERT. Column order must match exactly the
    columns listed in _INSERT_MENTIONS_SQL.
    """
    return (
        str(mention.get("event_id", "")),
        str(mention.get("event_time", "")),
        str(mention.get("mention_time", "")),
        str(mention.get("mention_type", "")),
        str(mention.get("source_name", "")),
        str(mention.get("mention_url", "")),
        float(mention.get("confidence", 0.0)),
        float(mention.get("doc_tone", 0.0)),
        str(mention.get("article_title", "")),
        str(mention.get("article_keywords", "")),
        int(bool(mention.get("enriched", False))),
    )
