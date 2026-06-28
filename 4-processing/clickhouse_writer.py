"""
src/storage/clickhouse_writer.py

"""

import logging
import os
from typing import Optional

import pandas as pd

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
    source_url, source
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
        Create the silver_events table if it does not exist (idempotent).
        Call once at service startup.
        """
        client = self._get_client()
        client.execute(_CREATE_TABLE_SQL)
        logger.info("Table silver_events is ready on ClickHouse")

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

    def query_silver(
        self,
        country_code: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
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
        limit          : int   — maximum number of rows returned

        Returns
        -------
        list[dict] — list of events in the silver schema
        """
        conditions = ["1=1"]
        params: dict = {"lim": limit}

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
                source_url, source
            FROM silver_events
            WHERE {where}
            ORDER BY date DESC
            LIMIT %(lim)s
        """
        client = self._get_client()
        rows = client.execute(sql, params, with_column_types=True)

        if not rows or len(rows) < 2:
            return []

        data_rows, columns = rows
        col_names = [c[0] for c in columns]
        return [dict(zip(col_names, row)) for row in data_rows]

    # ── New store readers (gdelt_events / gdelt_mentions) ────────────────────

    def query_events(self, date_from=None, date_to=None, limit=5000) -> pd.DataFrame:
        """
        Read deduplicated events from the gdelt_events Distributed table.
        Returns a DataFrame (with column headers even when there are 0 rows).
        date_from/date_to are YYYYMMDD bounds applied to the GDELT `Day` column.
        """
        conditions = ["1=1"]
        params: dict = {"lim": int(limit)}
        if date_from:
            conditions.append("Day >= %(df)s")
            params["df"] = date_from
        if date_to:
            conditions.append("Day <= %(dt)s")
            params["dt"] = date_to
        where = " AND ".join(conditions)
        sql = (
            f"SELECT * FROM gdelt_events FINAL WHERE {where} "
            f"ORDER BY DATEADDED DESC LIMIT %(lim)s"
        )
        data, cols = self._get_client().execute(sql, params, with_column_types=True)
        return pd.DataFrame(data, columns=[c[0] for c in cols])

    def query_mentions_for_events(self, event_ids, limit=50000) -> pd.DataFrame:
        """
        Read the mentions whose GLOBALEVENTID is in `event_ids` from the
        gdelt_mentions Distributed table. Returns a DataFrame (headers even when
        empty — an empty id list yields a 0-row frame with the right columns).
        """
        ids = [int(i) for i in event_ids if i]
        client = self._get_client()
        if not ids:
            data, cols = client.execute(
                "SELECT * FROM gdelt_mentions LIMIT 0", with_column_types=True)
            return pd.DataFrame(data, columns=[c[0] for c in cols])
        data, cols = client.execute(
            "SELECT * FROM gdelt_mentions WHERE GLOBALEVENTID IN %(ids)s LIMIT %(lim)s",
            {"ids": ids, "lim": int(limit)}, with_column_types=True,
        )
        return pd.DataFrame(data, columns=[c[0] for c in cols])

    def query_user_documents(
        self,
        cameo_codes=None,
        fips_codes=None,
        keywords=None,
        event_limit: int = 20000,
        mention_limit: int = 50000,
    ):
        """
        Per-user filter, pushed down into ClickHouse (the scalable path).

        1. Geographic filter on gdelt_events (FINAL): keep events whose
           Actor1/Actor2CountryCode is in the user's CAMEO set OR whose
           ActionGeo_/Actor1Geo_/Actor2Geo_CountryCode is in the FIPS set
           (match if EITHER standard hits). No geo codes -> all recent events.
        2. Keyword filter on gdelt_mentions for those events via
           build_keyword_clause (URL ngrambf LIKE / enriched position match).
           No keywords -> every mention of the matched events.

        Returns (events_df, mentions_df), ready for gold.build_article_rows().
        """
        from processor import build_keyword_clause  # local: avoid import cycle

        client = self._get_client()

        geo_sql, geo_params = _build_geo_clause(cameo_codes, fips_codes)
        ev_params = {**geo_params, "elim": int(event_limit)}
        ev_sql = (
            f"SELECT * FROM gdelt_events FINAL WHERE {geo_sql or '1=1'} "
            f"ORDER BY DATEADDED DESC LIMIT %(elim)s"
        )
        edata, ecols = client.execute(ev_sql, ev_params, with_column_types=True)
        events_df = pd.DataFrame(edata, columns=[c[0] for c in ecols])

        if events_df.empty:
            mdata, mcols = client.execute(
                "SELECT * FROM gdelt_mentions LIMIT 0", with_column_types=True)
            return events_df, pd.DataFrame(mdata, columns=[c[0] for c in mcols])

        event_ids = [int(i) for i in events_df["GLOBALEVENTID"].tolist() if i]
        kw_sql, kw_params = build_keyword_clause(keywords or [])
        m_where = "GLOBALEVENTID IN %(ids)s"
        m_params = {"ids": event_ids, "mlim": int(mention_limit)}
        if kw_sql:
            m_where += f" AND {kw_sql}"
            m_params.update(kw_params)
        m_sql = f"SELECT * FROM gdelt_mentions WHERE {m_where} LIMIT %(mlim)s"
        mdata, mcols = client.execute(m_sql, m_params, with_column_types=True)
        return events_df, pd.DataFrame(mdata, columns=[c[0] for c in mcols])

    def silver_watermark(self):
        """
        Max DATEADDED on gdelt_events — advances each time the validation layer
        appends a new 15-min batch. Used as a cheap change signal by the silver
        trigger (ClickHouse has no change stream). Returns an int, or None when
        the store is empty.
        """
        rows = self._get_client().execute("SELECT max(DATEADDED) FROM gdelt_events")
        if rows and rows[0] and rows[0][0]:
            return int(rows[0][0])
        return None

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
        str(event.get("source_url", "")),
        str(event.get("source", "gdelt_events")),
    )


def _build_geo_clause(cameo_codes, fips_codes):
    """
    Build the per-user geographic WHERE fragment for gdelt_events.

    CAMEO codes match the actor-affiliation columns (Actor1/Actor2CountryCode);
    FIPS codes match the geo columns (ActionGeo_/Actor1Geo_/Actor2Geo_CountryCode).
    Match if EITHER standard hits. Returns ("", {}) when there is no geo filter.
    """
    parts: list[str] = []
    params: dict = {}
    if cameo_codes:
        params["cameo"] = list(cameo_codes)
        parts.append("(Actor1CountryCode IN %(cameo)s OR Actor2CountryCode IN %(cameo)s)")
    if fips_codes:
        params["fips"] = list(fips_codes)
        parts.append(
            "(ActionGeo_CountryCode IN %(fips)s "
            "OR Actor1Geo_CountryCode IN %(fips)s "
            "OR Actor2Geo_CountryCode IN %(fips)s)"
        )
    if not parts:
        return "", {}
    return "(" + " OR ".join(parts) + ")", params
