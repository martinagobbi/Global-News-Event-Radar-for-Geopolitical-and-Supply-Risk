"""
3-validation_and_storage/storage.py
-----------------------

ClickHouse access for the validation layer, against the 2-shard cluster.

The store is split, per table, into:

    * a LOCAL table  (gdelt_events_local / gdelt_mentions_local) that physically
      holds rows on each shard node, created ON CLUSTER so it exists on both;
    * a DISTRIBUTED table (gdelt_events / gdelt_mentions) that holds no data —
      it routes each written row to a shard by cityHash64(GLOBALEVENTID) and
      fans reads out across the shards.

Both tables are sharded by cityHash64(GLOBALEVENTID), so an event and all of
its mentions land on the SAME node (local joins), and duplicate copies of an
event from successive 15-min batches land on the same node (so the per-node
ReplacingMergeTree(DATEADDED) dedup actually sees and collapses them).

Original GDELT column names are preserved. GLOBALEVENTID and DATEADDED are
typed UInt64 (the latter is the ReplacingMergeTree version: largest = newest =
kept); everything else is String for ingest robustness.

The validation layer is the SOLE owner of this store: it creates the tables
(ON CLUSTER) and runs the events dedup. The processing layer only reads.
"""

import logging
import os
from typing import Iterable

from gdelt import EVENT_COLUMNS, MENTION_COLUMNS

logger = logging.getLogger("validation.storage")

# ── Column + skip-index bodies (shared by the LOCAL tables) ───────────────────
# Sorting key is set per-table in the ENGINE clause below, not here.
_EVENTS_BODY = """(
    GLOBALEVENTID          UInt64,
    Day                    String,
    MonthYear              String,
    Year                   String,
    FractionDate           String,
    Actor1Code             String,
    Actor1Name             String,
    Actor1CountryCode      String,
    Actor1KnownGroupCode   String,
    Actor1EthnicCode       String,
    Actor1Religion1Code    String,
    Actor1Religion2Code    String,
    Actor1Type1Code        String,
    Actor1Type2Code        String,
    Actor1Type3Code        String,
    Actor2Code             String,
    Actor2Name             String,
    Actor2CountryCode      String,
    Actor2KnownGroupCode   String,
    Actor2EthnicCode       String,
    Actor2Religion1Code    String,
    Actor2Religion2Code    String,
    Actor2Type1Code        String,
    Actor2Type2Code        String,
    Actor2Type3Code        String,
    IsRootEvent            String,
    EventCode              String,
    EventBaseCode          String,
    EventRootCode          String,
    QuadClass              String,
    GoldsteinScale         String,
    NumMentions            String,
    NumSources             String,
    NumArticles            String,
    AvgTone                String,
    Actor1Geo_Type         String,
    Actor1Geo_FullName     String,
    Actor1Geo_CountryCode  String,
    Actor1Geo_ADM1Code     String,
    Actor1Geo_ADM2Code     String,
    Actor1Geo_Lat          String,
    Actor1Geo_Long         String,
    Actor1Geo_FeatureID    String,
    Actor2Geo_Type         String,
    Actor2Geo_FullName     String,
    Actor2Geo_CountryCode  String,
    Actor2Geo_ADM1Code     String,
    Actor2Geo_ADM2Code     String,
    Actor2Geo_Lat          String,
    Actor2Geo_Long         String,
    Actor2Geo_FeatureID    String,
    ActionGeo_Type         String,
    ActionGeo_FullName     String,
    ActionGeo_CountryCode  String,
    ActionGeo_ADM1Code     String,
    ActionGeo_ADM2Code     String,
    ActionGeo_Lat          String,
    ActionGeo_Long         String,
    ActionGeo_FeatureID    String,
    DATEADDED              UInt64,
    SOURCEURL              String,
    INDEX idx_sourceurl lower(SOURCEURL) TYPE ngrambf_v1(4, 4096, 3, 0) GRANULARITY 4,
    INDEX idx_action_cc  ActionGeo_CountryCode TYPE set(0) GRANULARITY 4,
    INDEX idx_actor1_cc  Actor1CountryCode     TYPE set(0) GRANULARITY 4,
    INDEX idx_actor2_cc  Actor2CountryCode     TYPE set(0) GRANULARITY 4
)"""

_MENTIONS_BODY = """(
    GLOBALEVENTID              UInt64,
    EventTimeDate              String,
    MentionTimeDate            String,
    MentionType                String,
    MentionSourceName          String,
    MentionIdentifier          String,
    SentenceID                 String,
    Actor1CharOffset           String,
    Actor2CharOffset           String,
    ActionCharOffset           String,
    InRawText                  String,
    Confidence                 String,
    MentionDocLen              String,
    MentionDocTone             String,
    MentionDocTranslationInfo  String,
    Extras                     String,
    INDEX idx_mentionid lower(MentionIdentifier) TYPE ngrambf_v1(4, 4096, 3, 0) GRANULARITY 4
)"""

_INSERT_EVENTS_SQL = (
    "INSERT INTO gdelt_events (" + ", ".join(EVENT_COLUMNS) + ") VALUES"
)
_INSERT_MENTIONS_SQL = (
    "INSERT INTO gdelt_mentions (" + ", ".join(MENTION_COLUMNS) + ") VALUES"
)


def _to_uint(value) -> int:
    """Parse a GLOBALEVENTID / DATEADDED string into an int, 0 on failure."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


class Storage:
    """clickhouse-driver wrapper for the validation layer's cluster writes."""

    def __init__(self, host="clickhouse-01", port=9000,
                 database="default", user="default", password="",
                 cluster=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.cluster = cluster or os.getenv("CLICKHOUSE_CLUSTER", "gnews_cluster")
        self._client = None

    def _get_client(self):
        if self._client is None:
            from clickhouse_driver import Client
            self._client = Client(
                host=self.host, port=self.port, database=self.database,
                user=self.user, password=self.password,
                # insert_distributed_sync: an INSERT into a Distributed table
                # returns only once the rows have reached their target shards,
                # so the dedup / lookups below see them immediately.
                settings={"use_numpy": False, "insert_distributed_sync": 1},
            )
            logger.info("ClickHouse connected: %s:%d/%s (cluster=%s)",
                        self.host, self.port, self.database, self.cluster)
        return self._client

    def ensure_tables(self) -> None:
        """
        Create, on every shard, the local tables and the Distributed routers
        (idempotent). cityHash64(GLOBALEVENTID) is the sharding key for both.
        """
        client = self._get_client()
        c, db = self.cluster, self.database

        # Events: local ReplacingMergeTree + Distributed router.
        client.execute(
            f"CREATE TABLE IF NOT EXISTS gdelt_events_local ON CLUSTER {c} "
            f"{_EVENTS_BODY} "
            f"ENGINE = ReplacingMergeTree(DATEADDED) "
            f"PARTITION BY substring(Day, 1, 6) "
            f"ORDER BY (ActionGeo_CountryCode, GLOBALEVENTID) "
            f"SETTINGS index_granularity = 8192"
        )
        client.execute(
            f"CREATE TABLE IF NOT EXISTS gdelt_events ON CLUSTER {c} "
            f"AS gdelt_events_local "
            f"ENGINE = Distributed({c}, {db}, gdelt_events_local, cityHash64(GLOBALEVENTID))"
        )

        # Mentions: local MergeTree (no dedup) + Distributed router.
        client.execute(
            f"CREATE TABLE IF NOT EXISTS gdelt_mentions_local ON CLUSTER {c} "
            f"{_MENTIONS_BODY} "
            f"ENGINE = MergeTree() "
            f"PARTITION BY substring(MentionTimeDate, 1, 6) "
            f"ORDER BY (GLOBALEVENTID, MentionIdentifier) "
            f"SETTINGS index_granularity = 8192"
        )
        client.execute(
            f"CREATE TABLE IF NOT EXISTS gdelt_mentions ON CLUSTER {c} "
            f"AS gdelt_mentions_local "
            f"ENGINE = Distributed({c}, {db}, gdelt_mentions_local, cityHash64(GLOBALEVENTID))"
        )
        logger.info("Cluster tables ready (local + Distributed) on '%s'", c)

    def existing_event_ids(self, candidate_ids: Iterable[int]) -> set[int]:
        """
        Return the subset of candidate_ids already present in gdelt_events.
        Reads the Distributed table, so it checks every shard. Only the
        candidate ids are queried, keeping this cheap as the table grows.
        """
        ids = [i for i in {int(c) for c in candidate_ids} if i > 0]
        if not ids:
            return set()
        rows = self._get_client().execute(
            "SELECT DISTINCT GLOBALEVENTID FROM gdelt_events "
            "WHERE GLOBALEVENTID IN %(ids)s",
            {"ids": ids},
        )
        return {row[0] for row in rows}

    def append_events(self, df) -> int:
        """Append an events DataFrame to the Distributed gdelt_events router."""
        if df.empty:
            return 0
        rows = [
            tuple(
                _to_uint(r[col]) if col in ("GLOBALEVENTID", "DATEADDED")
                else str(r[col])
                for col in EVENT_COLUMNS
            )
            for r in df.to_dict("records")
        ]
        self._get_client().execute(_INSERT_EVENTS_SQL, rows, types_check=True)
        logger.info("Appended %d rows to gdelt_events", len(rows))
        return len(rows)

    def append_mentions(self, df) -> int:
        """Append a mentions DataFrame to the Distributed gdelt_mentions router."""
        if df.empty:
            return 0
        rows = [
            tuple(
                _to_uint(r[col]) if col == "GLOBALEVENTID" else str(r[col])
                for col in MENTION_COLUMNS
            )
            for r in df.to_dict("records")
        ]
        self._get_client().execute(_INSERT_MENTIONS_SQL, rows, types_check=True)
        logger.info("Appended %d rows to gdelt_mentions", len(rows))
        return len(rows)

    def optimize_events(self) -> None:
        """
        Local dedup fallback: collapse duplicate GLOBALEVENTIDs on every shard,
        keeping the largest DATEADDED. In normal operation the storage container
        does this; here it runs ON CLUSTER so each node's local table is merged.
        """
        self._get_client().execute(
            f"OPTIMIZE TABLE gdelt_events_local ON CLUSTER {self.cluster} FINAL"
        )
        logger.info("OPTIMIZE gdelt_events_local ON CLUSTER FINAL complete")

    def close(self) -> None:
        if self._client is not None:
            self._client.disconnect()
            self._client = None
