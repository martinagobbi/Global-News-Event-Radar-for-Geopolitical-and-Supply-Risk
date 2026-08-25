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

Original GDELT column names are preserved. GLOBALEVENTID is String (an opaque
id, never arithmetic); DATEADDED is UInt64 — not by choice but because it is
the ReplacingMergeTree version column (largest = newest = kept), and that
position requires a plain non-nullable integer or Date/DateTime type,
regardless of what would otherwise be the natural type for the column (see
_EVENTS_BODY). Everything else is Nullable(String) or a Nullable numeric type,
for ingest robustness.

The validation layer is the SOLE owner of this store: it creates the tables
(ON CLUSTER) and runs the events dedup. The processing layer only reads.
"""

import logging
import os
from typing import Iterable

from gdelt import EVENT_COLUMNS, MENTION_COLUMNS

logger = logging.getLogger("validation.storage")

# Per-operation ClickHouse time cap (seconds). Bounds the referential-integrity
# lookup, the two appends, and the dedup OPTIMIZE so a validation cycle can't run
# away. Total cycle time ~ enrichment budget (ENRICH_TIMEOUT_SECONDS) + these ops.
_OP_TIMEOUT = int(os.getenv("CLICKHOUSE_OP_TIMEOUT", "120"))

# Insert quorum: an append is confirmed only once it has reached this many
# replicas of the target shard (the cluster has 3 per shard). Requires at least
# this many replicas up; set CLICKHOUSE_INSERT_QUORUM=1 for a single-replica dev
# setup, or 0 to disable.
_INSERT_QUORUM = int(os.getenv("CLICKHOUSE_INSERT_QUORUM", "2"))

# ── Column + skip-index bodies (shared by the LOCAL tables) ───────────────────
# Sorting key is set per-table in the ENGINE clause below, not here.
# ── Nullability: identity is NOT NULL, everything else is Nullable ───────────
# Two columns per table stay non-null because they are the row's IDENTITY, and
# validation now DROPS any row missing one rather than storing a placeholder:
#     events   GLOBALEVENTID
#     mentions GLOBALEVENTID, MentionIdentifier
# They are also the sorting/sharding keys, and ClickHouse cannot use a Nullable
# column in a sorting key at all — so this is both a design choice and a
# constraint.
#
# GLOBALEVENTID is String, not UInt64. It is an opaque identifier, never
# arithmetic, and typing it as an integer bakes in an assumption about GDELT's id
# format that nothing else in the pipeline needs. Measured cost of the change:
# +41% on that column (385 KiB -> 542 KiB on 107k rows), because a near-sequential
# UInt64 delta-compresses better than its decimal text. That is the price of not
# breaking the day an id contains a letter.
#
# EVERY other column is Nullable, so "not provided" is representable and distinct
# from 0 or "". Note this is NOT a storage win — measured on the same 107k rows
# the typed/nullable variants were mostly LARGER (NumArticles +157%, lat +27%),
# because Nullable adds a per-row null mask and short repetitive strings compress
# extremely well. The win is semantic: avg() skips NULL but is skewed by 0.
_EVENTS_BODY = """(
    GLOBALEVENTID          String,
    Day                    Nullable(String),
    Actor1Name             Nullable(String),
    Actor1CountryCode      Nullable(String),
    Actor2Name             Nullable(String),
    Actor2CountryCode      Nullable(String),
    EventCode              Nullable(String),
    EventRootCode          Nullable(String),
    GoldsteinScale         Nullable(Float64),
    NumArticles            Nullable(Int32),
    AvgTone                Nullable(Float64),
    Actor1Geo_CountryCode  Nullable(String),
    Actor2Geo_CountryCode  Nullable(String),
    ActionGeo_FullName     Nullable(String),
    ActionGeo_CountryCode  Nullable(String),
    ActionGeo_Lat          Nullable(Float64),
    ActionGeo_Long         Nullable(Float64),
    -- Not Nullable, and not String despite every other column here being one:
    -- this is the ReplacingMergeTree version column for gdelt_events_local
    -- (see ENGINE clause below), and that position rejects both a Nullable
    -- wrapper and a non-integer/date underlying type outright:
    --   Code: 169. The column DATEADDED cannot be used as a version column
    --   for storage ReplacingMergeTree because it is of type Nullable(String)
    --   (must be of an integer type or of type Date/DateTime/DateTime64).
    --   (BAD_TYPE_OF_FIELD)
    -- Safe because validator.clean_dateadded() now DROPS any row with no
    -- usable date (instead of nulling the field, which is what it did before
    -- this column had to become non-nullable) — see that function's docstring
    -- for why dropping preserves the same watermark-safety property nulling
    -- had, without writing a 0 sentinel that would sort below every real row.
    DATEADDED              UInt64,
    SOURCEURL              Nullable(String),
    -- coalesce(), same reason as the PARTITION BY fix above: SOURCEURL is
    -- Nullable(String), and ngrambf_v1 (unlike the set(0) indexes below)
    -- refuses a Nullable expression outright:
    --   Code: 80. Ngram and token bloom filter indexes can only be used with
    --   column types `String`, `FixedString`, ... (INCORRECT_QUERY)
    -- idx_mentionid below does not need this: MentionIdentifier is one of the
    -- two NOT NULL identity columns (see the header comment), never Nullable.
    INDEX idx_sourceurl lower(coalesce(SOURCEURL, '')) TYPE ngrambf_v1(4, 4096, 3, 0) GRANULARITY 4,
    INDEX idx_action_cc  ActionGeo_CountryCode TYPE set(0) GRANULARITY 4,
    INDEX idx_actor1_cc  Actor1CountryCode     TYPE set(0) GRANULARITY 4,
    INDEX idx_actor2_cc  Actor2CountryCode     TYPE set(0) GRANULARITY 4
)"""

_MENTIONS_BODY = """(
    GLOBALEVENTID              String,
    MentionTimeDate            Nullable(String),
    MentionSourceName          Nullable(String),
    MentionIdentifier          String,
    SentenceID                 Nullable(String),
    InRawText                  Nullable(Int32),
    Confidence                 Nullable(Int32),
    MentionDocLen              Nullable(Int32),
    MentionDocTone             Nullable(Float64),
    article_title              Nullable(String),
    article_keywords           Nullable(String),
    -- Not Nullable: this is the ReplacingMergeTree version column for
    -- gdelt_mentions_local (see ENGINE clause below), and ClickHouse rejects a
    -- Nullable version column outright, independent of the underlying type:
    --   Code: 169. The column enriched cannot be used as a version column for
    --   storage ReplacingMergeTree because it is of type Nullable(UInt8) (must
    --   be of an integer type or of type Date/DateTime/DateTime64).
    --   (BAD_TYPE_OF_FIELD)
    -- Safe because _to_bool_uint() below always returns a real 0 or 1, never
    -- None — there is no unmapped value this column needs to represent.
    enriched                   UInt8,
    INDEX idx_mentionid lower(MentionIdentifier) TYPE ngrambf_v1(4, 4096, 3, 0) GRANULARITY 4
)"""

_INSERT_EVENTS_SQL = (
    "INSERT INTO gdelt_events (" + ", ".join(EVENT_COLUMNS) + ") VALUES"
)
_INSERT_MENTIONS_SQL = (
    "INSERT INTO gdelt_mentions (" + ", ".join(MENTION_COLUMNS) + ") VALUES"
)


# ── Value coercion for the insert ────────────────────────────────────────────
# GLOBALEVENTID is a String and is never coerced. DATEADDED is an int() cast
# below, not a coercion with a failure fallback: validator.clean_dateadded()
# already dropped every row it could not parse into a clean 14-digit string,
# so by the time a row reaches here DATEADDED is guaranteed present and
# well-formed — int() either succeeds or the row genuinely should not be
# here, in which case it is correct for this to raise rather than silently
# write a placeholder. (An earlier version of this file used a `_to_uint()`
# helper that mapped anything unparseable to 0 for both GLOBALEVENTID and
# DATEADDED — that 0 was the quiet failure behind a frozen watermark:
# max(DATEADDED) ignores NULL but is perfectly happy to return a row of zeros
# as data. That helper is gone; do not reintroduce a numeric fallback here.)
def _text(value):
    """String, or None for a missing value. Never '' — that is a known-empty."""
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() in ("nan", "none", "null"):
        return None
    return text


def _dateadded(value) -> int:
    """
    Convert DATEADDED to int. clean_dateadded() has already validated and filtered
    out bad rows, ensuring DATEADDED is a valid 14-digit string. Handle both string
    and numeric input (Int64 from pandas, or string from legacy code).
    
    THIS MUST NOT RETURN NONE OR RAISE EXCEPTION. By this point, clean_dateadded()
    has already dropped every row with an invalid date. If a row reaches here with
    a bad DATEADDED, it is a pipeline bug, and we should fail loudly so it gets fixed.
    """
    if value is None:
        # This should never happen — clean_dateadded() drops rows with None.
        # If it does, the bug is upstream.
        raise ValueError("DATEADDED is None (should have been filtered by clean_dateadded)")
    # If already numeric (int or float type), just convert and return
    if isinstance(value, (int, float)):
        logger.debug(f"_dateadded: numeric input {repr(value)} ({type(value).__name__}) -> {int(value)}")
        return int(value)
    # Otherwise it's a string. Strip whitespace and convert.
    text = str(value).strip()
    logger.debug(f"_dateadded: string input {repr(value)} ({type(value).__name__}) -> text={repr(text)} -> {int(text)}")
    try:
        return int(text)
    except ValueError:
        # This should not happen — clean_dateadded() validates all dates.
        # If it does, the bug is in clean_dateadded().
        logger.error(f"_dateadded: FAILED to parse {repr(value)} ({type(value).__name__})")
        raise ValueError(f"clean_dateadded() allowed an unparseable DATEADDED: {repr(value)}")


def _key_text(value) -> str:
    """A NOT NULL identity column: always a string, never None."""
    return "" if value is None else str(value).strip()


def _num(value, cast):
    """Numeric, or None. `cast` is int or float."""
    text = _text(value)
    if text is None:
        return None
    try:
        return cast(float(text))
    except (TypeError, ValueError):
        return None


_EVENT_NUMERIC = {"GoldsteinScale": float, "AvgTone": float,
                  "ActionGeo_Lat": float, "ActionGeo_Long": float,
                  "NumArticles": int}
_MENTION_NUMERIC = {"InRawText": int, "Confidence": int,
                    "MentionDocLen": int, "MentionDocTone": float}


def _to_bool_uint(value) -> int:
    """Parse an 'enriched' flag (True/False/1/0/empty) into a UInt8 0/1."""
    return 1 if str(value).strip().lower() in ("1", "true", "t", "yes") else 0


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
                # Entry nodes to fall back on when `host` cannot be reached. In
                # intended mode the six servers sit on six different machines, so
                # losing the one named here would otherwise stop ingestion
                # outright even though the shard's other two replicas hold every
                # row — the data survives the failure but the connection does
                # not. Empty in testing mode, which has a single node.
                alt_hosts=os.getenv("CLICKHOUSE_ALT_HOSTS", "") or None,
                # Socket timeout sits just above the server-side cap below.
                send_receive_timeout=_OP_TIMEOUT + 10,
                # insert_distributed_sync: an INSERT into a Distributed table
                # returns only once the rows have reached their target shards,
                # so the dedup / lookups below see them immediately.
                # max_execution_time: server-side cap on every query (lookup,
                # appends, OPTIMIZE) so a validation cycle stays bounded.
                settings={"use_numpy": False, "insert_distributed_sync": 1,
                          "max_execution_time": _OP_TIMEOUT,
                          "insert_quorum": _INSERT_QUORUM},
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

        # Events: local ReplicatedReplacingMergeTree (3 replicas/shard) + router.
        # {shard}/{replica} are ClickHouse macros from each node's macros.xml;
        # the doubled braces emit them literally through the Python f-string.
        client.execute(
            f"CREATE TABLE IF NOT EXISTS gdelt_events_local ON CLUSTER {c} "
            f"{_EVENTS_BODY} "
            f"ENGINE = ReplicatedReplacingMergeTree("
            f"'/clickhouse/tables/{{shard}}/gdelt_events_local', '{{replica}}', DATEADDED) "
            # coalesce(), not allow_nullable_key. Day is Nullable(String) — see
            # _EVENTS_BODY — and substring() of a Nullable value is itself
            # Nullable, which ClickHouse refuses as a partition key by default:
            #   Code: 44. Partition key contains nullable columns, but merge
            #   tree setting `allow_nullable_key` is disabled.
            # Verified 2026-08-25: this was never caught before now because the
            # live table predated Day becoming Nullable — CREATE TABLE IF NOT
            # EXISTS silently kept the old definition, so this statement had
            # never actually executed until a `recreate` finally dropped that
            # old table. allow_nullable_key=1 would also satisfy ClickHouse, but
            # nullable partition/sort keys have real documented rough edges
            # (NULL handling in comparisons and merges); coalescing to a
            # sentinel keeps the safer default on and gives every row with no
            # usable Day a single, well-defined partition instead.
            f"PARTITION BY substring(coalesce(Day, '000000'), 1, 6) "
            # coalesce(), same reason as the PARTITION BY fix above:
            # ActionGeo_CountryCode is Nullable(String), and a sorting key
            # can't contain a Nullable column any more than a partition key
            # can (same Code 44 as Day, same fix).
            f"ORDER BY (GLOBALEVENTID, coalesce(ActionGeo_CountryCode, '')) "
            f"SETTINGS index_granularity = 8192"
        )
        client.execute(
            f"CREATE TABLE IF NOT EXISTS gdelt_events ON CLUSTER {c} "
            f"AS gdelt_events_local "
            f"ENGINE = Distributed({c}, {db}, gdelt_events_local, cityHash64(GLOBALEVENTID))"
        )

        # Mentions: local ReplicatedReplacingMergeTree (3 replicas/shard) — dedups
        # by the sort key (GLOBALEVENTID, MentionIdentifier), the `enriched` row
        # winning, so re-ingesting a slice (e.g. after a failover re-poll) is
        # idempotent. Readers use FINAL to collapse duplicates at query time.
        client.execute(
            f"CREATE TABLE IF NOT EXISTS gdelt_mentions_local ON CLUSTER {c} "
            f"{_MENTIONS_BODY} "
            f"ENGINE = ReplicatedReplacingMergeTree("
            f"'/clickhouse/tables/{{shard}}/gdelt_mentions_local', '{{replica}}', enriched) "
            # Same reason and same fix as gdelt_events_local above:
            # MentionTimeDate is Nullable(String), so it needs coalescing before
            # it can be a partition key.
            f"PARTITION BY substring(coalesce(MentionTimeDate, '000000'), 1, 6) "
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
                _key_text(r[col]) if col == "GLOBALEVENTID"
                else _dateadded(r[col]) if col == "DATEADDED"
                else _num(r[col], _EVENT_NUMERIC[col]) if col in _EVENT_NUMERIC
                else _text(r[col])
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
                _key_text(r[col]) if col in ("GLOBALEVENTID", "MentionIdentifier")
                else _to_bool_uint(r[col]) if col == "enriched"
                else _num(r[col], _MENTION_NUMERIC[col]) if col in _MENTION_NUMERIC
                else _text(r[col])
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
