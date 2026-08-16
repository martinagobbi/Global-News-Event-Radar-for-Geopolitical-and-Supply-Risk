#!/usr/bin/env python
"""
4-processing/spark_gold.py
--------------------------
DISTRIBUTED silver -> gold, with PySpark. An alternative to the in-process
pandas path in main.py, for when the per-user result no longer fits (or should
no longer fit) on one machine.

Why this removes the row caps
-----------------------------
The pandas path caps its ClickHouse reads (GOLD_EVENTS_LIMIT / mention_limit)
because every row lands in ONE process's memory. Here nothing is materialised
centrally:

  * READ  — a partitioned JDBC read. Spark issues `numPartitions` concurrent
            queries, each with a WHERE clause over a disjoint GLOBALEVENTID
            range (that is what partitionColumn/lowerBound/upperBound do), so
            each executor pulls only its own slice, in parallel.
  * JOIN  — events x mentions is a distributed shuffle join across executors.
  * WRITE — df.write.jdbc() opens one connection PER PARTITION, so the gold
            insert is executed by the executors in parallel too.

Memory per machine is therefore bounded by the partition size, not by the total,
and adding workers adds throughput. No LIMIT is applied anywhere in this file.

What it produces (identical contract to the pandas path)
--------------------------------------------------------
    articles      — one row per article (doc_id = SHA-256 of the URL)
    user_articles — (user_id, doc_id) for every user the article matches

Per-user matching runs against a CACHED, distributed DataFrame: the catalogue is
built once, then each user's geo + keyword predicate is evaluated by the cluster.

Submitting it
-------------
    docker compose -f docker-compose.spark.yml up -d          # master + workers
    docker compose -f docker-compose.spark.yml run --rm spark-submit

Environment (same names as the rest of the pipeline)
    CLICKHOUSE_HOST/PORT, POSTGRES_DSN (or POSTGRES_HOST/PORT/DB/USER/PASSWORD),
    MONGO_URI/MONGO_DB/MONGO_COLLECTION
    SPARK_MASTER            default spark://spark-master:7077
    SPARK_READ_PARTITIONS   parallel JDBC readers  (default 8)
    SPARK_WRITE_PARTITIONS  parallel PostgreSQL writers(default 4)

NOTE: never run against live stores from this session — treat the first run as a
smoke test and watch the Spark UI on :8080.
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType

import countries
from processor import normalize_keyword, tokenize_keyword_enriched, stem_token

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("spark_gold")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-s1r1")
# DELIBERATELY NOT CLICKHOUSE_PORT. That variable is the NATIVE protocol port
# (9000), used by clickhouse-driver for the watermark poll and the retention
# deletes, and it is set to 9000 on this very container. JDBC speaks ClickHouse's
# HTTP protocol instead, on 8123; pointing it at 9000 connects and then fails
# with "java.sql.SQLException: Connection reset", because the native port answers
# nothing it understands. The two ports need two variables.
CH_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CH_DB   = os.getenv("CLICKHOUSE_DATABASE", "default")

PG_HOST = os.getenv("POSTGRES_HOST", "pipeline_postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB   = os.getenv("POSTGRES_DB", "radar")
PG_USER = os.getenv("POSTGRES_USER", "radar")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "radar")
# The driver-side connection (the publish step). In intended mode this lists all
# three members with target_session_attrs=read-write, so libpq lands on whichever
# node Patroni has made leader.
PG_DSN  = os.getenv("POSTGRES_DSN") or \
    f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

READ_PARTITIONS  = int(os.getenv("SPARK_READ_PARTITIONS", "8"))
WRITE_PARTITIONS = int(os.getenv("SPARK_WRITE_PARTITIONS", "4"))

# The global status file the validation layer writes on the shared volume. The
# Spark container mounts that volume read-only so it can mirror the value into
# PostgreSQL, exactly as the pandas path's recompute_all does.
STATUS_FILE = Path(os.getenv("STATUS_DIR", "/data/status")) / "pipeline_status.json"

CH_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}"
# The executors' JDBC URL. targetServerType=primary makes the PostgreSQL JDBC
# driver skip any standby it is offered, which matters because the executors
# write and a standby is read-only; it is the JDBC counterpart of libpq's
# target_session_attrs=read-write used by PG_DSN above.
PG_URL = (f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
          "?targetServerType=primary")


# ── UDF: the gold primary key ────────────────────────────────────────────────
# Same definition as postgres_writer._doc_id, so both paths address the same rows.
def _doc_id(url: str):
    if url is None:
        return None
    return hashlib.sha256(url.encode("utf-8")).digest()


doc_id_udf = F.udf(_doc_id, BinaryType())


def _spark() -> SparkSession:
    """
    The session, created once and then reused.

    SPARK_MASTER selects where the work runs, and is the ONLY difference between
    the two modes:

        local[*]                  testing — Spark runs inside this container, on
                                  every available core. No master or worker
                                  containers exist, so testing mode stays a small
                                  stack while running exactly the same code.
        spark://spark-master:7077 intended — the work is distributed across the
                                  worker containers.

    `getOrCreate` returns the existing session on every call after the first.
    That matters now that this module is driven by a resident FastAPI process
    rather than a one-shot spark-submit: building a session costs seconds (a JVM
    start in local mode, a cluster handshake otherwise), and a recompute runs
    whenever silver advances or a user edits their preferences.
    """
    return (
        SparkSession.builder
        .appName("radar-silver-to-gold")
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        # Spark's default is 200 shuffle partitions, which is sized for a cluster
        # and a dataset far larger than this one. Measured on the 30-day seed
        # (~104k events, ~111k mentions) it put ~500 rows in each partition, so a
        # recompute spent almost all of its ~3 minutes on task scheduling rather
        # than work. This is the single knob that matters for how long a recompute
        # takes; raise it in intended mode, where there are real executors to
        # spread across and the partitions are worth having.
        .config("spark.sql.shuffle.partitions",
                os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .getOrCreate()
    )


def read_partitioned(spark, table: str):
    """
    Parallel JDBC read of a whole ClickHouse table — NO LIMIT.

    Spark splits [min, max] of GLOBALEVENTID into `numPartitions` ranges and runs
    one query per range, concurrently, on different executors. The bounds come
    from ClickHouse itself so the ranges match the real key distribution.
    """
    bounds = (
        spark.read.format("jdbc")
        .option("url", CH_URL)
        .option("query", f"SELECT min(GLOBALEVENTID) lo, max(GLOBALEVENTID) hi FROM {table}")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .load()
        .collect()[0]
    )
    lo, hi = bounds["lo"], bounds["hi"]
    if lo is None or hi is None:
        logger.warning("%s is empty", table)
        return None

    logger.info("reading %s over %d partitions (ids %s..%s)", table, READ_PARTITIONS, lo, hi)
    return (
        spark.read.format("jdbc")
        .option("url", CH_URL)
        .option("dbtable", f"{table} FINAL")       # FINAL collapses re-ingested duplicates
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("partitionColumn", "GLOBALEVENTID")
        .option("lowerBound", str(lo))
        .option("upperBound", str(hi))
        .option("numPartitions", READ_PARTITIONS)
        .load()
    )


def build_catalogue(events, mentions):
    """
    events x mentions -> the `articles` rows, as a distributed join.

    Mirrors gold._article_row: the headline is the enriched title falling back to
    the URL, and duplicates are dropped per (event, headline) so a syndicated
    story doesn't appear several times on one card.
    """
    ev = events.select(
        "GLOBALEVENTID", "Day", "EventCode", "GoldsteinScale", "Actor1Name",
        "ActionGeo_FullName", "ActionGeo_CountryCode",
        "ActionGeo_Lat", "ActionGeo_Long",
        "Actor1CountryCode", "Actor2CountryCode",
        "Actor1Geo_CountryCode", "Actor2Geo_CountryCode",
    )
    mn = mentions.select(
        "GLOBALEVENTID", "MentionIdentifier", "InRawText", "Confidence",
        "MentionDocTone", "article_title", "article_keywords", "enriched",
        "MentionTimeDate",
    ).filter(F.col("MentionIdentifier").isNotNull() & (F.trim("MentionIdentifier") != ""))

    df = mn.join(ev, on="GLOBALEVENTID", how="inner")

    event_date = F.to_date(F.col("Day").cast("string"), "yyyyMMdd")
    headline = F.when(F.trim(F.coalesce("article_title", F.lit(""))) != "",
                      F.trim("article_title")).otherwise(F.col("MentionIdentifier"))
    # "Beijing, ..., China" -> "China"
    country = F.trim(F.element_at(F.split(F.col("ActionGeo_FullName"), ","), -1))

    df = (
        df.withColumn("document_identifier", F.col("MentionIdentifier"))
          .withColumn("doc_id", doc_id_udf(F.col("MentionIdentifier")))
          .withColumn("mention_identifier", headline)
          .withColumn("global_event_id", F.col("GLOBALEVENTID").cast("string"))
          .withColumn("in_raw_text", F.col("InRawText").cast("int"))
          .withColumn("confidence", F.col("Confidence").cast("int"))
          .withColumn("mention_doc_tone", F.col("MentionDocTone").cast("double"))
          .withColumn("country", F.when(country.isNotNull() & (country != ""), country)
                                  .otherwise(F.col("ActionGeo_CountryCode")))
          .withColumn("risk_category", F.lit(""))
          .withColumn("goldstein", F.col("GoldsteinScale").cast("double"))
          .withColumn("cameo_code", F.col("EventCode").cast("string"))
          .withColumn("cameo_label", F.lit(""))
          .withColumn("actor", F.col("Actor1Name"))
          .withColumn("latitude", F.col("ActionGeo_Lat").cast("double"))
          .withColumn("longitude", F.col("ActionGeo_Long").cast("double"))
          .withColumn("event_date", event_date)
          .withColumn("age_days", F.datediff(F.current_date(), event_date))
          # The ARTICLE's own timestamp; event_date above is per-EVENT and so
          # identical across a card. Mirrors gold._mention_time().
          .withColumn("mention_time",
                      F.to_timestamp(F.col("MentionTimeDate").cast("string"),
                                     "yyyyMMddHHmmss"))
    )

    # ONE de-duplication, on (doc_id, global_event_id) — the true grain, matching
    # silver's gdelt_mentions ORDER BY key. Collapsing on doc_id alone, which this
    # used to do, kept one arbitrary event and discarded the rest: 51.8% of URLs
    # on the shipped seed mention more than one event, one of them 64.
    #
    # There is deliberately NO title de-duplication here any more. It used to sit
    # on this line, keyed (global_event_id, _title_key), and it was WRONG in a way
    # that only showed up as drifting totals (566 -> 567 articles across identical
    # runs). The reason is ordering: this runs on the SHARED catalogue, BEFORE any
    # user predicate. Two rows can share an event and a headline while differing in
    # MentionIdentifier and article_keywords — and those are exactly the fields the
    # keyword predicate reads. So whichever row `dropDuplicates` happened to keep
    # decided whether a user matched at all. Same group count, different result.
    #
    # Syndication still gets collapsed, in the right place: the serving layer does
    # it per card, where "one headline per card" is actually meaningful —
    # postgres_store._build_event_card -> _sort_and_cap -> _dedupe_by_title.
    df = df.dropDuplicates(["doc_id", "global_event_id"])
    return df


def user_predicate(profile: dict):
    """
    One user's filter as a Spark Column. This is the ONLY implementation of the
    per-user predicate: the SQL builders it was once mirrored from
    (clickhouse_writer._build_geo_clause, processor.build_keyword_clause) belonged
    to the removed pandas path and have been deleted, precisely so the two cannot
    drift apart again.
      geo:      CAMEO actor codes OR FIPS geo codes
      keywords: URL substring (normalised) OR enriched title/keywords
      the two sides are ANDed; an empty side is no constraint.
    """
    cameo, fips = countries.codes_for_names(profile.get("territories", []))

    geo = F.lit(True)
    clauses = []
    if cameo:
        cam = list(cameo)
        clauses.append(F.col("Actor1CountryCode").isin(cam) | F.col("Actor2CountryCode").isin(cam))
    if fips:
        fp = list(fips)
        clauses.append(F.col("ActionGeo_CountryCode").isin(fp)
                       | F.col("Actor1Geo_CountryCode").isin(fp)
                       | F.col("Actor2Geo_CountryCode").isin(fp))
    if clauses:
        geo = clauses[0]
        for c in clauses[1:]:
            geo = geo | c

    raw_keywords = []
    for values in (profile.get("keywords") or {}).values():
        raw_keywords.extend(str(v).strip() for v in (values or []) if str(v).strip())

    kw = F.lit(True)
    if raw_keywords:
        url_variants: set[str] = set()
        for k in raw_keywords:
            url_variants |= normalize_keyword(k)
        token_groups = [t for t in (tokenize_keyword_enriched(k) for k in raw_keywords) if t]

        parts = []
        # The URL, for every row: match any normalised variant of a keyword.
        if url_variants:
            url_hit = F.lit(False)
            for v in sorted(url_variants):
                url_hit = url_hit | F.lower(F.col("MentionIdentifier")).contains(v)
            parts.append(url_hit)

        # The enriched text, for every row: the row's title and keywords are
        # tokenised once and stemmed by the same rule as stem_token(), then each
        # keyword requires ALL of its tokens to be present.
        # `enriched` is deliberately NOT consulted — routing each row to a single
        # field (URL *or* text, by whether it was enriched) discarded real matches,
        # because an enriched row can still match on its URL.
        if token_groups:
            row_tokens = (
                "array_distinct(transform("
                "  split(lower(concat_ws(' ', article_title, article_keywords)), '[^a-z0-9]+'),"
                "  t -> CASE WHEN length(t) > 3 AND right(t, 1) = 's'"
                "            THEN substring(t, 1, length(t) - 1) ELSE t END))"
            )
            text_hit = F.lit(False)
            for toks in token_groups:
                stems = [stem_token(t) for t in toks]
                arr = "array(" + ", ".join(
                    "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'" for s in stems
                ) + ")"
                text_hit = text_hit | F.expr(
                    f"forall({arr}, s -> array_contains({row_tokens}, s))"
                )
            parts.append(text_hit)

        if parts:
            kw = parts[0]
            for p in parts[1:]:
                kw = kw | p

    return geo & kw


ARTICLE_COLUMNS = [
    "doc_id", "document_identifier", "mention_identifier", "global_event_id",
    "in_raw_text", "confidence", "mention_doc_tone", "country", "risk_category",
    "goldstein", "cameo_code", "cameo_label", "actor", "latitude", "longitude",
    "event_date", "age_days", "mention_time",
]


def write_gold(df, table: str, mode: str = "append", truncate: bool = False) -> None:
    """
    Distributed JDBC write: one connection per partition.

    truncate=True makes `overwrite` empty the table instead of DROPping it, so
    the stage tables keep the column types declared in _STAGE_DDL (BYTEA for
    doc_id in particular — Spark would otherwise invent its own).
    """
    writer = (df.repartition(WRITE_PARTITIONS)
                .write.format("jdbc")
                .option("url", PG_URL)
                .option("dbtable", table)
                .option("user", PG_USER)
                .option("password", PG_PASS)
                .option("driver", "org.postgresql.Driver")
                .option("batchsize", 5000))
    if truncate:
        writer = writer.option("truncate", "true")
    writer.mode(mode).save()


# ── Publish: stage -> live, with the pandas path's exact semantics ───────────
# postgres_writer.write_articles      = upsert on doc_id (never deletes)
# postgres_writer.write_user_articles = DELETE this user's rows, then INSERT
#
# ON CONFLICT would raise "cannot affect row a second time" if articles_stage
# held two rows with the same doc_id — but it cannot: the DataFrame is
# dropDuplicates(["doc_id"]) before it is staged. Oracle's MERGE had the same
# requirement (ORA-30926 otherwise), so this is not a new constraint.
_ARTICLE_UPSERT_COLUMNS = [
    "doc_id", "document_identifier", "mention_identifier", "global_event_id",
    "in_raw_text", "confidence", "mention_doc_tone", "country", "risk_category",
    "goldstein", "cameo_code", "cameo_label", "actor", "latitude", "longitude",
    "event_date", "age_days", "mention_time",
]

_UPSERT_ARTICLES = (
    "INSERT INTO articles ({cols}) SELECT {cols} FROM articles_stage "
    "ON CONFLICT (doc_id, global_event_id) DO UPDATE SET {sets}"
).format(
    cols=", ".join(_ARTICLE_UPSERT_COLUMNS),
    sets=", ".join(f"{c} = EXCLUDED.{c}" for c in _ARTICLE_UPSERT_COLUMNS
                   if c not in ("doc_id", "global_event_id")),
)


def read_pipeline_status() -> str:
    """The state written by the validation layer; 'OK' when absent/unreadable."""
    if not STATUS_FILE.exists():
        return "OK"
    try:
        return json.loads(STATUS_FILE.read_text(encoding="utf-8")).get("state", "OK")
    except (json.JSONDecodeError, OSError):
        return "OK"


# The stage tables are created by the job itself and dropped again once the rows
# have been published, so nothing has to be pre-created in the database and no
# scratch tables are left behind by a successful run.
#
# Each one carries a table COMMENT, so anyone browsing the schema (or a tool
# listing it) can see what these two odd tables are and that they are safe to
# drop, without having to find this file.
_STAGE_COMMENT = (
    "TRANSIENT scratch table for the distributed silver-to-gold job "
    "(4-processing/spark_gold.py). Dropped and recreated at the start of every "
    "run, and dropped again after a successful publish; rows are only ever left "
    "here by a FAILED run, kept for inspection. Not read by the application - "
    "safe to drop at any time."
)

# Types mirror postgres-init/01_schema.sql. TIMESTAMP rather than DATE for the
# two time columns, because PostgreSQL's DATE holds no time-of-day and
# mention_time drives the card ordering.
_STAGE_DDL = {
    "articles_stage": """
        CREATE TABLE articles_stage (
          doc_id BYTEA, document_identifier VARCHAR(2000),
          mention_identifier VARCHAR(2000), global_event_id VARCHAR(50),
          in_raw_text SMALLINT, confidence SMALLINT,
          mention_doc_tone DOUBLE PRECISION,
          country VARCHAR(200), risk_category VARCHAR(500),
          goldstein DOUBLE PRECISION,
          cameo_code VARCHAR(10), cameo_label VARCHAR(200), actor VARCHAR(500),
          latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
          event_date TIMESTAMP, age_days SMALLINT,
          mention_time TIMESTAMP)
    """,
    "user_articles_stage": """
        CREATE TABLE user_articles_stage (
          user_id VARCHAR(200), doc_id BYTEA, global_event_id VARCHAR(50))
    """,
}


def _connect_postgres():
    import psycopg
    return psycopg.connect(PG_DSN)


def recreate_stage_tables() -> None:
    """
    Drop and recreate the scratch tables at the start of every run.

    Dropping first (rather than reusing whatever is there) guarantees the columns
    are exactly the ones declared above. If a previous run, a manual edit or a
    Spark drop-and-recreate had left a table with different types — doc_id as a
    BLOB instead of RAW(32), say — reusing it would fail at write time or store
    the wrong thing; here it is simply replaced.

    Rows left behind by a failed run therefore survive until the NEXT run starts,
    which is the window in which they are useful for inspection.
    """
    from psycopg import sql

    with _connect_postgres() as conn:
        cur = conn.cursor()
        for name, ddl in _STAGE_DDL.items():
            # DROP TABLE IF EXISTS replaces the Oracle version's catch-and-inspect
            # of ORA-00942 ("table or view does not exist"). That matters for more
            # than tidiness here: in PostgreSQL a failed statement aborts the
            # whole transaction, so swallowing an error and carrying on would not
            # have worked — every later statement would fail too.
            cur.execute(f"DROP TABLE IF EXISTS {name}")
            cur.execute(ddl)
            # COMMENT ON accepts no query parameters, so the text has to be a
            # literal in the statement; sql.Literal quotes it properly rather
            # than by hand-rolled escaping.
            cur.execute(sql.SQL("COMMENT ON TABLE {} IS {}").format(
                sql.Identifier(name), sql.Literal(_STAGE_COMMENT)))
            logger.info("created stage table %s", name)
        conn.commit()


def drop_stage_tables() -> None:
    """Remove the scratch tables after a successful publish."""
    with _connect_postgres() as conn:
        cur = conn.cursor()
        for name in _STAGE_DDL:
            try:
                cur.execute(f"DROP TABLE IF EXISTS {name}")
            except Exception as exc:  # noqa: BLE001
                logger.warning("could not drop %s: %s", name, exc)
        conn.commit()
    logger.info("stage tables dropped")


def publish(processed_uids: list[str]) -> None:
    """
    Move the staged rows into the live tables, in ONE transaction, reproducing
    exactly what the pandas path does:

      articles        — MERGE (upsert) on doc_id, then any row no user still
                        references is deleted, so the table cannot accumulate
                        unreachable rows.
      user_articles   — every processed user's rows are deleted and re-inserted,
                        including users who matched nothing this run (so their old
                        set is cleared), which is what write_user_articles([]) did.
      pipeline_status — replaced with the single row mirroring the validation
                        layer's status file, as write_pipeline_status did.
    """
    with _connect_postgres() as conn:
        cur = conn.cursor()
        cur.execute(_UPSERT_ARTICLES)
        logger.info("articles: upserted %d staged rows", cur.rowcount)

        if processed_uids:
            cur.execute(
                "DELETE FROM user_articles WHERE user_id = ANY(%(uids)s)",
                {"uids": list(processed_uids)})
            deleted = cur.rowcount
            cur.execute(
                "INSERT INTO user_articles (user_id, doc_id, global_event_id) "
                "SELECT user_id, doc_id, global_event_id FROM user_articles_stage")
            logger.info("user_articles: replaced %d rows with %d", deleted, cur.rowcount)

        # Purge orphans AFTER user_articles has been rebuilt, so the anti-join
        # sees the new sets. Mirrors postgres_writer.delete_orphan_articles(), and
        # is inside the same transaction as everything else here.
        #
        # Tracked events are protected for the same reason as in the pandas path:
        # the serving layer reads needs-action / monitoring cards from `articles`
        # WITHOUT joining user_articles, so those rows are legitimately
        # unreferenced and must survive. ARCHIVED events are not protected —
        # archiving says the event does not matter. Skipping the sweep is the
        # safe failure.
        sweep = ("DELETE FROM articles a WHERE NOT EXISTS "
                 "(SELECT 1 FROM user_articles ua WHERE ua.doc_id = a.doc_id "
                 "   AND ua.global_event_id = a.global_event_id)")
        params: dict = {}
        try:
            import mongo_reader
            protected = sorted(mongo_reader.get_protected_event_ids())
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping orphan sweep: could not read tags (%s)", exc)
            protected = None

        if protected is None:
            pass                                    # sweep skipped entirely
        else:
            if protected:
                # One array parameter for the whole set. Oracle needed it split
                # into OR-ed 900-entry IN lists because its IN list caps at 1000.
                sweep += " AND NOT (a.global_event_id = ANY(%(protected)s))"
                params["protected"] = [str(p) for p in protected]
            cur.execute(sweep, params)
            if cur.rowcount:
                logger.info("articles: removed %d orphaned rows (%d triaged "
                            "events protected)", cur.rowcount, len(protected))

        state = read_pipeline_status()
        cur.execute("DELETE FROM pipeline_status")
        cur.execute(
            "INSERT INTO pipeline_status (status, timestamp_of_last_update) "
            "VALUES (%(s)s, %(t)s)",
            # Naive UTC, to match postgres_writer: an aware value written to a
            # TIMESTAMP (without time zone) column is converted using the
            # session's TimeZone and the offset then dropped, so the stored
            # instant would depend on server configuration.
            {"s": state,
             "t": datetime.now(timezone.utc).replace(tzinfo=None)},
        )
        logger.info("pipeline_status set to %s", state)
        conn.commit()


def recompute(only_user: str | None = None) -> dict:
    """
    Rebuild the gold layer from silver. THE silver -> gold path, in both modes.

    `only_user` is what the MongoDB change-stream trigger uses: when a single
    profile is edited there is no reason to re-evaluate everyone, so only that
    user's predicate is run and only that user's `user_articles` rows are
    replaced. The semantics then match what the in-process path used to do in
    recompute_user(): their articles are upserted (never deleted), their links
    are rebuilt from scratch, and the orphan sweep runs afterwards.

    Returns a summary dict so the caller — the FastAPI routes and the triggers —
    can log and report without reaching into Spark.

    The session is NOT stopped here: it is process-wide and reused (see _spark).
    """
    import mongo_reader
    import postgres_writer

    # Bring the gold schema up to date BEFORE anything is staged. This used to be
    # reached only through postgres_writer.write_articles(), which the Spark path
    # never calls — it writes over JDBC and publishes with raw SQL — so on a
    # database created before the key changed, `user_articles` still lacked
    # global_event_id and publish() failed on every run. The recompute then
    # retried on the next watermark poll, giving a silent ~4-minute failure loop
    # in which gold was never updated.
    postgres_writer.ensure_schema()

    spark = _spark()
    events = read_partitioned(spark, "gdelt_events")
    mentions = read_partitioned(spark, "gdelt_mentions")
    if events is None or mentions is None:
        logger.warning("silver is empty — nothing to do")
        return {"users": 0, "articles": 0, "status": "EMPTY"}

    catalogue = build_catalogue(events, mentions).cache()
    logger.info("catalogue built: %d candidate articles", catalogue.count())

    # Per-user sets: each predicate is evaluated across the cluster.
    processed_uids: list[str] = []
    per_user = None
    for profile in mongo_reader.get_all_profiles():
        uid = str(profile.get("_id") or profile.get("user_id") or "")
        if not uid or (only_user is not None and uid != only_user):
            continue
        processed_uids.append(uid)
        hits = (catalogue.filter(user_predicate(profile))
                         .select(F.lit(uid).alias("user_id"), "doc_id",
                                 "global_event_id")
                         # Pair-scoped: the same article reaching this user
                         # through two events is TWO rows, one per card.
                         .dropDuplicates(["doc_id", "global_event_id"]))
        per_user = hits if per_user is None else per_user.unionByName(hits)

    if per_user is None:
        # Reached only when NO PROFILE was selected — there are none at all, or
        # `only_user` names one that does not exist. Distinct from a profile that
        # matches nothing: that leaves `hits` as an EMPTY DataFrame rather than
        # None, so the run continues and publish() clears that user's rows and
        # inserts none. That is deliberate and matches what the in-process path
        # did with write_user_articles(uid, []) — a user who narrows their
        # preferences until nothing matches must end up with an empty pool, not
        # a stale one.
        logger.warning("no matching user profile%s — nothing to do",
                       f" for {only_user}" if only_user else "s")
        return {"users": 0, "articles": 0, "status": "NO_PROFILES"}
    per_user = per_user.cache()

    # `articles` holds only documents at least one processed user receives — the
    # same union the in-process path accumulated in its `catalog` dict.
    matched_ids = (per_user.select("doc_id", "global_event_id")
                           .dropDuplicates(["doc_id", "global_event_id"]))
    articles = catalogue.join(matched_ids, on=["doc_id", "global_event_id"],
                              how="inner")

    recreate_stage_tables()
    write_gold(articles.select(*ARTICLE_COLUMNS), "articles_stage",
                 mode="overwrite", truncate=True)
    write_gold(per_user.select("user_id", "doc_id", "global_event_id"), "user_articles_stage",
                 mode="overwrite", truncate=True)
    n_articles = articles.count()
    logger.info("staged %d articles for %d user(s)", n_articles, len(processed_uids))

    publish(processed_uids)
    # Only dropped once the rows are safely published; if publish() raised, the
    # stage tables are left behind on purpose so the run can be inspected.
    drop_stage_tables()
    logger.info("published — articles, user_articles and pipeline_status are live")
    return {"users": len(processed_uids), "articles": n_articles, "status": "OK"}


def main() -> None:
    """Entry point for a one-shot `spark-submit` (the manual/batch route)."""
    try:
        recompute()
    finally:
        # A submitted job owns its session and must release it; the resident
        # service does not, which is why this lives here and not in recompute().
        SparkSession.builder.getOrCreate().stop()


if __name__ == "__main__":
    main()
