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
  * WRITE — df.write.jdbc() opens one connection PER PARTITION, so the Oracle
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
    CLICKHOUSE_HOST/PORT, ORACLE_HOST/PORT/SERVICE/USER/PASSWORD,
    MONGO_URI/MONGO_DB/MONGO_COLLECTION
    SPARK_MASTER            default spark://spark-master:7077
    SPARK_READ_PARTITIONS   parallel JDBC readers  (default 8)
    SPARK_WRITE_PARTITIONS  parallel Oracle writers(default 4)

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
CH_PORT = os.getenv("CLICKHOUSE_PORT", "8123")          # JDBC uses the HTTP port
CH_DB   = os.getenv("CLICKHOUSE_DATABASE", "default")

OR_HOST = os.getenv("ORACLE_HOST", "pipeline_oracle")
OR_PORT = os.getenv("ORACLE_PORT", "1521")
OR_SVC  = os.getenv("ORACLE_SERVICE", "FREEPDB1")
OR_USER = os.getenv("ORACLE_USER", "radar")
OR_PASS = os.getenv("ORACLE_PASSWORD", "radar")

READ_PARTITIONS  = int(os.getenv("SPARK_READ_PARTITIONS", "8"))
WRITE_PARTITIONS = int(os.getenv("SPARK_WRITE_PARTITIONS", "4"))

# The global status file the validation layer writes on the shared volume. The
# Spark container mounts that volume read-only so it can mirror the value into
# Oracle, exactly as the pandas path's recompute_all does.
STATUS_FILE = Path(os.getenv("STATUS_DIR", "/data/status")) / "pipeline_status.json"

CH_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}"
OR_URL = f"jdbc:oracle:thin:@//{OR_HOST}:{OR_PORT}/{OR_SVC}"


# ── UDF: the Oracle primary key ──────────────────────────────────────────────
# Same definition as oracle_writer._doc_id, so both paths address the same rows.
def _doc_id(url: str):
    if url is None:
        return None
    return hashlib.sha256(url.encode("utf-8")).digest()


doc_id_udf = F.udf(_doc_id, BinaryType())


def _spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("radar-silver-to-gold")
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
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

    # De-duplicate: one row per URL, then one per (event, normalised headline).
    df = df.dropDuplicates(["doc_id"])
    df = df.withColumn("_title_key", F.lower(F.regexp_replace(F.trim("mention_identifier"), r"\s+", " ")))
    df = df.dropDuplicates(["global_event_id", "_title_key"]).drop("_title_key")
    return df


def user_predicate(profile: dict):
    """
    One user's filter as a Spark Column — the same semantics as
    clickhouse_writer._build_geo_clause + processor.build_keyword_clause:
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
        # The URL, for every row — mirrors the LIKE branch of build_keyword_clause.
        if url_variants:
            url_hit = F.lit(False)
            for v in sorted(url_variants):
                url_hit = url_hit | F.lower(F.col("MentionIdentifier")).contains(v)
            parts.append(url_hit)

        # The enriched text, for every row. Mirrors build_keyword_clause exactly:
        # the row's title and keywords are tokenised once and stemmed by the same
        # rule as stem_token(), then each keyword requires ALL of its tokens.
        # `enriched` is deliberately NOT consulted — see build_keyword_clause for
        # why routing each row to a single field discarded real matches.
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


def write_oracle(df, table: str, mode: str = "append", truncate: bool = False) -> None:
    """
    Distributed JDBC write: one connection per partition.

    truncate=True makes `overwrite` empty the table instead of DROPping it, so
    the stage tables keep the column types declared in oracle-init (RAW(32) for
    doc_id in particular — Spark would otherwise invent its own).
    """
    writer = (df.repartition(WRITE_PARTITIONS)
                .write.format("jdbc")
                .option("url", OR_URL)
                .option("dbtable", table)
                .option("user", OR_USER)
                .option("password", OR_PASS)
                .option("driver", "oracle.jdbc.OracleDriver")
                .option("batchsize", 5000))
    if truncate:
        writer = writer.option("truncate", "true")
    writer.mode(mode).save()


# ── Publish: stage -> live, with the pandas path's exact semantics ───────────
# oracle_writer.write_articles      = MERGE on doc_id (upsert, never deletes)
# oracle_writer.write_user_articles = DELETE this user's rows, then INSERT
_MERGE_ARTICLES = """
MERGE INTO articles t
USING articles_stage s ON (t.doc_id = s.doc_id)
WHEN MATCHED THEN UPDATE SET
    t.document_identifier = s.document_identifier,
    t.mention_identifier  = s.mention_identifier,
    t.global_event_id     = s.global_event_id,
    t.in_raw_text         = s.in_raw_text,
    t.confidence          = s.confidence,
    t.mention_doc_tone    = s.mention_doc_tone,
    t.country             = s.country,
    t.risk_category       = s.risk_category,
    t.goldstein           = s.goldstein,
    t.cameo_code          = s.cameo_code,
    t.cameo_label         = s.cameo_label,
    t.actor               = s.actor,
    t.latitude            = s.latitude,
    t.longitude           = s.longitude,
    t.event_date          = s.event_date,
    t.age_days            = s.age_days,
    t.mention_time        = s.mention_time
WHEN NOT MATCHED THEN INSERT
    (doc_id, document_identifier, mention_identifier, global_event_id, in_raw_text,
     confidence, mention_doc_tone, country, risk_category, goldstein, cameo_code,
     cameo_label, actor, latitude, longitude, event_date, age_days, mention_time)
VALUES
    (s.doc_id, s.document_identifier, s.mention_identifier, s.global_event_id,
     s.in_raw_text, s.confidence, s.mention_doc_tone, s.country, s.risk_category,
     s.goldstein, s.cameo_code, s.cameo_label, s.actor, s.latitude, s.longitude,
     s.event_date, s.age_days, s.mention_time)
"""


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
# Each one carries an Oracle table COMMENT, so anyone browsing the schema (or a
# tool listing it) can see what these two odd tables are and that they are safe
# to drop, without having to find this file.
_STAGE_COMMENT = (
    "TRANSIENT scratch table for the distributed silver-to-gold job "
    "(4-processing/spark_gold.py). Dropped and recreated at the start of every "
    "run, and dropped again after a successful publish; rows are only ever left "
    "here by a FAILED run, kept for inspection. Not read by the application - "
    "safe to drop at any time."
)

_STAGE_DDL = {
    "articles_stage": """
        CREATE TABLE articles_stage (
          doc_id RAW(32), document_identifier VARCHAR2(2000),
          mention_identifier VARCHAR2(2000), global_event_id VARCHAR2(50),
          in_raw_text NUMBER(1), confidence NUMBER(3), mention_doc_tone FLOAT,
          country VARCHAR2(200), risk_category VARCHAR2(500), goldstein FLOAT,
          cameo_code VARCHAR2(10), cameo_label VARCHAR2(200), actor VARCHAR2(500),
          latitude FLOAT, longitude FLOAT, event_date DATE, age_days NUMBER(4),
          mention_time DATE)
    """,
    "user_articles_stage": """
        CREATE TABLE user_articles_stage (
          user_id VARCHAR2(200), doc_id RAW(32))
    """,
}


def _connect_oracle():
    import oracledb
    return oracledb.connect(user=OR_USER, password=OR_PASS,
                            dsn=f"{OR_HOST}:{OR_PORT}/{OR_SVC}")


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
    with _connect_oracle() as conn:
        cur = conn.cursor()
        for name, ddl in _STAGE_DDL.items():
            try:
                cur.execute(f"DROP TABLE {name}")
                logger.info("dropped pre-existing stage table %s", name)
            except Exception as exc:  # noqa: BLE001
                if "ORA-00942" not in str(exc):   # "table or view does not exist"
                    raise
            cur.execute(ddl)
            cur.execute(f"COMMENT ON TABLE {name} IS :c", c=_STAGE_COMMENT)
            logger.info("created stage table %s", name)
        conn.commit()


def drop_stage_tables() -> None:
    """Remove the scratch tables after a successful publish."""
    with _connect_oracle() as conn:
        cur = conn.cursor()
        for name in _STAGE_DDL:
            try:
                cur.execute(f"DROP TABLE {name}")
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
    with _connect_oracle() as conn:
        cur = conn.cursor()
        cur.execute(_MERGE_ARTICLES)
        logger.info("articles: merged %d staged rows", cur.rowcount)

        if processed_uids:
            binds = {f"u{n}": u for n, u in enumerate(processed_uids)}
            placeholders = ", ".join(f":{k}" for k in binds)
            cur.execute(
                f"DELETE FROM user_articles WHERE user_id IN ({placeholders})", binds)
            deleted = cur.rowcount
            cur.execute(
                "INSERT INTO user_articles (user_id, doc_id) "
                "SELECT user_id, doc_id FROM user_articles_stage")
            logger.info("user_articles: replaced %d rows with %d", deleted, cur.rowcount)

        # Purge orphans AFTER user_articles has been rebuilt, so the anti-join
        # sees the new sets. Mirrors oracle_writer.delete_orphan_articles(), and
        # is inside the same transaction as everything else here.
        #
        # Triaged events are protected for the same reason as in the pandas path:
        # the serving layer reads needs-action / monitoring / archive cards from
        # `articles` WITHOUT joining user_articles, so those rows are legitimately
        # unreferenced and must survive. Skipping the sweep is the safe failure.
        sweep = ("DELETE FROM articles a WHERE NOT EXISTS "
                 "(SELECT 1 FROM user_articles ua WHERE ua.doc_id = a.doc_id)")
        binds: dict = {}
        try:
            import mongo_reader
            protected = sorted(mongo_reader.get_all_tagged_event_ids())
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping orphan sweep: could not read tags (%s)", exc)
            protected = None

        if protected is None:
            pass                                    # sweep skipped entirely
        else:
            if protected:
                chunks = [protected[i:i + 900] for i in range(0, len(protected), 900)]
                clauses = []
                for ci, chunk in enumerate(chunks):
                    names = []
                    for vi, value in enumerate(chunk):
                        key = f"p{ci}_{vi}"
                        binds[key] = value
                        names.append(f":{key}")
                    clauses.append(f"a.global_event_id IN ({', '.join(names)})")
                sweep += " AND NOT (" + " OR ".join(clauses) + ")"
            cur.execute(sweep, binds)
            if cur.rowcount:
                logger.info("articles: removed %d orphaned rows (%d triaged "
                            "events protected)", cur.rowcount, len(protected))

        state = read_pipeline_status()
        cur.execute("DELETE FROM pipeline_status")
        cur.execute(
            "INSERT INTO pipeline_status (status, timestamp_of_last_update) "
            "VALUES (:s, :t)",
            s=state, t=datetime.now(timezone.utc),
        )
        logger.info("pipeline_status set to %s", state)
        conn.commit()


def main() -> None:
    import mongo_reader

    spark = _spark()
    try:
        events = read_partitioned(spark, "gdelt_events")
        mentions = read_partitioned(spark, "gdelt_mentions")
        if events is None or mentions is None:
            logger.warning("silver is empty — nothing to do")
            return

        catalogue = build_catalogue(events, mentions).cache()
        logger.info("catalogue built: %d candidate articles", catalogue.count())

        # Per-user sets: each predicate is evaluated across the cluster.
        processed_uids: list[str] = []
        per_user = None
        for profile in mongo_reader.get_all_profiles():
            uid = str(profile.get("_id") or profile.get("user_id") or "")
            if not uid:
                continue
            processed_uids.append(uid)
            hits = (catalogue.filter(user_predicate(profile))
                             .select(F.lit(uid).alias("user_id"), "doc_id")
                             .dropDuplicates(["doc_id"]))
            per_user = hits if per_user is None else per_user.unionByName(hits)

        if per_user is None:
            logger.warning("no user profiles — nothing to do")
            return
        per_user = per_user.cache()

        # `articles` holds only documents at least one user receives — the same
        # union the pandas path accumulated in its `catalog` dict.
        matched_ids = per_user.select("doc_id").dropDuplicates(["doc_id"])
        articles = catalogue.join(matched_ids, on="doc_id", how="inner")

        recreate_stage_tables()
        write_oracle(articles.select(*ARTICLE_COLUMNS), "articles_stage",
                     mode="overwrite", truncate=True)
        write_oracle(per_user.select("user_id", "doc_id"), "user_articles_stage",
                     mode="overwrite", truncate=True)
        logger.info("staged %d articles for %d users",
                    articles.count(), len(processed_uids))

        publish(processed_uids)
        # Only dropped once the rows are safely published; if publish() raised,
        # the stage tables are left behind on purpose so the run can be inspected.
        drop_stage_tables()
        logger.info("published — articles, user_articles and pipeline_status are live")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
