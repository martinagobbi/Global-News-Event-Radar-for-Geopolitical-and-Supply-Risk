#!/usr/bin/env python
"""
bootstrap/bulk_load.py — fill the silver layer directly from a folder of GDELT
ZIP files, bypassing the 15-minute-at-a-time pipeline.

Why this exists
---------------
The live pipeline processes ONE 15-minute slice at a time: parsing publishes a
pair only when `latest_files` is empty, and validation then enriches and stores
it. Measured on this project, a slice costs about a minute end to end, so a
30-day backfill (2,880 slices) would take two to four days to appear.

This loader performs exactly the same transformations, but across many slices
at once, so the same 30 days land in silver in minutes rather than days.

It is deliberately faithful to the pipeline it replaces:
    * events   — filtered with 2-parsing/parser.passes_filter (the bronze->silver
                 supply-chain relevance filter)
    * mentions — kept only when their GLOBALEVENTID exists in the events of the
                 same slice (the same referential-integrity rule validation uses)
    * cleaned  — with validation's OWN drop/null functions (drop_rows_missing,
                 clean_confidence, clean_dateadded, clean_goldstein), imported
                 rather than reimplemented, so a rule can never drift between the
                 live pipeline and this loader
    * writes   — through 3-validation_and_storage/storage.Storage, so the schema,
                 the sharding and the ReplacingMergeTree dedup are identical
    * gold     — NOT written here. Once silver grows, the processing layer's own
                 watermark trigger fires and builds the gold exactly as it always
                 does, so the gold can never drift from what the pipeline
                 would have produced.

Enrichment (Newspaper3k article titles/keywords) is OFF by default: it scrapes
every article URL and is by far the slowest step. Turn it on with ENRICH=1 if you
want the enriched fields, and expect the load to take hours instead of minutes.

Progress and timings are appended to the report file so the run can be measured.

── Why PySpark, and what it is doing here ───────────────────────────────────
This is the ONE place in the pipeline where pandas was replaced with PySpark
outside of the actual silver -> gold job. The other pipeline layers (ingestion,
parsing, validation) apply pandas to a SINGLE 15-minute slice — a few thousand
rows, sub-second — where a JVM-backed engine would only add startup cost. This
script processes THOUSANDS of slices in one run, and the slices are completely
independent of each other (see "Why this is safe to parallelize" below), which
is the shape of problem a distributed engine is for.

Unlike 4-processing/spark_gold.py, this is NOT a DataFrame/SQL job. There are no
`spark.read.csv(...)` or `df.filter(...)` calls anywhere below. Spark is used at
its lower level, as a distributed WORK QUEUE: `sc.parallelize()` splits the list
of slices into partitions, and `mapPartitions()` runs an ordinary Python function
— using the exact same pandas calls the single-process version used — once per
partition, each in its own OS process. The "engine change" is entirely about HOW
MANY of those single-process loops run at once and on how many cores; the
transformation logic inside each one is untouched pandas code, imported from the
same modules the live pipeline uses.

That design is deliberate, not a shortcut: reimplementing passes_filter's
keyword/CAMEO-code logic or validator.py's cleaning rules as native Spark
SQL/DataFrame expressions would require translating every rule by hand, and any
mismatch between the pandas original and its Spark restatement would silently
diverge the two paths in exactly the way this project has repeatedly found and
fixed elsewhere. Calling the SAME functions removes that risk by construction.

── Why this is safe to parallelize ───────────────────────────────────────────
Each slice's read -> filter -> clean -> referential-integrity join depends only
on that slice's own two files. Nothing here looks up other slices or the store
(this loader's referential-integrity check is scoped to "this slice's own kept
events", not "this slice's events plus whatever validate_pair() already checked
against ClickHouse" — the live pipeline's validator.py additionally consults
storage.existing_event_ids() for ids not in the current events file; this loader
does not, and that is UNCHANGED by moving to Spark). So slices can run in any
order, on any number of workers, with no coordination between them.

Writes are then safe under Spark's automatic task-retry too, for a reason that
is a property of the ClickHouse SCHEMA rather than of Spark: both gdelt_events
and gdelt_mentions are ReplicatedReplacingMergeTree, keyed so that re-inserting
the same slice's rows collapses to one copy at merge time (events by
GLOBALEVENTID, largest DATEADDED wins; mentions by (GLOBALEVENTID,
MentionIdentifier), enriched=1 wins over 0). If Spark ever retries a failed
partition, re-writing already-written slices in it is a duplicate insert that
the store itself resolves — not silent data loss, and not permanent duplication.

── What is genuinely different from the single-process version ─────────────
1. Progress logging is per-partition, not global. The single loop used to print
   one "n/total slices | ETA" line with true global progress; N partitions each
   print their own "i/mine" line, and Spark's own executor/task log lines are
   interleaved with them. The final DONE summary and the report file are still
   single, driver-side, aggregated lines with the exact same format as before.
2. Concurrent ClickHouse writes. Each partition opens its own Storage connection
   and flushes its own batch every BATCH_SLICES, so up to SPARK_BULK_LOAD_PARTITIONS
   partitions can be inserting at once instead of one process inserting
   sequentially. This is within what the cluster is built for (see
   CLICKHOUSE_INSERT_QUORUM), but it is a real change from "one writer".
3. Concurrent enrichment. If ENRICH=1, EVERY partition that has enrichable
   mentions opens its OWN MENTION_ENRICH_WORKERS-sized thread pool of scrapers.
   Total concurrent HTTP fetches against article source sites is therefore
   MENTION_ENRICH_WORKERS x (partitions doing enrichment right now), not
   MENTION_ENRICH_WORKERS as before. Lower MENTION_ENRICH_WORKERS if raising
   SPARK_BULK_LOAD_PARTITIONS, unless the larger number is actually what you want.
4. A JVM is now part of this container. See bootstrap/Dockerfile: the base image
   changed from python:3.11-slim to the same bitnamilegacy/spark:3.5 image
   4-processing already uses, which is where the pyspark package comes from —
   it ships with that image and is deliberately not pinned in requirements.txt.

Usage (see docker-compose.bootstrap.yml):
    docker compose -f docker-compose.bootstrap.yml run --rm bootstrap

Useful settings (on top of the ones the pandas version already had):
    SPARK_MASTER                 local[*] (default, both modes work unmodified —
                                  see the module-level note above SPARK_MASTER)
    SPARK_BULK_LOAD_PARTITIONS   how many slices run at once (default 4)
"""

import io
import logging
import os
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import warnings
from dateutil.parser import UnknownTimezoneWarning

# Newspaper3k (from `enrichment`, imported below) will extract dates
# even if we don't want it to (GDELT already provides dates).
# Sometimes, Newspaper3k can't extract a date due to time zone ambiguity.
# Let's suppress that warning: it's for a problem we won't even have.
warnings.filterwarnings("ignore", category=UnknownTimezoneWarning)
# When dateutil makes this an exception instead of a warning,
# the error message I get may change, but Newspaper3k should still be able
# to give me the info I will actually save. At that point, a different
# "swallow this message" line will have to be added here.

import pandas as pd
from pyspark import SparkConf
from pyspark.sql import SparkSession

sys.path.insert(0, "/app/parsing")
sys.path.insert(0, "/app/validation")

# Below, extra steps are being taken to make absolutely sure that bulk load
# applies the cleaning made by the validation layer.
from parser import (passes_filter, GDELT_COLUMNS, MENTIONS_COLUMNS,
                    PARSED_EVENT_COLUMNS, PARSED_MENTION_COLUMNS)
from gdelt import (EVENT_ID, EVENT_COLUMNS, MENTION_COLUMNS,
                   check_field_width, RAW_EXPECTED_FIELD_COUNT)
from validator import drop_rows_missing, clean_dateadded, clean_goldstein, clean_confidence
from storage import Storage                                          # 3-validation

# The frames are read with parsing's names (which passes_filter expects) and then
# renamed positionally to validation's names before being written, even though both
# layers should agree on all names anyways.

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("bulk_load")

SOURCE_DIR = Path(os.getenv("SOURCE_DIR", "/source"))
REPORT_FILE = Path(os.getenv("REPORT_DIR", "/report")) / "bulk_load_report.csv"
ENRICH = os.getenv("ENRICH", "0") == "1"
BATCH_SLICES = int(os.getenv("BATCH_SLICES", "50"))
LIMIT_SLICES = int(os.getenv("LIMIT_SLICES", "0"))    # 0 = no limit

# ── SPARK_MASTER ──────────────────────────────────────────────────────────────
# Same env var, same meaning, as 4-processing/spark_gold.py — but a DIFFERENT
# default. spark_gold.py defaults to the cluster URL because it is meant to run
# resident, inside the processing service, in whichever mode that service is
# deployed in. This script is a one-shot tool most often reached for on a single
# operator machine (that is what "bootstrap" means here), so it defaults to
# in-process local[*] and only distributes across real worker machines if you
# explicitly point it at the cluster — see the Dockerfile/dependency note below
# for why that is not yet a complete story.
#
# Genuinely trying SPARK_MASTER=spark://spark-master:7077 today will FAIL, and
# it is worth knowing why rather than being surprised by it: this script's
# per-slice work calls into parser.py / gdelt.py / validator.py / storage.py /
# enrichment.py, all copied into THIS image (see bootstrap/Dockerfile) along
# with pandas, clickhouse-driver, newspaper3k, nltk and lxml. The existing
# spark-worker service in docker-stack.pipeline.yml runs the generic
# bitnamilegacy/spark:3.5 image with NONE of that — spark_gold.py never needs it,
# because its per-user predicate is built as Spark SQL expressions, not calls
# into this project's own Python modules. Making a REAL multi-machine run of
# THIS script possible would mean building spark-worker from an image that also
# carries this dependency set (or shipping this file's dependencies to the
# workers some other way) — a docker-stack.pipeline.yml change that needs actual
# worker machines to build and test, so it is intentionally not done here.
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# How many slices are processed AT ONCE. Each one becomes a Spark partition,
# processed by its own worker process, so this is the real parallelism knob —
# SPARK_WORKER_CORES/SPARK_WORKERS govern the resident silver -> gold job, not
# this script, which is why this gets its own variable. Default is deliberately
# modest: measured on this project's own dev machine (8 cores, 6.8 GiB), the
# full testing stack alone already holds ~4.5 GiB, and each partition opens its
# own ClickHouse connection and (if ENRICH=1) its own thread pool of scrapers —
# see the module docstring's point 2 and 3. Raise it once you have confirmed
# there is headroom for that many concurrent writers/scrapers, not before.
SPARK_PARTITIONS = int(os.getenv("SPARK_BULK_LOAD_PARTITIONS", "4"))

EVENTS_SUFFIX = ".export.CSV"
MENTIONS_SUFFIX = ".mentions.CSV"

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-s1r1")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_DB   = os.getenv("CLICKHOUSE_DATABASE", "default")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "")

# Per-slice tuning for enrichment, read once here rather than inside every
# worker process re-reading os.environ per slice.
_ENRICH_WORKERS = int(os.getenv("MENTION_ENRICH_WORKERS", "8"))
_ENRICH_NLP = os.getenv("MENTION_ENRICH_NLP", "0") == "1"
_ENRICH_TIMEOUT = int(os.getenv("ENRICH_TIMEOUT_SECONDS", "60"))


def find_slices() -> dict:
    """
    Map slice-id -> {"events": path, "mentions": path} for every GDELT ZIP found
    anywhere under SOURCE_DIR (the release archive nests them in a sub-folder,
    so the search is recursive).
    """
    slices: dict = defaultdict(dict)
    for path in SOURCE_DIR.rglob("*.zip"):
        name = path.name
        if name.startswith("."):
            continue
        if name.endswith(".export.CSV.zip"):
            slices[name[: -len(".export.CSV.zip")]]["events"] = path
        elif name.endswith(".mentions.CSV.zip"):
            slices[name[: -len(".mentions.CSV.zip")]]["mentions"] = path
    return slices


def read_zip(path: Path, columns: list) -> pd.DataFrame:
    """
    Read the single CSV inside a GDELT ZIP as a header-less, all-string frame.

    Width-checked first, and this site needs it as much as parsing does: the
    loader reads GDELT archives DIRECTLY, never through 2-parsing, so nothing
    upstream has normalised the file. It also writes thousands of slices in one
    run, so an unnoticed column shift here would corrupt a whole backfill rather
    than a single 15-minute slice.

    UNCHANGED from the pandas version, including which engine reads the CSV: this
    is still plain pandas, called from inside a Spark worker process rather than
    from the driver's own loop. Reimplementing this with Spark's native CSV
    reader was deliberately avoided — pandas and Spark do not parse tab-separated
    text identically at the edges (empty-field vs. null handling, whitespace,
    quoting), and that gap is exactly the kind of silent drift this loader exists
    to avoid. See the module docstring's "Why PySpark" section.
    """
    with zipfile.ZipFile(path) as zf:
        member = zf.namelist()[0]
        raw = zf.read(member)
        kind = "events" if len(columns) >= 61 else "mentions"
        check_field_width(raw, RAW_EXPECTED_FIELD_COUNT[kind], kind, path)
        return pd.read_csv(io.BytesIO(raw), sep="\t", header=None, names=columns,
                           dtype=str, keep_default_na=False, low_memory=False)


def _process_one_slice(ev_path: Path, mn_path: Path):
    """
    Read, filter, clean and cross-check ONE slice.

    Line-for-line the body of the single-process version's per-slice loop,
    unchanged, just factored out so it can be called once per slice inside a
    partition instead of once per iteration of one big loop. Returns
    (events_df, mentions_df_or_None) on success, or raises on failure — the
    caller (_process_partition) is what turns a failure into a skipped slice,
    exactly as the single try/except around the whole loop body used to.
    """
    events = read_zip(ev_path, GDELT_COLUMNS)
    mentions = read_zip(mn_path, MENTIONS_COLUMNS)
    mentions, _ = drop_rows_missing(mentions, EVENT_ID, "mentions")
    mentions, _ = drop_rows_missing(mentions, "MentionIdentifier", "mentions")
    mentions, _ = clean_confidence(mentions)

    # bronze -> silver: supply-chain relevance (same filter as 2-parsing).
    # Applied while the frame still carries parsing's column names.
    kept = events[events.apply(lambda r: passes_filter(r.to_dict()), axis=1)].copy()
    kept, _ = drop_rows_missing(kept, EVENT_ID, "events")
    kept, _ = clean_dateadded(kept)
    kept, _ = clean_goldstein(kept)
    if kept.empty:
        return None, None

    # Switch to the names the silver schema uses (positional: same columns,
    # same order).
    kept = kept[PARSED_EVENT_COLUMNS].copy()
    kept.columns = EVENT_COLUMNS
    # NOTE: DATEADDED stays as STRING here, matching the live pipeline's validator.py.
    # The storage layer's _dateadded() function handles string -> int conversion.
    # Do NOT convert to Int64; Spark's pickle/unpickle of nullable types causes
    # serialization issues across processes.
    mentions = mentions.copy()
    mentions = mentions[PARSED_MENTION_COLUMNS].copy()
    mentions.columns = MENTION_COLUMNS[:len(mentions.columns)]

    # This loader's own referential-integrity rule: scoped to THIS slice's own
    # kept events only (not "this slice OR the store", which is what the live
    # pipeline's validator.py additionally checks). Unchanged by this rewrite —
    # see the module docstring's "Why this is safe to parallelize".
    valid_ids = set(kept[EVENT_ID].astype(str).str.strip().tolist())
    m_ids = mentions[EVENT_ID].astype(str).str.strip()
    mentions = mentions[m_ids.isin(valid_ids)].copy()

    # the three enrichment columns must exist — the table has 12 columns
    if ENRICH and not mentions.empty:
        from enrichment import enrich_dataframe
        enrich_dataframe(mentions, url_column="MentionIdentifier",
                         max_workers=_ENRICH_WORKERS, do_nlp=_ENRICH_NLP,
                         time_budget_s=_ENRICH_TIMEOUT)
    else:
        mentions["article_title"] = ""
        mentions["article_keywords"] = ""
        mentions["enriched"] = 0

    return kept, (mentions if not mentions.empty else None)


def _process_partition(triples):
    """
    Runs ONCE PER PARTITION, each invocation in its own worker process (a real
    OS process even under local[*] — this is not thread-based parallelism, and
    is not subject to the GIL the way a plain Python thread pool would be).

    `triples` is an iterator of (slice_id, events_path, mentions_path) — the
    slice of the overall work Spark assigned to this partition. This function
    owns ONE Storage connection for its whole share of the work (opening one per
    row would be absurd; this is the standard Spark idiom for "do something
    with side effects, once per partition" — the RDD equivalent of
    df.write.jdbc()'s per-partition connections in spark_gold.py), walks its
    slices sequentially exactly as the original single loop did, and batches
    writes every BATCH_SLICES.

    Yields exactly one summary tuple, which the driver collects and aggregates.
    Nothing here communicates with any OTHER partition — see "Why this is safe
    to parallelize" in the module docstring.
    """
    triples = list(triples)
    if not triples:
        return iter([])

    storage = Storage(host=CH_HOST, port=CH_PORT, database=CH_DB,
                      user=CH_USER, password=CH_PASS)

    n_ok = n_failed = total_events = total_mentions = 0
    ev_batch: list = []
    mn_batch: list = []

    def flush():
        nonlocal ev_batch, mn_batch, total_events, total_mentions
        if ev_batch:
            df = pd.concat(ev_batch, ignore_index=True)
            storage.append_events(df)
            total_events += len(df)
            ev_batch = []
        if mn_batch:
            df = pd.concat(mn_batch, ignore_index=True)
            storage.append_mentions(df)
            total_mentions += len(df)
            mn_batch = []

    for i, (slice_id, ev_path, mn_path) in enumerate(triples, start=1):
        try:
            kept, mentions = _process_one_slice(ev_path, mn_path)
        except Exception as exc:                      # noqa: BLE001
            log.warning("slice %s unreadable (%s) — skipped", slice_id, exc)
            n_failed += 1
            continue

        n_ok += 1
        if kept is not None:
            ev_batch.append(kept)
        if mentions is not None:
            mn_batch.append(mentions)

        if i % BATCH_SLICES == 0:
            flush()
            log.info("partition: %d/%d slices | %d events, %d mentions so far",
                     i, len(triples), total_events, total_mentions)

    flush()
    storage.close()
    return iter([(n_ok, n_failed, total_events, total_mentions)])


def _spark() -> SparkSession:
    """
    One session for this one-shot run — unlike spark_gold.py's resident _spark(),
    there is no dead-JVM recovery concern here, because the process exits when
    the job finishes.

    spark.executorEnv.PYTHONPATH is set explicitly rather than relying on the
    sys.path.insert() calls above being inherited: those calls mutate ONLY this
    driver process's own interpreter state. Spark ships the function passed to
    mapPartitions to worker PROCESSES (real OS processes, even under local[*]),
    and those workers import parser/gdelt/validator/storage/enrichment BY NAME
    when they unpickle it — they need their OWN sys.path to already contain
    /app/parsing and /app/validation, which a runtime sys.path.insert() in a
    different process can never provide. This is set as a real environment
    variable (also exported at the OS level in the Dockerfile) precisely because
    that is the one thing guaranteed to reach a freshly spawned worker process.
    """
    pypath = f"/app/parsing:/app/validation:{os.environ.get('PYTHONPATH', '')}"
    conf = (
        SparkConf()
        .set("spark.executorEnv.PYTHONPATH", pypath)
        .set("spark.sql.shuffle.partitions", "1")   # unused: no DataFrame ops here
    )
    return (
        SparkSession.builder
        .appName("radar-bulk-load")
        .master(SPARK_MASTER)
        .config(conf=conf)
        .getOrCreate()
    )


def main() -> None:
    started = time.time()
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    new_report = not REPORT_FILE.exists()
    report = REPORT_FILE.open("a", encoding="utf-8")
    if new_report:
        report.write("finished_at_utc,slices_done,events_kept,mentions_kept,"
                     "elapsed_seconds,slices_per_minute\n")

    slices = find_slices()
    if not slices:
        log.error("no GDELT ZIPs found under %s — nothing to load", SOURCE_DIR)
        return

    # Only slices with BOTH files are usable: the referential-integrity filter
    # needs the events of the same slice to validate its mentions against.
    complete = sorted(s for s, f in slices.items() if "events" in f and "mentions" in f)
    incomplete = len(slices) - len(complete)
    if LIMIT_SLICES:
        complete = complete[-LIMIT_SLICES:]          # newest N

    log.info("found %d complete slices (%d incomplete, skipped)", len(complete), incomplete)
    log.info("enrichment is %s", "ON (slow)" if ENRICH else "OFF")
    log.info("spark master=%s, %d partitions", SPARK_MASTER, SPARK_PARTITIONS)

    storage = Storage(
        host=CH_HOST, port=CH_PORT, database=CH_DB, user=CH_USER, password=CH_PASS,
    )
    while True:
        try:
            storage.ensure_tables()
            break
        except Exception as exc:                      # noqa: BLE001
            log.warning("ClickHouse not ready (%s) — retrying in 5s", exc)
            time.sleep(5)
    storage.close()          # the driver does not write; only ensures the schema

    if not complete:
        report.close()
        return

    # (slice_id, events_path, mentions_path) triples — resolved on the driver so
    # each worker gets exactly the paths it needs, with no shared lookup dict to
    # serialize or reason about.
    triples = [(s, slices[s]["events"], slices[s]["mentions"]) for s in complete]

    spark = _spark()
    try:
        n_partitions = max(1, min(SPARK_PARTITIONS, len(triples)))
        rdd = spark.sparkContext.parallelize(triples, numSlices=n_partitions)
        results = rdd.mapPartitions(_process_partition).collect()
    finally:
        spark.stop()

    n_ok = sum(r[0] for r in results)
    n_failed = sum(r[1] for r in results)
    total_events = sum(r[2] for r in results)
    total_mentions = sum(r[3] for r in results)

    elapsed = time.time() - started
    rate = len(complete) / (elapsed / 60) if elapsed else 0
    log.info("DONE — %d slices (%d ok, %d failed), %d events, %d mentions in "
             "%.1f min (%.1f slices/min, %d partitions)",
             len(complete), n_ok, n_failed, total_events, total_mentions,
             elapsed / 60, rate, n_partitions)
    # slices_done counts every ATTEMPTED slice, ok or failed — matching the
    # pandas version, which used len(complete) here regardless of how many were
    # skipped inside the loop.
    report.write(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')},"
                 f"{len(complete)},{total_events},{total_mentions},"
                 f"{elapsed:.0f},{rate:.1f}\n")
    report.close()
    log.info("timings appended to %s", REPORT_FILE)


if __name__ == "__main__":
    main()
