#!/usr/bin/env python
"""
4-processing/main.py — Processing layer.

Reads silver from ClickHouse (gdelt_events / gdelt_mentions), maps it to the
serving's PostgreSQL gold schema, and writes the three tables serving reads:

    articles         — per-article event records (upserted)
    user_articles    — which articles each user gets (the per-user gold)
    pipeline_status  — OK/ERROR + last-update time (mirrors the global status)

Entry points (also driven automatically by triggers.py, started on startup):
    POST /process-all        — silver changed -> rebuild articles + every user's set
    POST /process/{user_id}  — one user's prefs changed -> recompute only their set

The silver -> gold work itself is done by PySpark (spark_gold.py), which is the
only implementation of it. A user's geographic predicate (CAMEO actor codes +
FIPS geo codes, via countries.py) and keyword predicate are evaluated by the
Spark cluster against a cached catalogue, rather than being pushed into
ClickHouse and materialised in this process.

This module keeps what does NOT distribute: the two triggers, the daily
retention job, the orphan sweep and the status mirror — all small transactional
statements against PostgreSQL and MongoDB.

Environment
-----------
    CLICKHOUSE_HOST / PORT / DATABASE / USER / PASSWORD   (silver source)
    MONGO_URI / MONGO_DB / MONGO_COLLECTION               (user profiles)
    POSTGRES_DSN                                          (gold sink)
    SPARK_MASTER      local[*] in testing, spark://spark-master:7077 in intended
    STATUS_DIR        global status dir (default /data/status)
"""

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from clickhouse_writer import ClickHouseWriter
import countries
import mongo_reader
import postgres_writer
import spark_gold
import retention
import triggers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("processing")

CH_HOST     = os.getenv("CLICKHOUSE_HOST",     "clickhouse-s1r1")
CH_PORT     = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CH_USER     = os.getenv("CLICKHOUSE_USER",     "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

STATUS_FILE  = Path(os.getenv("STATUS_DIR", "/data/status")) / "pipeline_status.json"

# ── Incremental gold ─────────────────────────────────────────────────────────
# A watermark-triggered recompute normally re-evaluates ALL of silver for every
# profile: a slice bringing 34 mentions rebuilt 111,430 candidates x 3 users in
# ~134 s. Incremental considers only mentions newer than the last published
# watermark, which is the same work for the same result in the common case.
#
# FULL_EVERY forces a full run periodically, because incremental can only ADD
# (see spark_gold.recompute). Without it, rows that stopped matching, orphaned
# articles and retention deletes would never be cleared. At the 15-minute
# cadence, 12 puts a full rebuild roughly every three hours.
#
# The counter is in memory on purpose: a restart resets it to 0, and run 0 is
# always full. Restarting therefore forces a clean rebuild, which is the safe
# direction — the opposite of persisting it and resuming mid-cycle.
INCREMENTAL_GOLD = os.getenv("INCREMENTAL_GOLD", "1") == "1"
FULL_EVERY       = int(os.getenv("GOLD_FULL_EVERY", "12"))
_incremental_runs = 0


def _bump_incremental_counter() -> int:
    """Return the current run number, then advance. Run 0 is always full."""
    global _incremental_runs
    n = _incremental_runs
    _incremental_runs += 1
    return n

app = FastAPI(title="Supply Risk — Processing Layer")


@app.on_event("startup")
def _seed_reference_data() -> None:
    """Publish the territory table to Mongo so the serving backend can serve it to
    the (remote) frontend. Runs in a background thread that RETRIES until Mongo is
    ready: the replica set can take a while to elect a primary after startup, so a
    single attempt would often lose the race and leave the onboarding picker empty.
    """
    def _loop() -> None:
        while True:
            try:
                mongo_reader.seed_territories({
                    "options": countries.COUNTRY_OPTIONS,
                    "codes": countries.COUNTRY_CODES,
                    "count": len(countries.COUNTRY_OPTIONS),
                })
                logger.info("Seeded %d territories into Mongo (reference)",
                            len(countries.COUNTRY_OPTIONS))
                return  # success → stop retrying
            except Exception as exc:  # noqa: BLE001
                logger.warning("territory seed failed, retrying in 5s: %s", exc)
                time.sleep(5)
    threading.Thread(target=_loop, name="seed-territories", daemon=True).start()


def _ch() -> ClickHouseWriter:
    return ClickHouseWriter(
        host=CH_HOST, port=CH_PORT,
        database=CH_DATABASE, user=CH_USER, password=CH_PASSWORD,
    )


def read_pipeline_status() -> dict:
    """The global error status written by the validation layer."""
    if not STATUS_FILE.exists():
        return {"state": "OK"}
    try:
        return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"state": "OK"}





@app.get("/health")
def health() -> dict:
    return {"status": "processing layer is running"}


def _sweep_orphans() -> int:
    """
    Delete gold `articles` rows nobody references, protecting tracked events.

    "Protected" means filed under "needs action" or "monitoring" — NOT archived.
    Archiving says the event does not matter, so once it stops matching the user's
    preferences too there is nothing worth keeping the row for.

    Skips the sweep entirely if the tag list cannot be read. Leaving a few
    unreachable rows behind costs a little space; deleting a card someone filed
    under "needs action" loses their work, so the safe failure is to do nothing.
    """
    try:
        protected = mongo_reader.get_protected_event_ids()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Skipping orphan sweep: could not read tags (%s)", exc)
        return 0
    return postgres_writer.delete_orphan_articles(protected)


def recompute_all() -> dict:
    """
    Recompute every user's `user_articles`, refresh `articles`, and mirror the
    pipeline status. Pure function (no HTTP) — shared by the /process-all route
    and the silver-watermark trigger.

    The work itself is done by PySpark (4-processing/spark_gold.py), which is now
    the ONLY silver -> gold implementation. There used to be a second, in-process
    pandas path here; keeping two implementations of identical semantics meant
    every rule had to be changed in both places and could silently diverge.

    Spark is used in BOTH modes, and only SPARK_MASTER differs: `local[*]` in
    testing mode, so the job runs inside this container with no cluster to
    deploy, and `spark://spark-master:7077` in intended mode, where it is spread
    across worker machines.

    Dropping the pandas path is what allowed GOLD_EVENTS_LIMIT to go. That cap
    existed because the old path pulled every matching event into one process's
    memory with `SELECT *`, and its id list then had to fit inside ClickHouse's
    256 KB max_query_size. Spark reads in partitions and joins events to mentions
    as a distributed shuffle rather than a client-side IN list, so neither ceiling
    applies and no truncation is needed.
    """
    # Block overlapping runs by acquiring the database lock
    with postgres_writer.advisory_lock():
        # Sampled BEFORE the recompute, deliberately. Spark reads silver at some
        # point during the run, so a value read afterwards can name a slice that
        # arrived mid-run and was never actually read — which would overstate how
        # fresh the gold is, and the serving tier uses this to decide whether the
        # briefing is stale. Sampling first makes it a lower bound: gold contains
        # at least this much. Understating costs one poll cycle of apparent lag
        # (the next watermark advance recomputes and corrects it); overstating
        # would hide a stalled pipeline, which is the failure this exists to show.
        try:
            with _ch() as ch:
                watermark = ch.silver_watermark()
            watermark = str(watermark) if watermark else None
        except Exception as exc:  # noqa: BLE001 — never fail a publish over this
            logger.warning("could not read the silver watermark: %s", exc)
            watermark = postgres_writer.KEEP

        # ── Full or incremental? ────────────────────────────────────────────
        # Incremental is an OPTIMISATION and is only correct when this run is
        # genuinely "the previous gold, plus one more slice of mentions". Every
        # condition below is a case where that premise fails, and each falls back
        # to a full rebuild rather than producing a subtly wrong gold:
        #
        #   prev is None      gold has never been built, or the column predates
        #                     this feature — there is no baseline to add to.
        #   watermark <= prev silver did not move forwards. Either nothing new
        #                     (no work) or it moved BACKWARDS — a trim, wipe or
        #                     seed restore — after which gold must be rebuilt to
        #                     match the smaller silver, which adding cannot do.
        #   every FULL_EVERY  a periodic full run, so the things incremental can
        #                     never do (drop rows that stopped matching, sweep
        #                     orphans, honour retention deletes) still happen on
        #                     a bounded schedule instead of never.
        #
        # INCREMENTAL_GOLD=0 disables it outright and restores the previous
        # behaviour, which is the intended way to rule it out when diagnosing a
        # gold that looks wrong.
        since = None
        if INCREMENTAL_GOLD and watermark and watermark is not postgres_writer.KEEP:
            prev = postgres_writer.read_silver_watermark()
            n = _bump_incremental_counter()
            if prev is None:
                logger.info("full recompute: no previous watermark recorded")
            elif watermark <= prev:
                logger.info("full recompute: watermark did not advance (%s -> %s)",
                            prev, watermark)
            elif n % FULL_EVERY == 0:
                logger.info("full recompute: periodic full run (every %d)", FULL_EVERY)
            else:
                since = prev
                logger.info("INCREMENTAL recompute: mentions after %s", since)

        result = spark_gold.recompute(since=since)
        # The sweep and the status mirror stay here, in plain Python: they are small
        # transactional statements against PostgreSQL and MongoDB with nothing to
        # distribute. Spark's publish() runs the same sweep inside its own
        # transaction; running it again is harmless (it deletes what is already gone)
        # and keeps the behaviour identical whichever entry point was used.
        # Skipped for the same reason publish() skips its own copy: the sweep
        # asks "which articles does nobody reference now", which only a full run
        # can answer. After an incremental run nothing was removed from
        # user_articles, so nothing can newly have become an orphan.
        n_orphans = 0 if since else _sweep_orphans()
        state = read_pipeline_status().get("state", "OK")
        postgres_writer.write_pipeline_status(
            state, datetime.now(timezone.utc), watermark=watermark)
    return {"articles": result.get("articles", 0), "orphans_removed": n_orphans,
            "users": result.get("users", 0), "pipeline_status": state}


@app.post("/process-all")
def process_all():
    """Silver changed: recompute every user's `user_articles`, refresh `articles`, status."""
    try:
        result = recompute_all()
    except Exception as exc:  # noqa: BLE001
        logger.exception("process-all failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"process-all failed: {exc}")
    return JSONResponse({"status": "success", **result})


def recompute_user(user_id: str) -> int | None:
    """
    Recompute one user's `user_articles` (and upsert the articles they
    reference). Returns the count, or None if the user has no profile. Shared by
    the /process/{user_id} route and the Mongo change-stream trigger.
    """
    profile = mongo_reader.get_user_profile(user_id)
    if profile is None:
        return None
    # Same Spark job, restricted to this user: only their predicate is evaluated
    # and only their user_articles rows are replaced. A user who has narrowed
    # their preferences until nothing matches still ends up with an empty pool
    # rather than a stale one, which is what the old write_user_articles(uid, [])
    # guaranteed.
    # Block overlapping runs by acquiring the database lock
    with postgres_writer.advisory_lock():
        result = spark_gold.recompute(only_user=user_id)
        # This user's set has just been replaced, so articles they dropped may now be
        # referenced by nobody. Rows any OTHER user still references are kept by the
        # anti-join, so purging here is safe even though only one user was recomputed.
        _sweep_orphans()
    return result.get("articles", 0)


@app.post("/process/{user_id}")
def process_user(user_id: str):
    """One user's prefs changed: recompute only their `user_articles`."""
    try:
        n = recompute_user(user_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("process failed for %s: %s", user_id, exc)
        raise HTTPException(status_code=500, detail=f"process failed: {exc}")
    if n is None:
        raise HTTPException(status_code=404, detail=f"No profile for user '{user_id}'")
    return JSONResponse({"status": "success", "user_id": user_id, "user_articles": n})


def _report_silver_stale(idle_seconds: int) -> None:
    """
    Record in the gold layer that silver has stopped advancing, so the serving
    tier can say the data is stale instead of reporting OK simply because Oracle
    answered. The last-update timestamp is deliberately left where it was: it
    marks when the data was actually refreshed, which is the fact being reported.
    """
    logger.warning("Recording pipeline_status=ERROR: silver idle for %d s", idle_seconds)
    postgres_writer.mark_pipeline_stale()


@app.on_event("startup")
def _startup() -> None:
    """Start the background triggers and the daily retention job."""
    if os.getenv("ENABLE_TRIGGERS", "1") == "1":
        triggers.start(
            ch_factory=_ch,
            recompute_all=recompute_all,
            recompute_user=recompute_user,
            report_stale=_report_silver_stale,
        )
    # Separately gated: retention is the only thing in the pipeline that deletes
    # data, so it must be possible to run the processing layer without it.
    if os.getenv("ENABLE_RETENTION", "1") == "1":
        retention.start(ch_factory=_ch)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
