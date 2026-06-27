#!/usr/bin/env python
"""
4-processing/main.py — Processing layer.

Reads silver from ClickHouse (gdelt_events / gdelt_mentions), maps it to the
serving's Oracle gold schema, and writes the three tables serving reads:

    articles         — per-article event records (upserted)
    user_articles    — which articles each user gets (the per-user gold)
    pipeline_status  — OK/ERROR + last-update time (mirrors the global status)

Entry points (the triggers that *call* these are a loose end — see below):
    POST /process-all        — silver changed -> rebuild articles + every user's set
    POST /process/{user_id}  — one user's prefs changed -> recompute only their set

The per-user FILTER (countries via CAMEO/FIPS, risk categories, keywords) is a
work in progress: gold.select_document_ids_for_user currently returns ALL
articles for every user.

Environment
-----------
    CLICKHOUSE_HOST / PORT / DATABASE / USER / PASSWORD   (silver source)
    MONGO_URI / MONGO_DB / MONGO_COLLECTION               (user profiles)
    ORACLE_HOST / PORT / SERVICE / USER / PASSWORD        (gold sink)
    STATUS_DIR        global status dir (default /data/status)
    GOLD_EVENTS_LIMIT max events pulled per run (default 20000)
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from clickhouse_writer import ClickHouseWriter
import mongo_reader
import oracle_writer
import gold

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
EVENTS_LIMIT = int(os.getenv("GOLD_EVENTS_LIMIT", "20000"))

app = FastAPI(title="Supply Risk — Processing Layer")


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


def _build_article_rows() -> list[dict]:
    """Pull silver from ClickHouse and map it to Oracle `articles` rows."""
    with _ch() as ch:
        events_df = ch.query_events(limit=EVENTS_LIMIT)
        event_ids = events_df["GLOBALEVENTID"].tolist() if not events_df.empty else []
        mentions_df = ch.query_mentions_for_events(event_ids)
    return gold.build_article_rows(events_df, mentions_df)


@app.get("/health")
def health() -> dict:
    return {"status": "processing layer is running"}


@app.post("/process-all")
def process_all():
    """Silver changed: rebuild `articles`, every user's `user_articles`, and status."""
    try:
        rows = _build_article_rows()
        n_articles = oracle_writer.write_articles(rows)

        per_user: dict[str, int] = {}
        for profile in mongo_reader.get_all_profiles():
            uid = str(profile.get("_id") or profile.get("user_id") or "")
            if not uid:
                continue
            docs = gold.select_document_ids_for_user(profile, rows)
            oracle_writer.write_user_articles(uid, docs)
            per_user[uid] = len(docs)

        state = read_pipeline_status().get("state", "OK")
        oracle_writer.write_pipeline_status(state, datetime.now(timezone.utc))
    except Exception as exc:  # noqa: BLE001
        logger.exception("process-all failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"process-all failed: {exc}")

    return JSONResponse({
        "status": "success",
        "articles": n_articles,
        "users": per_user,
        "pipeline_status": state,
    })


@app.post("/process/{user_id}")
def process_user(user_id: str):
    """One user's prefs changed: recompute only their `user_articles`."""
    profile = mongo_reader.get_user_profile(user_id)
    if profile is None:
        raise HTTPException(status_code=404, detail=f"No profile for user '{user_id}'")

    try:
        rows = _build_article_rows()
        docs = gold.select_document_ids_for_user(profile, rows)
        # Upsert the articles this user references so the user_articles join is
        # always satisfied even if /process-all hasn't run yet.
        doc_set = set(docs)
        oracle_writer.write_articles([r for r in rows if r["document_identifier"] in doc_set])
        n = oracle_writer.write_user_articles(user_id, docs)
    except Exception as exc:  # noqa: BLE001
        logger.exception("process failed for %s: %s", user_id, exc)
        raise HTTPException(status_code=500, detail=f"process failed: {exc}")

    return JSONResponse({"status": "success", "user_id": user_id, "user_articles": n})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
