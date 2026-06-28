#!/usr/bin/env python
"""
4-processing/main.py — Processing layer.

Reads silver from ClickHouse (gdelt_events / gdelt_mentions), maps it to the
serving's Oracle gold schema, and writes the three tables serving reads:

    articles         — per-article event records (upserted)
    user_articles    — which articles each user gets (the per-user gold)
    pipeline_status  — OK/ERROR + last-update time (mirrors the global status)

Entry points (also driven automatically by triggers.py, started on startup):
    POST /process-all        — silver changed -> rebuild articles + every user's set
    POST /process/{user_id}  — one user's prefs changed -> recompute only their set

The per-user FILTER is pushed down into ClickHouse
(clickhouse_writer.query_user_documents): a geographic clause on the event
country codes (CAMEO actor codes + FIPS geo codes, via countries.py) plus the
keyword clause (processor.build_keyword_clause) select exactly the mentions a
user receives.

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
import countries
import mongo_reader
import oracle_writer
import gold
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


def _gather_keywords(profile: dict) -> list[str]:
    """Flatten a profile's per-question keyword lists into one de-duplicated list."""
    kw = profile.get("keywords") or {}
    out: list[str] = []
    seen: set[str] = set()
    for vals in kw.values():
        for v in (vals or []):
            v = str(v).strip()
            if v and v not in seen:
                seen.add(v)
                out.append(v)
    return out


def _rows_for_profile(ch: ClickHouseWriter, profile: dict) -> list[dict]:
    """Run a user's geo + keyword filter in ClickHouse and map the hits to rows."""
    cameo_codes, fips_codes = countries.codes_for_names(profile.get("territories", []))
    keywords = _gather_keywords(profile)
    events_df, mentions_df = ch.query_user_documents(
        cameo_codes=cameo_codes,
        fips_codes=fips_codes,
        keywords=keywords,
        event_limit=EVENTS_LIMIT,
    )
    return gold.build_article_rows(events_df, mentions_df)


@app.get("/health")
def health() -> dict:
    return {"status": "processing layer is running"}


def recompute_all() -> dict:
    """
    Recompute every user's `user_articles`, refresh `articles`, and mirror the
    pipeline status. Pure function (no HTTP) — shared by the /process-all route
    and the silver-watermark trigger.
    """
    per_user: dict[str, int] = {}
    catalog: dict[str, dict] = {}
    with _ch() as ch:
        for profile in mongo_reader.get_all_profiles():
            uid = str(profile.get("_id") or profile.get("user_id") or "")
            if not uid:
                continue
            rows = _rows_for_profile(ch, profile)
            for r in rows:
                catalog[r["document_identifier"]] = r
            docs = [r["document_identifier"] for r in rows]
            oracle_writer.write_user_articles(uid, docs)
            per_user[uid] = len(docs)

    # `articles` only needs the rows some user references (serving joins
    # user_articles -> articles); upsert the de-duplicated union once.
    n_articles = oracle_writer.write_articles(list(catalog.values()))
    state = read_pipeline_status().get("state", "OK")
    oracle_writer.write_pipeline_status(state, datetime.now(timezone.utc))
    return {"articles": n_articles, "users": per_user, "pipeline_status": state}


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
    with _ch() as ch:
        rows = _rows_for_profile(ch, profile)
    docs = [r["document_identifier"] for r in rows]
    # Upsert the articles this user references so the user_articles join is
    # always satisfied even if /process-all hasn't run yet.
    oracle_writer.write_articles(rows)
    return oracle_writer.write_user_articles(user_id, docs)


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


@app.on_event("startup")
def _startup() -> None:
    """Start the background triggers (silver watermark + Mongo change stream)."""
    if os.getenv("ENABLE_TRIGGERS", "1") == "1":
        triggers.start(
            ch_factory=_ch,
            recompute_all=recompute_all,
            recompute_user=recompute_user,
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
