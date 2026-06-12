#!/usr/bin/env python
"""
4-processing/main.py


Compatibility with the serving layer
--------------------------------------
5-serving/app.py reads:
    Path(PROCESSED_DIR) / f"processed_{user_id}.csv"
This layer writes exactly that file.

User preferences format (saved by 5-serving/app.py):
    {
        "theme":            "light",
        "notifications":    true,
        "data_format":      "table",
        "cameo_countries":  ["US", "CN"],       <- added by updated app.py
        "fips_countries":   ["EI"],              <- added by updated app.py
        "country_weights":  {"CN": 1.5},         <- added by updated app.py
        "min_risk_score":   3.0                  <- added by updated app.py
    }

Environment variables
---------------------
    CLICKHOUSE_HOST / PORT / DATABASE / USER / PASSWORD
    PREFS_DIR      (default: /data/user_preferences)
    PROCESSED_DIR  (default: /data/processed)
"""

import json
import logging
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from clickhouse_writer import ClickHouseWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("processing")

CH_HOST     = os.getenv("CLICKHOUSE_HOST",     "clickhouse-01")
CH_PORT     = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CH_USER     = os.getenv("CLICKHOUSE_USER",     "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

PREFS_DIR     = os.getenv("PREFS_DIR",     "/data/user_preferences")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "/data/processed")
STATUS_FILE   = Path(os.getenv("STATUS_DIR", "/data/status")) / "pipeline_status.json"

os.makedirs(PREFS_DIR,     exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

app = FastAPI(title="Supply Risk — Processing Layer")


def _get_writer() -> ClickHouseWriter:
    return ClickHouseWriter(
        host=CH_HOST, port=CH_PORT,
        database=CH_DATABASE, user=CH_USER, password=CH_PASSWORD,
    )


def read_pipeline_status() -> dict:
    """Read the global error status written by the validation layer."""
    if not STATUS_FILE.exists():
        return {"state": "OK"}
    try:
        return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"state": "OK"}


def _load_user_prefs(user_id: str) -> dict:
    prefs_file = Path(PREFS_DIR) / f"{user_id}_prefs.json"
    if not prefs_file.exists():
        raise FileNotFoundError(
            f"Preferences file not found for user '{user_id}'. "
            f"Expected: {prefs_file}"
        )
    with open(prefs_file, "r") as f:
        return json.load(f)


@app.post("/process/{user_id}")
async def process_user(
    user_id: str,
    date_from: str | None = Query(default=None, description="Start date YYYYMMDD"),
    date_to:   str | None = Query(default=None, description="End date YYYYMMDD"),
    limit:     int        = Query(default=5000,  description="Max events"),
):
    """
    Read this user's slice of the store from gdelt_events / gdelt_mentions and
    write it to the shared volume for the serving layer.

    NOTE: the per-user geographic and keyword filters, and the silver->gold
    transform, are NOT applied yet — those are defined once we know how each
    user's data reaches this layer. For now this reads the deduplicated raw
    events and their related mentions from the store and writes them as-is.
    """
    pipeline = read_pipeline_status()
    if pipeline.get("state") == "ERROR":
        raise HTTPException(
            status_code=500,
            detail=f"Pipeline in error state ({pipeline.get('reason')}); "
                   f"data may be stale — refusing to serve.",
        )

    try:
        with _get_writer() as writer:
            events_df = writer.query_events(
                date_from=date_from, date_to=date_to, limit=limit,
            )
            event_ids = events_df["GLOBALEVENTID"].tolist() if not events_df.empty else []
            mentions_df = writer.query_mentions_for_events(event_ids)
    except Exception as exc:
        logger.error("ClickHouse query failed for user %s: %s", user_id, exc)
        raise HTTPException(status_code=500, detail=f"ClickHouse error: {exc}")

    # No silver->gold transform and no per-user geo/keyword filtering yet.
    events_file   = Path(PROCESSED_DIR) / f"processed_{user_id}.csv"
    mentions_file = Path(PROCESSED_DIR) / f"processed_{user_id}_mentions.csv"
    events_df.to_csv(events_file, index=False)
    mentions_df.to_csv(mentions_file, index=False)

    logger.info(
        "User %s: %d events, %d mentions → %s",
        user_id, len(events_df), len(mentions_df), events_file,
    )

    return JSONResponse({
        "status":         "success",
        "user_id":        user_id,
        "events_count":   int(len(events_df)),
        "mentions_count": int(len(mentions_df)),
        "events_file":    str(events_file),
        "mentions_file":  str(mentions_file),
        "note": "raw read from gdelt_events/gdelt_mentions; "
                "per-user filtering and gold transform not applied yet",
    })


@app.get("/health")
async def health_check():
    return JSONResponse({"status": "processing layer is running"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
