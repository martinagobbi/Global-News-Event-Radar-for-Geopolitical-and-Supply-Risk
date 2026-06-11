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

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from clickhouse_writer import ClickHouseWriter
from processor import silver_to_gold, build_user_geo_filter

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
    limit:     int        = Query(default=5000,  description="Max silver rows"),
):
    """
    Read silver events from ClickHouse, apply the user's geographic filter,
    and write processed_{user_id}.csv to the shared volume.
    Called automatically by 5-serving/app.py when the user saves preferences.
    """
    pipeline = read_pipeline_status()
    if pipeline.get("state") == "ERROR":
        raise HTTPException(
            status_code=500,
            detail=f"Pipeline in error state ({pipeline.get('reason')}); "
                   f"data may be stale — refusing to serve.",
        )

    try:
        prefs = _load_user_prefs(user_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    cameo_codes, fips_codes = build_user_geo_filter(prefs)
    country_weights = prefs.get("country_weights", {})
    min_risk_score  = float(prefs.get("min_risk_score", 0.0))

    try:
        with _get_writer() as writer:
            silver_events = writer.query_silver(
                date_from=date_from,
                date_to=date_to,
                min_risk_score=0.0,  # country weights may raise an event above min_risk_score
                limit=limit,
            )
    except Exception as exc:
        logger.error("ClickHouse query failed for user %s: %s", user_id, exc)
        raise HTTPException(status_code=500, detail=f"ClickHouse error: {exc}")

    gold_events = silver_to_gold(
        silver_events=silver_events,
        cameo_codes=cameo_codes,
        fips_codes=fips_codes,
        country_weights=country_weights,
        min_risk_score=min_risk_score,
    )

    # Write CSV with the filename expected by 5-serving/app.py
    output_file = Path(PROCESSED_DIR) / f"processed_{user_id}.csv"
    if gold_events:
        pd.DataFrame(gold_events).to_csv(output_file, index=False)
    else:
        # Empty file with header so app.py does not raise an error
        pd.DataFrame(columns=[
            "event_id", "date", "event_code", "event_root",
            "actor1", "actor2", "country_code", "fips_country",
            "lat", "lon", "goldstein", "avg_tone", "num_articles",
            "risk_score", "source_url", "source", "layer",
        ]).to_csv(output_file, index=False)

    logger.info(
        "User %s: %d silver → %d gold → %s",
        user_id, len(silver_events), len(gold_events), output_file,
    )

    return JSONResponse({
        "status":         "success",
        "user_id":        user_id,
        "silver_count":   len(silver_events),
        "gold_count":     len(gold_events),
        "output_file":    str(output_file),
        "cameo_filter":   sorted(cameo_codes) if cameo_codes else None,
        "fips_filter":    sorted(fips_codes)  if fips_codes  else None,
        "min_risk_score": min_risk_score,
    })


@app.get("/health")
async def health_check():
    return JSONResponse({"status": "processing layer is running"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
