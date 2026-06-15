from __future__ import annotations

import json
import os
from pathlib import Path

from mock_gold_layer import demo_gold_layer


DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
USERS_DIR = DATA_DIR / "users"
TAGS_DIR  = DATA_DIR / "tags"
GOLD_DIR  = DATA_DIR / "gold"
STATUS_FILE = DATA_DIR / "status" / "pipeline_status.json"

# Single global gold file written exclusively by the processing pipeline.
GLOBAL_GOLD_FILE = GOLD_DIR / "global.json"


DEMO_USERS = {
    "demo_logistics": {
        "user_id": "demo_logistics",
        "display_name": "Demo Logistics",
        "countries": ["Italy", "Germany", "United States", "United Kingdom"],
        "risk_categories": [
            "Labour disputes involving worker associations",
            "Supply-side financial instability",
            "Recent supply-side transit-related accidents",
            "Civil movements",
        ],
        "briefing_days": 30,
        "older_news_days": 90,
        "status": "registered",
    },
    "demo_energy": {
        "user_id": "demo_energy",
        "display_name": "Demo Energy",
        "countries": ["Germany", "United States"],
        "risk_categories": [
            "Supply-side financial instability",
            "Major supply-side accidents or breakdowns",
            "Inflation in supply-side economy",
        ],
        "briefing_days": 30,
        "older_news_days": 120,
        "status": "registered",
    },
}


def ensure_storage() -> None:
    for folder in (USERS_DIR, TAGS_DIR, GOLD_DIR, STATUS_FILE.parent):
        folder.mkdir(parents=True, exist_ok=True)
    for user_id, profile in DEMO_USERS.items():
        path = USERS_DIR / f"{user_id}.json"
        if not path.exists():
            write_json(path, profile)
    # Seed the global gold file with mock data only on first boot.
    # The processing pipeline owns this file once it starts running.
    if not GLOBAL_GOLD_FILE.exists():
        write_json(GLOBAL_GOLD_FILE, demo_gold_layer())


def read_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ── Users ──────────────────────────────────────────────────────────────────

def is_first_login(user_id: str) -> bool:
    ensure_storage()
    return not (USERS_DIR / f"{user_id}.json").exists()


def get_profile(user_id: str) -> dict:
    ensure_storage()
    return read_json(
        USERS_DIR / f"{user_id}.json",
        {
            "user_id": user_id,
            "display_name": user_id,
            "countries": [],
            "risk_categories": [],
            "briefing_days": 30,
            "older_news_days": 90,
            "status": "new",
        },
    )


def save_profile(user_id: str, profile: dict) -> dict:
    ensure_storage()
    payload = {**profile, "user_id": user_id, "status": "registered"}
    write_json(USERS_DIR / f"{user_id}.json", payload)
    return payload


# ── Tags (per-user, stored separately from the global gold layer) ──────────

def get_tags(user_id: str) -> dict[str, str]:
    ensure_storage()
    return read_json(TAGS_DIR / f"{user_id}.json", {})


def set_tag(user_id: str, global_event_id: str, tag: str) -> dict:
    ensure_storage()
    tags = get_tags(user_id)
    tags[str(global_event_id)] = tag
    write_json(TAGS_DIR / f"{user_id}.json", tags)
    return {"global_event_id": global_event_id, "tag": tag}


# ── Gold layer ─────────────────────────────────────────────────────────────

def get_gold_layer(user_id: str) -> dict:
    """
    Read the global gold layer (written by the processing pipeline) and
    attach the calling user's per-event tags before returning.
    The profile-based country/category filter is applied in main.py.
    """
    ensure_storage()
    gold = read_json(
        GLOBAL_GOLD_FILE,
        {"timestamp_of_last_update": None, "events": []},
    )
    tags = get_tags(user_id)
    for event in gold.get("events", []):
        eid = str(event.get("global_event_id", ""))
        event["user_tag"] = tags.get(eid)
    return gold


def refresh_gold_layer(user_id: str) -> dict:
    """
    Triggered when the user requests a manual refresh from the dashboard.
    The processing pipeline runs on its own schedule; this endpoint is a
    no-op hook for future integration (e.g. writing a trigger flag file).
    Returns the current gold layer timestamp so the UI can display it.
    """
    gold = get_gold_layer(user_id)
    return {
        "status": "refresh_requested",
        "timestamp_of_last_update": gold.get("timestamp_of_last_update"),
    }


# ── Pipeline status ────────────────────────────────────────────────────────

def get_pipeline_status(user_id: str | None = None) -> dict:
    """
    Read /data/status/pipeline_status.json written by the processing pipeline.

    Expected schema (flat, no nesting):
        {"status": "OK",    "timestamp_of_last_update": "<iso-datetime>"}
        {"status": "ERROR", "timestamp_of_last_update": "<iso-datetime>"}

    If the file is absent or unreadable, we treat the system as healthy
    (the pipeline simply hasn't run yet / we're still using mock data).
    The timestamp shown in the error banner comes from global.json so it
    always reflects when real news data was last successfully written.
    """
    ensure_storage()
    raw = read_json(STATUS_FILE, {"status": "OK"})

    pipeline_status = raw.get("status", "OK")

    # Timestamp for the error banner: prefer the one from the global gold
    # file because it reflects the last successful data write, which is
    # exactly what the user cares about ("last updated at …").
    gold = read_json(GLOBAL_GOLD_FILE, {})
    last_update = gold.get("timestamp_of_last_update") or raw.get("timestamp_of_last_update")

    return {
        "status": pipeline_status,
        "timestamp_of_last_update": last_update,
    }
