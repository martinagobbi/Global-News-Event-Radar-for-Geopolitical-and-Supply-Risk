from __future__ import annotations

import json
import os
from pathlib import Path

from mock_gold_layer import demo_gold_layer


DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
USERS_DIR = DATA_DIR / "users"
TAGS_DIR = DATA_DIR / "tags"
GOLD_DIR = DATA_DIR / "gold"
STATUS_FILE = DATA_DIR / "status" / "pipeline_status.json"


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
        gold_path = GOLD_DIR / f"{user_id}.json"
        if not gold_path.exists():
            write_json(gold_path, demo_gold_layer(user_id))


def read_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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


def get_tags(user_id: str) -> dict[str, str]:
    ensure_storage()