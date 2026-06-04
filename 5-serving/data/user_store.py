from __future__ import annotations

import json
import os
from pathlib import Path

import streamlit as st


DATA_DIR = Path(os.getenv("STORAGE_DIR", Path(__file__).resolve().parents[1] / "_storage"))
USERS_FILE = DATA_DIR / "users.json"
TAGS_FILE = DATA_DIR / "tags.json"


def ensure_storage() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not USERS_FILE.exists():
        USERS_FILE.write_text("{}", encoding="utf-8")
    if not TAGS_FILE.exists():
        TAGS_FILE.write_text("{}", encoding="utf-8")


def _read_json(path: Path) -> dict:
    ensure_storage()
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    ensure_storage()
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def get_current_user() -> str:
    return st.session_state.get("current_user_id", "demo_user")


def is_first_login(user_id: str) -> bool:
    users = _read_json(USERS_FILE)
    return user_id not in users


def save_user_profile(profile: dict) -> None:
    users = _read_json(USERS_FILE)
    users[profile["user_id"]] = profile
    _write_json(USERS_FILE, users)


def get_user_profile(user_id: str) -> dict:
    users = _read_json(USERS_FILE)
    profile = users.get(
        user_id,
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
    if "risk_categories" not in profile:
        profile["risk_categories"] = profile.get("sectors", [])
    return profile


def set_event_tag(user_id: str, global_event_id: str, tag: str) -> None:
    tags = _read_json(TAGS_FILE)
    user_tags = tags.setdefault(user_id, {})
    user_tags[global_event_id] = tag
    _write_json(TAGS_FILE, tags)


def get_event_tags(user_id: str) -> dict[str, str]:
    return _read_json(TAGS_FILE).get(user_id, {})