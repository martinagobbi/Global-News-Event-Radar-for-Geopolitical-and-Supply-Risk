from __future__ import annotations

import streamlit as st

from data.api_client import get_json, put_json


def get_current_user() -> str:
    return st.session_state.get("current_user_id", "demo_logistics")


def is_first_login(user_id: str) -> bool:
    payload = get_json(f"/users/{user_id}/first-login")
    return bool(payload["first_login"])


def get_user_profile(user_id: str) -> dict:
    return get_json(f"/users/{user_id}/profile")


def save_user_profile(profile: dict) -> None:
    put_json(f"/users/{profile['user_id']}/profile", profile)


def set_event_tag(user_id: str, global_event_id: str, tag: str) -> None:
    put_json(f"/users/{user_id}/events/{global_event_id}/tag", {"tag": tag})


def get_event_tags(user_id: str) -> dict[str, str]:
    return get_json(f"/users/{user_id}/tags")