from __future__ import annotations

from data.api_client import get_json, post_json


def trigger_gold_layer_computation(user_id: str) -> None:
    post_json(f"/users/{user_id}/refresh-gold-layer")


def get_gold_layer_status(user_id: str) -> str:
    payload = get_json(f"/users/{user_id}/gold-layer/status")
    return payload.get("status", "unknown")


def get_briefing_events(user_id: str, days: int = 30, include_older: bool = False) -> list[dict]:
    payload = get_json(f"/users/{user_id}/briefing?days={days}&include_older={str(include_older).lower()}")
    return payload["events"]


def get_older_events(user_id: str, older_news_days: int = 90) -> list[dict]:
    payload = get_json(f"/users/{user_id}/older-events?older_news_days={older_news_days}")
    return payload["events"]


def get_archived_events(user_id: str) -> list[dict]:
    payload = get_json(f"/users/{user_id}/archive")
    return payload["events"]


def get_dashboard_summary(user_id: str) -> list[dict]:
    payload = get_json(f"/users/{user_id}/summary")
    return payload["summary"]


def get_system_status(user_id: str) -> dict:
    return get_json(f"/system/status?user_id={user_id}")