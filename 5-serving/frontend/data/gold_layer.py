from __future__ import annotations

from data.api_client import get_json, put_json


def get_system_status() -> dict:
    return get_json("/system/status")


def get_events(
    user_id: str,
    briefing_days: int | None = None,
    max_age_days: int = 90,
    exclude_archived: bool = True,
) -> list[dict]:
    """
    Fetch event cards for a user.
    The backend already applied InRawText filter, ordering, and 20-article cap.
    """
    params = f"max_age_days={max_age_days}&exclude_archived={str(exclude_archived).lower()}"
    if briefing_days is not None:
        params += f"&briefing_days={briefing_days}"
    payload = get_json(f"/users/{user_id}/events?{params}")
    return payload["events"]


def get_event_detail(user_id: str, global_event_id: str) -> dict:
    """Fetch a single event with all its articles (for the detail/click view)."""
    return get_json(f"/users/{user_id}/events/{global_event_id}")


def get_archived_events(user_id: str) -> list[dict]:
    payload = get_json(f"/users/{user_id}/archived-events")
    return payload["events"]


def get_events_summary(user_id: str) -> list[dict]:
    """Lightweight data for the heatmap."""
    payload = get_json(f"/users/{user_id}/events-summary")
    return payload["summary"]


def get_gold_layer_status(user_id: str) -> str:
    status = get_system_status()
    return status.get("status", "unknown")