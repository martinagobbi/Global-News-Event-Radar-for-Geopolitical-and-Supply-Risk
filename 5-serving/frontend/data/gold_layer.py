from __future__ import annotations

from data.api_client import get_json, put_json
from data.event_grouping import group_events


def get_system_status() -> dict:
    return get_json("/system/status")


def get_events(
    user_id: str,
    briefing_days: int | None = None,
    max_age_days: int = 180,
    min_age_days: int | None = None,
    exclude_archived: bool = True,
) -> list[dict]:
    """
    Fetch event cards for a user.
    The backend already applied InRawText filter, ordering, and 20-article cap.

    min_age_days keeps only events OLDER than that many days — used by the
    "Older news" tab so it does not repeat what the main briefing already shows.
    """
    # Grouping must see archived members too; otherwise a grouped card can be
    # split before the frontend has a chance to rebuild its set.
    params = f"max_age_days={max_age_days}&exclude_archived=false"
    if briefing_days is not None:
        params += f"&briefing_days={briefing_days}"
    if min_age_days is not None:
        params += f"&min_age_days={min_age_days}"
    payload = get_json(f"/users/{user_id}/events?{params}")
    events = group_events(payload["events"])
    if exclude_archived:
        events = [event for event in events if event.get("user_tag") != "archive"]
    return events


def get_event_detail(user_id: str, global_event_id: str) -> dict:
    """Fetch a single event with all its articles (for the detail/click view)."""
    return get_json(f"/users/{user_id}/events/{global_event_id}")


def get_archived_events(user_id: str) -> list[dict]:
    payload = get_json(f"/users/{user_id}/archived-events")
    return group_events(payload["events"])


def get_tagged_events(user_id: str, tag: str) -> list[dict]:
    """Events this user filed under one tag: requires_action / monitor / archive."""
    payload = get_json(f"/users/{user_id}/tagged-events/{tag}")
    return group_events(payload["events"])


def get_events_summary(
    user_id: str,
    briefing_days: int | None = None,
    max_age_days: int = 180,
) -> list[dict]:
    """
    Lightweight data for the heatmap.

    Takes the same window arguments as get_events() so the map and the briefing
    it sits above always describe the same set of events.
    """
    params = f"max_age_days={max_age_days}"
    if briefing_days is not None:
        params += f"&briefing_days={briefing_days}"
    payload = get_json(f"/users/{user_id}/events-summary?{params}")
    return payload["summary"]


def get_gold_layer_status(user_id: str) -> str:
    status = get_system_status()
    return status.get("status", "unknown")


def get_events_version(user_id: str) -> str | None:
    """Cheap fingerprint of the user's gold set (None if the gold store is unreachable)."""
    return get_json(f"/users/{user_id}/events-version").get("version")
