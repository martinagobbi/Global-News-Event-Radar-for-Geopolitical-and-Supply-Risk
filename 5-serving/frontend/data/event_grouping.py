from __future__ import annotations

from datetime import date, datetime
from typing import Any


def _date_value(value: Any) -> tuple[int, Any]:
    if value in (None, ""):
        return (0, date.min)
    if isinstance(value, datetime):
        return (1, value)
    if isinstance(value, date):
        return (1, value)
    text = str(value).strip().replace("Z", "+00:00")
    try:
        return (1, datetime.fromisoformat(text))
    except ValueError:
        try:
            return (1, datetime.strptime(text[:8], "%Y%m%d"))
        except ValueError:
            return (0, date.min)


def _number(value: Any) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return -1

def _float_value(value: Any) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0

def _mention_set(event: dict) -> set[str]:
    return {
        str(article.get("mention_identifier", ""))
        for article in event.get("articles", [])
        if str(article.get("mention_identifier", ""))
    }


def _leader_key(event: dict) -> tuple[Any, ...]:
    articles = event.get("articles", [])
    max_confidence = _number(event.get("max_confidence"))
    if max_confidence < 0:
        max_confidence = max(
            (_number(article.get("confidence")) for article in articles),
            default=-1,
        )
    return (
        max_confidence, # The leader is the one with the highest `confidence`
        _date_value(event.get("event_date")), # If there's a tie, choose by recency of event
        _date_value(event.get("date_added")), # Etc.
        _number(event.get("global_event_id")), # Etc.
    )


def _merge_group(group: list[dict]) -> dict:
    leader = max(group, key=_leader_key)
    leader_id = str(leader["global_event_id"])
    other_ids = sorted(
        (str(event["global_event_id"]) for event in group if event is not leader),
        key=_number,
        reverse=True,
    )
    merged = dict(leader)
    merged["global_event_id"] = leader_id
    merged["event_ids"] = [leader_id, *other_ids]
    return merged


def group_events(events: list[dict]) -> list[dict]:
    """Collapse events with identical article mention_identifier sets."""
    groups: dict[frozenset[str], list[dict]] = {}
    for event in events:
        groups.setdefault(frozenset(_mention_set(event)), []).append(event)

    cards = [_merge_group(group) for group in groups.values()]
    return sorted(
        cards,
        key=lambda event: (
            _date_value(event.get("oldest_article_time")),
            _date_value(event.get("event_date")),
            -_float_value(event.get("goldstein")), # Stronger NEGATIVE Goldstein score takes priority
        ),
        reverse=True,
    )
