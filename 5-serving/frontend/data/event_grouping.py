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


# ── Null-tolerant keys ───────────────────────────────────────────────────────
# Every measure below is nullable, so each key is a PAIR: (has_value, value).
# Tuples compare left to right, so the flag decides first and sends "we do not
# know" to the bottom whatever the value is. Two missing values compare equal and
# fall through to the next tie-breaker.
#
# NOTE the sorts here pass reverse=True, so a HIGHER key sorts first. Present is
# therefore flagged 1 and missing 0 — the opposite of the ascending convention in
# the backend's postgres_store, and the reason the two files do not share these.
def _confidence_key(value: Any) -> tuple[int, float]:
    """Higher confidence first; missing last."""
    try:
        if value is None:
            return (0, 0.0)
        return (1, float(str(value).strip()))
    except (TypeError, ValueError):
        return (0, 0.0)


def _goldstein_key(value: Any) -> tuple[int, float]:
    """
    More NEGATIVE Goldstein first — a worse event outranks a milder one — with
    missing last. Negated here rather than at the call site so the (flag, value)
    pair stays monotonic under reverse=True.
    """
    try:
        if value is None:
            return (0, 0.0)
        return (1, -float(str(value).strip()))
    except (TypeError, ValueError):
        return (0, 0.0)


def _event_id_key(value: Any) -> tuple[int, str]:
    """
    The final tie-breaker, compared as a STRING.

    GLOBALEVENTID is a string everywhere in this pipeline and is never coerced to
    a number. The previous `int(...)` conversion mapped every non-numeric id to
    -1, which silently collapsed all of them into one tie — and would do so for
    every id at once the day GDELT introduces a letter.
    """
    text = "" if value is None else str(value).strip()
    return (1, text) if text else (0, "")

def _mention_set(event: dict) -> set[str]:
    return {
        str(article.get("mention_identifier", ""))
        for article in event.get("articles", [])
        if str(article.get("mention_identifier", ""))
    }


def _leader_key(event: dict) -> tuple[Any, ...]:
    articles = event.get("articles", [])
    confidence = _confidence_key(event.get("max_confidence"))
    if confidence[0] == 0:                       # not provided on the card itself
        # Fall back to the best confidence among the card's articles. max() over
        # the (flag, value) pairs picks a present value over any missing one for
        # free, because (1, x) > (0, 0.0) for every x.
        confidence = max(
            (_confidence_key(a.get("confidence")) for a in articles),
            default=(0, 0.0),
        )
    return (
        confidence,                              # highest confidence leads
        _date_value(event.get("event_date")),    # tie -> most recent event date
        _date_value(event.get("date_added")),    # tie -> most recently added
        _event_id_key(event.get("global_event_id")),   # final tie-break, as text
    )


def _merge_group(group: list[dict]) -> dict:
    leader = max(group, key=_leader_key)
    leader_id = str(leader.get("global_event_id") or "")
    other_ids = sorted(
        (str(event.get("global_event_id") or "")
         for event in group if event is not leader),
        key=_event_id_key,               # STRING order — never coerced to int
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
            _date_value(event.get("oldest_article_time")),   # coverage STARTED most recently (15-min-sharp precision)
            _date_value(event.get("event_date")),            # tie -> most recent event date (day-sharp precision)
            _goldstein_key(event.get("goldstein")),          # tie -> most NEGATIVE first
            _event_id_key(event.get("global_event_id")),     # final tie-break, as text
        ),
        reverse=True,
    )
