from __future__ import annotations

from typing import Any


def _tone_priority(value: float | int | None) -> float:
    if value is None:
        return float("inf")
    return abs(float(value))


def build_event_card(
    global_event_id: str,
    event_row: dict[str, Any],
    mentions: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Takes already-cleaned backend mentions and prepares a Streamlit-ready event card.
    Mentions are sorted by Confidence DESC, then by the most neutral MentionDocTone.
    """
    sorted_mentions = sorted(
        mentions,
        key=lambda item: (
            -float(item.get("confidence", 0)),
            _tone_priority(item.get("mention_doc_tone")),
        ),
    )[:20]

    top_article = sorted_mentions[0] if sorted_mentions else {}

    synthetic_title = event_row.get("synthetic_title")
    card_title = synthetic_title or top_article.get(
        "mention_identifier",
        f"Event {global_event_id}",
    )

    return {
        "global_event_id": global_event_id,
        "card_title": card_title,
        "top_article_title": top_article.get("mention_identifier", "No article title available"),
        "top_article_url": top_article.get("url"),
        "event_summary": event_row.get(
            "event_summary",
            "No summary provided by the analytics engine.",
        ),
        "country": event_row.get("country", "Unknown"),
        "latitude": event_row.get("latitude"),
        "longitude": event_row.get("longitude"),
        "cameo_code": event_row.get("cameo_code"),
        "cameo_label": event_row.get("cameo_label"),
        "actor": event_row.get("actor"),
        "risk_category": event_row.get("risk_category"),
        "goldstein": event_row.get("goldstein"),
        "risk_score": event_row.get("risk_score", 0),
        "articles": sorted_mentions,
    }
