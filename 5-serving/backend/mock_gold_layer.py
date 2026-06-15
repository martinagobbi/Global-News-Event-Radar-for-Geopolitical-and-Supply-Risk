from __future__ import annotations

from datetime import date, timedelta, datetime, timezone


# ---------------------------------------------------------------------------
# Mock data — used only to seed /data/gold/global.json on first boot.
# Once the processing pipeline is live it will overwrite this file.
# ---------------------------------------------------------------------------

_BASE_EVENTS = {
    "1001": {
        "card_title": "[UNREST] Labour dispute detected in Italy involving Rail workers",
        "country": "Italy",
        "latitude": 45.4642,
        "longitude": 9.19,
        "cameo_code": "171",
        "cameo_label": "Unrest",
        "actor": "Rail workers",
        "risk_category": "Labour disputes involving worker associations",
        "goldstein": -5.2,
        "risk_score": 86,
        "event_summary": "Local labour disruption is slowing freight flows across Northern Italy.",
        "articles": [
            {
                "mention_identifier": "Rail strike disrupts freight corridors in Northern Italy",
                "url": "https://example.com/italy-rail-strike",
                "confidence": 92,
                "mention_doc_tone": 0.4,
                "in_raw_text": 1,
            }
        ],
    },
    "1002": {
        "card_title": "[ECONOMIC PRESSURE] Supply-side instability detected in Germany",
        "country": "Germany",
        "latitude": 50.1109,
        "longitude": 8.6821,
        "cameo_code": "023",
        "cameo_label": "Economic pressure",
        "actor": "Industrial operators",
        "risk_category": "Supply-side financial instability",
        "goldstein": -2.0,
        "risk_score": 63,
        "event_summary": "Energy cost pressure is affecting German intermodal logistics nodes.",
        "articles": [
            {
                "mention_identifier": "Energy volatility puts German industrial shipments under scrutiny",
                "url": "https://example.com/germany-energy",
                "confidence": 88,
                "mention_doc_tone": 0.2,
                "in_raw_text": 0,
            }
        ],
    },
    "1003": {
        "card_title": "[TRANSPORT DISRUPTION] Port delays detected in United States",
        "country": "United States",
        "latitude": 29.7604,
        "longitude": -95.3698,
        "cameo_code": "061",
        "cameo_label": "Transport disruption",
        "actor": "Port operators",
        "risk_category": "Recent supply-side transit-related accidents",
        "goldstein": -3.1,
        "risk_score": 72,
        "event_summary": "Port service disruption is raising operational risk for high-priority shipments.",
        "articles": [
            {
                "mention_identifier": "Houston port backlogs raise concerns for transatlantic shipments",
                "url": "https://example.com/houston-backlogs",
                "confidence": 94,
                "mention_doc_tone": 0.7,
                "in_raw_text": 1,
            }
        ],
    },
    "1004": {
        "card_title": "[CIVIL MOVEMENTS] Protest activity detected in United Kingdom",
        "country": "United Kingdom",
        "latitude": 51.5072,
        "longitude": -0.1276,
        "cameo_code": "190",
        "cameo_label": "Civil movements",
        "actor": "Civil movements",
        "risk_category": "Civil movements",
        "goldstein": -4.7,
        "risk_score": 78,
        "event_summary": "Protest activity near logistics routes may affect delivery reliability.",
        "articles": [
            {
                "mention_identifier": "London logistics firms monitor disruption risk during planned demonstrations",
                "url": "https://example.com/london-demonstrations",
                "confidence": 89,
                "mention_doc_tone": -0.2,
                "in_raw_text": 1,
            }
        ],
    },
}

_DAYS_AGO = {"1001": 3, "1002": 10, "1003": 18, "1004": 25}


def _build_event(event_id: str) -> dict:
    days_ago = _DAYS_AGO[event_id]
    event_date = date.today() - timedelta(days=days_ago)
    event = {
        "global_event_id": event_id,
        **_BASE_EVENTS[event_id],
        "event_date": event_date.isoformat(),
        "age_days": days_ago,
    }
    # top_article_url is the URL of the first article (highest confidence).
    # The processing pipeline must provide this as a top-level field.
    event["top_article_url"] = event["articles"][0]["url"] if event["articles"] else None
    return event


def demo_gold_layer() -> dict:
    """Return a global gold layer dict with all mock events."""
    return {
        "timestamp_of_last_update": datetime.now(timezone.utc).isoformat(),
        "events": [_build_event(eid) for eid in _BASE_EVENTS],
    }