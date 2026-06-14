from __future__ import annotations

from fastapi import FastAPI

from storage import (
    get_gold_layer,
    get_pipeline_status,
    get_profile,
    get_tags,
    is_first_login,
    refresh_gold_layer,
    save_profile,
    set_tag,
)


app = FastAPI(title="Global News Event Radar Backend")


def _profile_filter(user_id: str, events: list[dict]) -> list[dict]:
    profile = get_profile(user_id)
    countries = set(profile.get("countries", []))
    categories = set(profile.get("risk_categories", []))
    filtered = []
    for event in events:
        if countries and event.get("country") not in countries:
            continue
        if categories and event.get("risk_category") not in categories:
            continue
        filtered.append(event)
    return filtered


def _visible_events(user_id: str) -> list[dict]:
    gold = get_gold_layer(user_id)
    events = _profile_filter(user_id, gold.get("events", []))
    return [event for event in events if event.get("user_tag") != "archive"]


@app.get("/health")
def health() -> dict:
    return {"status": "OK"}


@app.get("/system/status")
def system_status(user_id: str | None = None) -> dict:
    return get_pipeline_status(user_id)


@app.get("/users/{user_id}/first-login")
def first_login(user_id: str) -> dict:
    return {"first_login": is_first_login(user_id)}


@app.get("/users/{user_id}/profile")
def read_profile(user_id: str) -> dict:
    return get_profile(user_id)


@app.put("/users/{user_id}/profile")
def update_profile(user_id: str, profile: dict) -> dict:
    saved = save_profile(user_id, profile)
    refresh_gold_layer(user_id)
    return saved


@app.post("/users/{user_id}/refresh-gold-layer")
def refresh_user_gold_layer(user_id: str) -> dict:
    return refresh_gold_layer(user_id)


@app.get("/users/{user_id}/gold-layer/status")
def gold_layer_status(user_id: str) -> dict:
    gold = get_gold_layer(user_id)
    return {
        "status": "active",
        "timestamp_of_last_update": gold.get("timestamp_of_last_update"),
    }


@app.get("/users/{user_id}/tags")
def read_tags(user_id: str) -> dict:
    return get_tags(user_id)


@app.put("/users/{user_id}/events/{global_event_id}/tag")
def update_tag(user_id: str, global_event_id: str, payload: dict) -> dict:
    return set_tag(user_id, global_event_id, payload["tag"])


@app.get("/users/{user_id}/briefing")
def briefing(user_id: str, days: int = 30, include_older: bool = False) -> dict:
    events = _visible_events(user_id)
    if not include_older:
        events = [event for event in events if int(event.get("age_days", 0)) <= days]
    return {"events": events}


@app.get("/users/{user_id}/older-events")
def older_events(user_id: str, older_news_days: int = 90) -> dict:
    events = [
        event
        for event in _visible_events(user_id)
        if 30 < int(event.get("age_days", 0)) <= older_news_days
    ]
    return {"events": events}


@app.get("/users/{user_id}/archive")
def archive(user_id: str) -> dict:
    events = [
        event
        for event in _profile_filter(user_id, get_gold_layer(user_id).get("events", []))
        if event.get("user_tag") == "archive"
    ]
    return {"events": events}


@app.get("/users/{user_id}/summary")
def summary(user_id: str) -> dict:
    rows = [
        {
            "country": event["country"],
            "latitude": event["latitude"],
            "longitude": event["longitude"],
            "risk_score": event["risk_score"],
            "event_count": len(event.get("articles", [])),
        }
        for event in _visible_events(user_id)
    ]
    return {"summary": rows}