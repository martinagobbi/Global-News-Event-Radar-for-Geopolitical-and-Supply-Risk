from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from mongo_store import (
    check_mongo_health,
    cleanup_demo_users,
    get_all_profiles,
    get_profile,
    get_tags,
    is_first_login,
    save_profile,
    set_tag,
)
from oracle_store import (
    get_event_articles,
    get_events_for_user,
    get_pipeline_status,
)

logger = logging.getLogger(__name__)
app = FastAPI(title="Global News Event Radar — Backend")


# ── Global error handler ───────────────────────────────────────────────────
# Catches any unhandled exception and returns a structured JSON error instead
# of an HTML 500 page, so the Streamlit frontend can display a clean message.

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.exception("Unhandled error on %s: %s", request.url, exc)
    return JSONResponse(
        status_code=503,
        content={
            "error": "service_unavailable",
            "message": (
                "The backend is temporarily unavailable. "
                "Please try again in a few moments."
            ),
        },
    )


@app.on_event("startup")
def startup() -> None:
    # Remove any demo profiles a previous (category-based) build seeded.
    cleanup_demo_users()


# ── Health ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health() -> dict:
    return {"status": "OK"}


# ── System status ──────────────────────────────────────────────────────────

@app.get("/system/status")
def system_status() -> dict:
    """
    Combined health signal for the dashboard banner.

    Priority order:
      1. Oracle unreachable (503-ORACLE) — no event data can be shown at all,
         this is the most severe case.
      2. MongoDB unreachable (503-MONGO) — events can still be read from
         Oracle, but profiles/tags can't be saved or read.
      3. Pipeline-reported ERROR (processing layer issue, e.g. ingestion
         stalled) — data is stale but the stores themselves are fine.
      4. OK.
    """
    pipeline = get_pipeline_status()

    if pipeline.get("code") == "503-ORACLE":
        return pipeline   # already shaped as the error payload

    mongo_error = check_mongo_health()
    if mongo_error:
        return {
            "status": "ERROR",
            "timestamp_of_last_update": pipeline.get("timestamp_of_last_update"),
            **mongo_error,
        }

    return pipeline


# ── User profiles (MongoDB) ────────────────────────────────────────────────

@app.get("/users/{user_id}/first-login")
def first_login(user_id: str) -> dict:
    # is_first_login() fails-open (returns False) on MongoDB error,
    # so the user sees the dashboard rather than a broken login screen.
    return {"first_login": is_first_login(user_id)}


@app.get("/users/{user_id}/profile")
def read_profile(user_id: str) -> dict:
    # get_profile() returns a safe default on MongoDB error.
    return get_profile(user_id)


@app.put("/users/{user_id}/profile")
def update_profile(user_id: str, profile: dict) -> dict:
    try:
        return save_profile(user_id, profile)
    except Exception as e:
        logger.error("save_profile failed for %s: %s", user_id, e)
        raise HTTPException(
            status_code=503,
            detail="Could not save profile — database temporarily unavailable. Please try again.",
        )


@app.get("/users/all-profiles")
def all_profiles() -> dict:
    # Used directly by the processing layer.
    # Returns empty list on MongoDB error — processing handles that gracefully.
    return {"profiles": get_all_profiles()}


# ── Tags (MongoDB) ─────────────────────────────────────────────────────────

@app.get("/users/{user_id}/tags")
def read_tags(user_id: str) -> dict:
    # get_tags() returns {} on MongoDB error — events still show, just untagged.
    return get_tags(user_id)


@app.put("/users/{user_id}/events/{global_event_id}/tag")
def update_tag(user_id: str, global_event_id: str, payload: dict) -> dict:
    try:
        return set_tag(user_id, global_event_id, payload["tag"])
    except Exception as e:
        logger.error("set_tag failed for %s / %s: %s", user_id, global_event_id, e)
        raise HTTPException(
            status_code=503,
            detail="Could not save tag — database temporarily unavailable. Please try again.",
        )


# ── Events (Oracle) ────────────────────────────────────────────────────────

@app.get("/users/{user_id}/events")
def list_events(
    user_id: str,
    max_age_days: int = 90,
    briefing_days: int | None = None,
    exclude_archived: bool = True,
) -> dict:
    # get_events_for_user() returns [] on Oracle error.
    events = get_events_for_user(user_id, max_age_days=max_age_days)

    # Attach per-user tags — get_tags() returns {} on MongoDB error.
    tags = get_tags(user_id)
    for event in events:
        event["user_tag"] = tags.get(str(event["global_event_id"]))

    if briefing_days is not None:
        events = [e for e in events if int(e.get("age_days", 0)) <= briefing_days]

    if exclude_archived:
        events = [e for e in events if e.get("user_tag") != "archive"]

    return {"events": events}


@app.get("/users/{user_id}/events/{global_event_id}")
def get_event(user_id: str, global_event_id: str) -> dict:
    event = get_event_articles(user_id, global_event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found for this user.")
    tags = get_tags(user_id)
    event["user_tag"] = tags.get(str(global_event_id))
    return event


@app.get("/users/{user_id}/events-summary")
def events_summary(user_id: str) -> dict:
    events = get_events_for_user(user_id, max_age_days=90)
    tags = get_tags(user_id)
    rows = [
        {
            "country":    e["country"],
            "latitude":   e["latitude"],
            "longitude":  e["longitude"],
            "event_count": len(e.get("articles", [])),
        }
        for e in events
        if tags.get(str(e["global_event_id"])) != "archive"
    ]
    return {"summary": rows}


@app.get("/users/{user_id}/archived-events")
def archived_events(user_id: str) -> dict:
    events = get_events_for_user(user_id, max_age_days=90)
    tags = get_tags(user_id)
    archived = [
        {**e, "user_tag": "archive"}
        for e in events
        if tags.get(str(e["global_event_id"])) == "archive"
    ]
    return {"events": archived}