from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from mongo_store import (
    check_mongo_health,
    get_all_profiles,
    get_profile,
    get_tags,
    get_territories,
    is_first_login,
    save_profile,
    set_tag,
)
from oracle_store import (
    get_event_articles,
    get_events_by_ids,
    get_events_for_user,
    get_events_version,
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


@app.get("/territories")
def territories() -> dict:
    # Territory picker options for the frontend onboarding/dashboard, read from
    # Mongo (published there by the processing layer's startup seed).
    return {"territories": get_territories()}


@app.get("/users/{user_id}/events-version")
def events_version(user_id: str) -> dict:
    # Cheap fingerprint of the user's gold set; the dashboard polls it to show a
    # "your articles changed — refresh" nudge without re-fetching all events.
    return {"version": get_events_version(user_id)}


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
    min_age_days: int | None = None,
    exclude_archived: bool = True,
) -> dict:
    # min_age_days makes the window a true BAND: the "Older news" tab asks for
    # events older than the main briefing window, so the two never overlap.
    # get_events_for_user() returns [] on Oracle error.
    events = get_events_for_user(user_id, max_age_days=max_age_days)

    # Attach per-user tags — get_tags() returns {} on MongoDB error.
    tags = get_tags(user_id)
    for event in events:
        event["user_tag"] = tags.get(str(event["global_event_id"]))

    if briefing_days is not None:
        events = [e for e in events if int(e.get("age_days", 0)) <= briefing_days]

    if min_age_days is not None:
        events = [e for e in events if int(e.get("age_days", 0)) > min_age_days]

    if exclude_archived:
        # Only archived events leave the Radar View. Events tagged "needs action"
        # or "monitoring" stay visible here as well as on their own page.
        events = [e for e in events if e.get("user_tag") != "archive"]

    return {"events": events}


@app.get("/users/{user_id}/events/{global_event_id}")
def get_event(user_id: str, global_event_id: str) -> dict:
    event = get_event_articles(user_id, global_event_id)
    if not event:
        # An empty result can mean "no such event" OR "Oracle unreachable". Only
        # call it 404 when Oracle is actually up; otherwise surface 503.
        if get_pipeline_status().get("code") == "503-ORACLE":
            raise HTTPException(status_code=503, detail="Database temporarily unavailable. Please try again shortly.")
        raise HTTPException(status_code=404, detail="Event not found for this user.")
    tags = get_tags(user_id)
    event["user_tag"] = tags.get(str(global_event_id))
    return event


@app.get("/users/{user_id}/events-summary")
def events_summary(user_id: str) -> dict:
    events = get_events_for_user(user_id, max_age_days=90)
    tags = get_tags(user_id)

    # Aggrega per coordinata: somma event_count e filtra null
    aggregated: dict[tuple, dict] = {}
    for e in events:
        if tags.get(str(e["global_event_id"])) == "archive":
            continue
        lat = e.get("latitude")
        lon = e.get("longitude")
        country = e.get("country")
        if lat is None or lon is None or country is None:
            continue   # salta righe con coordinate null
        key = (country, round(lat, 4), round(lon, 4))
        if key not in aggregated:
            aggregated[key] = {
                "country": country,
                "latitude": lat,
                "longitude": lon,
                "event_count": 0,
            }
        aggregated[key]["event_count"] += len(e.get("articles", [])) or 1

    return {"summary": list(aggregated.values())}


@app.get("/users/{user_id}/archived-events")
def archived_events(user_id: str) -> dict:
    return tagged_events(user_id, "archive")


@app.get("/users/{user_id}/tagged-events/{tag}")
def tagged_events(user_id: str, tag: str) -> dict:
    """
    Events this user filed under one tag — 'requires_action', 'monitor' or
    'archive'. Backs the per-triage pages in the frontend.

    Read by GLOBALEVENTID straight from `articles`, NOT through user_articles, so
    a triaged card survives the user later dropping the territory that first
    brought it in. Tags live in Mongo keyed by user_id, so one user's triage
    never affects another's.
    """
    tags = get_tags(user_id)
    event_ids = [eid for eid, t in tags.items() if t == tag]
    events = get_events_by_ids(event_ids)
    return {"events": [{**e, "user_tag": tag} for e in events]}