from __future__ import annotations

import logging
import os
import time

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError

logger = logging.getLogger(__name__)

_MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
_MONGO_DB  = os.getenv("MONGO_DB", "radar")

# How long (seconds) to wait for a connection before giving up
_SERVER_SELECTION_TIMEOUT = int(os.getenv("MONGO_TIMEOUT", "5"))

_client: MongoClient | None = None


def _db():
    global _client
    if _client is None:
        _client = MongoClient(
            _MONGO_URI,
            serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT * 1000,
            # If using a replica set, PyMongo handles primary failover automatically.
            # The connection string should list all nodes:
            #   mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0
        )
    return _client[_MONGO_DB]


def _users() -> Collection:
    return _db()["users"]


def _tags() -> Collection:
    return _db()["tags"]


# ── Retry helper ───────────────────────────────────────────────────────────

_RETRYABLE = (ConnectionFailure, ServerSelectionTimeoutError)


def _with_retry(fn, retries: int = 3, backoff: float = 1.0):
    """
    Run fn(), retrying on transient MongoDB errors with exponential backoff.
    Non-retryable errors (e.g. OperationFailure for bad queries) are re-raised
    immediately. After all retries are exhausted, re-raises the last error so
    callers can decide on a fallback.
    """
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return fn()
        except _RETRYABLE as e:
            last_exc = e
            wait = backoff * (2 ** attempt)
            logger.warning(
                "MongoDB transient error (attempt %d/%d), retrying in %.1fs: %s",
                attempt + 1, retries, wait, e,
            )
            time.sleep(wait)
        except OperationFailure:
            raise   # bad query / auth — don't retry
    raise last_exc


# ── Demo users ─────────────────────────────────────────────────────────────
# Demo seeding is retired: a profile is now defined by its territories + the five
# supply-chain keyword questions (no risk categories). cleanup_demo_users()
# removes any category-based demo profiles a previous build inserted.

_DEMO_USER_IDS = ["demo_logistics", "demo_energy"]


def cleanup_demo_users() -> None:
    """Delete any demo profiles seeded by earlier builds. Silent on MongoDB error."""
    try:
        _with_retry(lambda: _users().delete_many({"_id": {"$in": _DEMO_USER_IDS}}))
    except Exception as e:
        logger.error("Could not remove demo users (MongoDB unavailable?): %s", e)


def check_mongo_health() -> dict | None:
    """
    Lightweight MongoDB reachability check, used by /system/status.
    Returns None if MongoDB is reachable, or an error payload if it is not
    (after exhausting retries). This is intentionally cheap — a ping, not a
    real query — so it can run on every dashboard poll without adding load.
    """
    try:
        _with_retry(lambda: _db().command("ping"))
        return None
    except Exception as e:
        logger.error("check_mongo_health failed (MongoDB unreachable): %s", e)
        return {
            "code": "503-MONGO",
            "message": (
                "The backend could not reach the MongoDB database after "
                "multiple attempts. User profiles and tags may be temporarily unavailable."
            ),
        }


# ── Users / profiles ───────────────────────────────────────────────────────

def is_first_login(user_id: str) -> bool:
    """Returns True if no profile exists. Returns False on MongoDB error (fail-open)."""
    try:
        return _with_retry(lambda: _users().find_one({"_id": user_id})) is None
    except Exception as e:
        logger.error("is_first_login failed for %s: %s", user_id, e)
        return False   # fail-open: treat as registered so user sees the dashboard


def get_profile(user_id: str) -> dict:
    """Returns the stored profile, or a safe default if MongoDB is unavailable."""
    _default = {
        "user_id": user_id,
        "display_name": user_id,
        "territories": [],
        "keywords": {},
        "briefing_days": 30,
        "older_news_days": 90,
        "status": "new",
    }
    try:
        doc = _with_retry(lambda: _users().find_one({"_id": user_id}))
        if doc is None:
            return _default
        doc.pop("_id", None)
        return doc
    except Exception as e:
        logger.error("get_profile failed for %s: %s", user_id, e)
        return _default


def save_profile(user_id: str, profile: dict) -> dict:
    """
    Saves the profile. Raises on failure — the caller should surface this
    to the user rather than silently swallowing it (the user needs to know
    their preferences were not saved).
    """
    payload = {**profile, "user_id": user_id, "status": "registered"}
    _with_retry(
        lambda: _users().replace_one(
            {"_id": user_id},
            {**payload, "_id": user_id},
            upsert=True,
        )
    )
    return payload


def get_all_profiles() -> list[dict]:
    """
    Used by the processing layer to read all user profiles directly from MongoDB.
    Returns empty list on failure — the processing layer should handle this gracefully.
    """
    try:
        docs = _with_retry(lambda: list(_users().find({})))
        for d in docs:
            d.pop("_id", None)
        return docs
    except Exception as e:
        logger.error("get_all_profiles failed: %s", e)
        return []


# ── Tags ───────────────────────────────────────────────────────────────────

def get_tags(user_id: str) -> dict[str, str]:
    """Returns stored tags, or empty dict on MongoDB error (events still show, just untagged)."""
    try:
        doc = _with_retry(lambda: _tags().find_one({"_id": user_id}))
        return doc["tags"] if doc else {}
    except Exception as e:
        logger.error("get_tags failed for %s: %s", user_id, e)
        return {}


def set_tag(user_id: str, global_event_id: str, tag: str) -> dict:
    """
    Saves a tag. Raises on failure — the user should know their tag was not saved.
    """
    _with_retry(
        lambda: _tags().update_one(
            {"_id": user_id},
            {"$set": {f"tags.{global_event_id}": tag}},
            upsert=True,
        )
    )
    return {"global_event_id": global_event_id, "tag": tag}