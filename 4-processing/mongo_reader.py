"""
4-processing/mongo_reader.py
----------------------------

Read per-user profiles from the MongoDB replica set (populated by the serving
layer's backend). A profile holds the user's filter inputs — their territories
and keywords — which processing uses to build that user's slice of the data.

Connection
----------
The store is a 3-node replica set (mongo1/2/3, replicaSet "rs0") on the private
Docker network. pymongo is given all three hosts + the replica-set name so it
discovers the primary and fails over automatically:

    mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0

NOTE: the replica set must be INITIATED once (rs.initiate) before any read
works — until a primary is elected, reads raise ServerSelectionTimeoutError.

Everything (URI, database, collection, the id field) is environment-configurable
because the exact schema is owned by the serving backend.
"""

import logging
import os

logger = logging.getLogger("processing.mongo")

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
)
# Schema matches the serving backend (5-serving/backend/mongo_store.py):
# db "radar", collection "users", profiles keyed by _id == user_id.
MONGO_DB = os.getenv("MONGO_DB", "radar")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "users")
MONGO_USER_KEY = os.getenv("MONGO_USER_KEY", "_id")
REFERENCE_COLLECTION = os.getenv("MONGO_REFERENCE_COLLECTION", "reference")
MONGO_TIMEOUT_MS = int(os.getenv("MONGO_TIMEOUT_MS", "5000"))

_client = None


def _get_db():
    """Return the radar database handle, connecting lazily on first use."""
    global _client
    if _client is None:
        from pymongo import MongoClient  # lazy import
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS, w="majority")
        logger.info("Mongo client created for %s (db=%s, coll=%s)",
                    MONGO_URI, MONGO_DB, MONGO_COLLECTION)
    return _client[MONGO_DB]


def _get_collection():
    """Return the user-profiles collection."""
    return _get_db()[MONGO_COLLECTION]


def seed_territories(payload: dict) -> None:
    """
    Publish the territory table to Mongo so the (stateless, possibly multi-host)
    serving backend can read it without sharing the processing layer's volume.
    Stored as a single document `_id == "territories"` in the reference collection.
    """
    _get_db()[REFERENCE_COLLECTION].replace_one(
        {"_id": "territories"},
        {**payload, "_id": "territories"},
        upsert=True,
    )


def get_user_profile(user_id: str) -> dict | None:
    """
    Return the user's profile document, or None if no such user exists.
    Raises on connection/replica-set failure (e.g. not yet initiated).
    """
    doc = _get_collection().find_one({MONGO_USER_KEY: user_id})
    if doc is None:
        logger.info("No Mongo profile found for user %s", user_id)
    return doc


def get_all_profiles() -> list[dict]:
    """
    Return every user profile. This mirrors the serving backend's
    mongo_store.get_all_profiles(), which is the batch access pattern the team
    intends the processing layer to use (generate each user's gold from all
    profiles, rather than one HTTP request per user).
    """
    return list(_get_collection().find({}))


def users_collection():
    """Return the user-profiles collection (used by the change-stream trigger)."""
    return _get_collection()


# Tags that survive the user's preferences changing, and therefore protect an
# article from the orphan sweep. "archive" is deliberately NOT among them: it
# means "not important, get this off my radar", so there is nothing to preserve
# when the event stops matching the user's territories and keywords either.
# "requires_action" and "monitor" are the opposite — the user has committed to
# following that story, and losing it because they edited a keyword would lose
# their work.
PROTECTED_TAGS = frozenset({"requires_action", "monitor"})


def get_protected_event_ids() -> set[str]:
    """
    Every GLOBALEVENTID any user has filed under a PROTECTED tag, across all users.

    The serving layer reads triaged cards straight from `articles`, deliberately
    WITHOUT joining `user_articles`, so a tagged card can survive the user later
    dropping the territory that first brought it in. The orphan sweep therefore
    has to treat those events as protected: an article can be unreferenced by
    every user_articles row and still be the only copy of something someone is
    actively tracking.

    That argument applies to "needs action" and "monitoring" but NOT to "archive".
    Archiving an event says it does not matter, so once it also stops matching the
    user's preferences there is no reason to hold the row: it is swept like any
    other orphan and the card stops appearing in the Archive.

    The tag itself is left in MongoDB when that happens, which is deliberate and
    useful: if the user later re-adds the territory or keyword, the article row is
    re-created by the next recompute and their archive entry simply reappears.

    Tags are stored one document per user, `{_id: user_id, tags: {event_id: tag}}`.
    Raises on any error — the caller then skips the sweep entirely rather than
    risk deleting a card it could not prove was safe to delete.
    """
    try:
        ids: set[str] = set()
        for doc in _get_db()["tags"].find({}, {"tags": 1}):
            ids.update(str(eid) for eid, tag in (doc.get("tags") or {}).items()
                       if tag in PROTECTED_TAGS)
        return ids
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not read tagged event ids: %s", exc)
        raise
