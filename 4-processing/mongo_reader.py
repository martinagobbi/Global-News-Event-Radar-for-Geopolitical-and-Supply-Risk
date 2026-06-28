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
MONGO_TIMEOUT_MS = int(os.getenv("MONGO_TIMEOUT_MS", "5000"))

_client = None


def _get_collection():
    """Return the user-profiles collection, connecting lazily on first use."""
    global _client
    if _client is None:
        from pymongo import MongoClient  # lazy import
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
        logger.info("Mongo client created for %s (db=%s, coll=%s)",
                    MONGO_URI, MONGO_DB, MONGO_COLLECTION)
    return _client[MONGO_DB][MONGO_COLLECTION]


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
