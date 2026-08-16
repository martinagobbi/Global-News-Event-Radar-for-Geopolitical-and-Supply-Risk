"""
service_errors.py
==================
Shared error codes for backend → frontend communication.

When a data store (PostgreSQL or MongoDB) fails after all retry attempts, the
backend raises one of these structured errors instead of a generic 500/503.
The frontend reads the "code" field and shows a specific, human-readable
banner instead of a blank failure.

Code range: 5xx — store entirely unreachable (e.g., 503 connection/timeout) 
          or specific query/operation failed (e.g., 500 query error)
"""

from __future__ import annotations


class ServiceUnavailableError(Exception):
    """
    Raised when a backing store (PostgreSQL or MongoDB) could not be reached
    after exhausting all retry attempts.
    """

    def __init__(self, code: str, store: str, message: str):
        self.code = code
        self.store = store
        self.message = message
        super().__init__(f"[{code}] {store}: {message}")

    def as_dict(self) -> dict:
        return {
            "error": True,
            "code": self.code,
            "store": self.store,
            "message": self.message,
        }


# ── Defined error codes ─────────────────────────────────────────────────────

POSTGRES_UNAVAILABLE = "503-POSTGRES"
MONGO_UNAVAILABLE    = "503-MONGO"
POSTGRES_QUERY_ERROR = "500-POSTGRES"
MONGO_QUERY_ERROR  = "500-MONGO"