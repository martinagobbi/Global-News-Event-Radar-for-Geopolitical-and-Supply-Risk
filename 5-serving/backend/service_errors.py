"""
service_errors.py
==================
Shared error codes for backend → frontend communication.

When a data store (Oracle or MongoDB) fails after all retry attempts, the
backend raises one of these structured errors instead of a generic 500/503.
The frontend reads the "code" field and shows a specific, human-readable
banner instead of a blank failure.

Code ranges:
    5xx — store entirely unreachable (connection/timeout exhausted retries)
    6xx — store reachable but the specific query/operation failed
"""

from __future__ import annotations


class ServiceUnavailableError(Exception):
    """
    Raised when a backing store (Oracle or MongoDB) could not be reached
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

ORACLE_UNAVAILABLE = "503-ORACLE"
MONGO_UNAVAILABLE  = "503-MONGO"
ORACLE_QUERY_ERROR = "500-ORACLE"
MONGO_QUERY_ERROR  = "500-MONGO"