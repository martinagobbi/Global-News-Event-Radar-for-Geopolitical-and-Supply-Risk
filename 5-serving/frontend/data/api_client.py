from __future__ import annotations

import os
from typing import Any

import requests


BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000").rstrip("/")


class BackendUnavailable(Exception):
    """Raised when the serving backend can't be reached or returns an error —
    so pages can show a banner instead of a raw Streamlit traceback."""


def _request(method: str, path: str, **kwargs: Any) -> Any:
    try:
        response = requests.request(method, f"{BACKEND_URL}{path}", timeout=20, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else None
    except requests.RequestException as e:
        raise BackendUnavailable(str(e)) from e


def get_json(path: str) -> Any:
    return _request("GET", path)


def post_json(path: str, payload: dict | None = None) -> Any:
    return _request("POST", path, json=payload or {})


def put_json(path: str, payload: dict) -> Any:
    return _request("PUT", path, json=payload)