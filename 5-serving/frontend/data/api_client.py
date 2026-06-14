from __future__ import annotations

import os
from typing import Any

import requests


BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000").rstrip("/")


def _request(method: str, path: str, **kwargs: Any) -> Any:
    response = requests.request(method, f"{BACKEND_URL}{path}", timeout=20, **kwargs)
    response.raise_for_status()
    if not response.content:
        return None
    return response.json()


def get_json(path: str) -> Any:
    return _request("GET", path)


def post_json(path: str, payload: dict | None = None) -> Any:
    return _request("POST", path, json=payload or {})


def put_json(path: str, payload: dict) -> Any:
    return _request("PUT", path, json=payload)