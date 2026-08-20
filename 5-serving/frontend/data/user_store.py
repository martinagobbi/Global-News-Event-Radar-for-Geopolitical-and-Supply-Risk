from __future__ import annotations

from auth import current_user
from data.api_client import get_json, put_json, delete_json

def get_current_user() -> str:
    return current_user() or ""

def is_first_login(user_id: str) -> bool:
    payload = get_json(f"/users/{user_id}/first-login")
    return bool(payload["first_login"])

def get_user_profile(user_id: str) -> dict:
    return get_json(f"/users/{user_id}/profile")

def save_user_profile(profile: dict) -> None:
    put_json(f"/users/{profile['user_id']}/profile", profile)
 
 
def set_event_tag(user_id: str, global_event_ids: str | list[str], tag: str | None) -> None:
    ids = [global_event_ids] if isinstance(global_event_ids, str) else global_event_ids
    if not ids:
        return
    put_json(
        f"/users/{user_id}/events/{ids[0]}/tag",
        {"tag": tag, "global_event_ids": [str(event_id) for event_id in ids]},
    )

def remove_event_tag(user_id: str, global_event_ids: str | list[str]) -> None:
    set_event_tag(user_id, global_event_ids, None)
 
def get_event_tags(user_id: str) -> dict[str, str]:
    return get_json(f"/users/{user_id}/tags")
