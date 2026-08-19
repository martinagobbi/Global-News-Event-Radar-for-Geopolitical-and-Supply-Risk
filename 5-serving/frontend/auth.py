"""
This gates the UI only. The serving backend has no authentication of its own,
so anyone who can reach it directly can still query any user's data.

The three accounts are fixed and their passwords are published in the README by
design.
"""
from __future__ import annotations

import hashlib
import hmac
import time

import streamlit as st

IDLE_TIMEOUT_SECONDS = 15 * 60

# user_id -> (salt, sha256(salt + password))
# Regenerate with: hashlib.sha256((salt + password).encode()).hexdigest()
CREDENTIALS: dict[str, tuple[str, str]] = {
    "radar_electronics": (
        "es26",
        "63e1563067aba3ccfd3220ebe7f2328b83447e40dafb40f6554c112a21ae26f0",
    ),
    "radar_pharma": (
        "ph26",
        "bea201aa6fba1db6971a3634432637d872140dd95d7731294900bd0cb771f511",
    ),
    "radar_agrifood": (
        "ag26",
        "f4849b62c9fe1383b102b163c131997a0968d1704556d400dc2510be5cd108f7",
    ),
}


def _digest(salt: str, password: str) -> str:
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def verify(user_id: str, password: str) -> bool:
    """Constant-time credential check. Unknown users take the same code path."""
    entry = CREDENTIALS.get(user_id)
    if entry is None:
        # Compare against a dummy so a missing user costs the same as a wrong
        # password (no timing signal on which usernames exist).
        hmac.compare_digest(_digest("x", password), _digest("x", "y"))
        return False
    salt, expected = entry
    return hmac.compare_digest(_digest(salt, password), expected)


def login(user_id: str) -> None:
    st.session_state["auth_user"] = user_id
    st.session_state["auth_last_seen"] = time.time()


def logout() -> None:
    """Drop the session identity and every per-user widget/cache key."""
    for key in list(st.session_state.keys()):
        del st.session_state[key]


def current_user() -> str | None:
    return st.session_state.get("auth_user")


def require_auth() -> str:
    """
    Gate a protected page. Returns the signed-in user id, or halts the script.

    Defence in depth. app.py already decides which pages exist for the current
    session — an unauthenticated run registers only the sign-in page, so the
    others are not routable and Streamlit answers their URLs with "Page not
    found". That was, until this function existed, the ONLY thing protecting
    them, and it protects by ABSENCE rather than by refusal.

    That is a fragile thing to rely on, for two reasons found on 2026-08-18:

      * Nothing else checked. Pages read the user through
        data.user_store.get_current_user(), which is `current_user() or ""` —
        it never stops and never redirects, so a page reached by any means at
        all would render happily for an empty user id.
      * The absence is not guaranteed. Streamlit automatically discovers a
        directory named `pages/` beside the entrypoint and builds its own
        navigation from it, which is how a sidebar listing every page appeared
        on the sign-in screen. That directory is now `views/`, which Streamlit
        does not auto-discover, but renaming a folder is not an access control.

    So each protected page calls this, and the guarantee stops depending on the
    layout of the app. is_authenticated() is safe to call twice per rerun: it
    only reads session state and refreshes the idle timestamp.

    NOT a substitute for backend authentication. As the module docstring says,
    the serving API has none of its own — this gates the UI, nothing more.
    """
    if not is_authenticated():
        st.error("Please sign in to view this page.")
        st.stop()
    return current_user()


def _expired() -> bool:
    last = st.session_state.get("auth_last_seen")
    if last is None:
        return True
    return (time.time() - last) > IDLE_TIMEOUT_SECONDS


def is_authenticated() -> bool:
    """
    True if a live, non-idle session exists. Logs out (and reports False) when
    the idle window has elapsed. Called once per rerun from app.py, before any
    page renders, so an expired session can never reach a protected page.
    """
    if current_user() is None:
        return False
    if _expired():
        logout()
        st.session_state["auth_notice"] = "idle_timeout"
        return False
    st.session_state["auth_last_seen"] = time.time()   # touch on activity
    return True
