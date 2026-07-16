"""
configuration/countries.py
--------------------------
Territory picker options for the onboarding / dashboard multiselect.

The frontend runs on each user's machine and does NOT share the operator's
volume, so the options are fetched from the serving backend (GET /territories),
which reads them from Mongo (published there by the processing layer's startup
seed). The result is cached for the session so it isn't re-fetched on every
Streamlit rerun.
"""
from __future__ import annotations

import streamlit as st

from data.api_client import get_json


@st.cache_data(ttl=3600, show_spinner=False)
def get_territory_options() -> list[str]:
    """Fetch the territory list from the backend; [] if the backend is unreachable."""
    try:
        payload = get_json("/territories")
        return list(payload.get("territories", []))
    except Exception:
        return []
