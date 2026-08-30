import time

import streamlit as st

from auth import require_auth
from components.branding import use_neutral_spinner
from components.briefing import render_briefing
from components.retention_notice import render_retention_notice
from data.api_client import BackendUnavailable
from data.gold_layer import get_events
from data.user_store import get_user_profile, is_first_login


DATA_REFRESH_SECONDS = 900
default_briefing_days = 60

use_neutral_spinner()
st.title("Historical Radar View")

user_id = require_auth()

try:
    if is_first_login(user_id):
        st.warning("First-time access detected. Complete the initial setup before opening the historical dashboard.")
        st.stop()

    profile = get_user_profile(user_id)
except BackendUnavailable:
    st.error("🔴 The backend is unreachable. Please try again shortly.")
    st.stop()

# ── Header ─────────────────────────────────────────────────────────────────
header_left, header_right = st.columns([3, 1])
with header_left:
    st.caption(
        f"Showing older events from {profile.get('briefing_days', default_briefing_days)} "
        f"to {profile.get('older_news_days', 180)} days ago."
    )
    render_retention_notice(preferences="follows")
with header_right:
    manual_refresh = st.button("Refresh now")

# ── Data fetch (rate-limited) ──────────────────────────────────────────────
now = time.time()
if "last_older_data_fetch" not in st.session_state:
    st.session_state.last_older_data_fetch = 0

should_refresh = manual_refresh or (now - st.session_state.last_older_data_fetch >= DATA_REFRESH_SECONDS)

if should_refresh:
    try:
        st.session_state.cached_historical_events = get_events(
            user_id,
            max_age_days=profile.get("older_news_days", 180),
            min_age_days=profile.get("briefing_days", default_briefing_days),
            exclude_archived=True,
        )
        st.session_state.last_older_data_fetch = now
        if manual_refresh:
            st.success("Data refreshed.")
    except BackendUnavailable:
        st.error("🔴 The backend is unreachable. Could not load historical events.")
        st.stop()

historical_events = st.session_state.get("cached_historical_events", [])

# ── Briefing ───────────────────────────────────────────────────────────────
st.subheader("Historical Event Cards")

if not historical_events:
    st.info(
        "Either no older events match your criteria, or you need to wait a "
        "few minutes for them to arrive."
    )
else:
    render_briefing(historical_events, selected_countries=profile.get("territories", []))
