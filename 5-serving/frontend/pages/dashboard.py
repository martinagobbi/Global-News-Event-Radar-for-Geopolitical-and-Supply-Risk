import time

import streamlit as st

from components.branding import use_neutral_spinner
from components.briefing import render_briefing
from components.heatmap import render_heatmap
from configuration.countries import get_territory_options
from data.api_client import BackendUnavailable
from data.gold_layer import (
    get_archived_events,
    get_events,
    get_events_summary,
    get_events_version,
    get_gold_layer_status,
    get_system_status,
)
from data.user_store import get_current_user, get_user_profile, is_first_login
from components.recompute_notice import (
    gold_never_built,
    recompute_pending,
    render_first_build_notice,
    render_no_matches_notice,
    render_recompute_notice,
)
from components.retention_notice import render_retention_notice


STATUS_POLL_SECONDS  = 30
DATA_REFRESH_SECONDS = 900   # 15 minutes — aligned with ingestion cadence


use_neutral_spinner()
st.title("Radar View")

user_id = get_current_user()

try:
    if is_first_login(user_id):
        st.warning("First-time access detected. Complete the initial setup before opening the dashboard.")
        st.stop()

    profile = get_user_profile(user_id)
    territory_options = get_territory_options()

    # ── Pipeline / store status (always re-fetched, cheap) ─────────────────────
    system_status = get_system_status()
    live_gold_version = get_events_version(user_id)
except BackendUnavailable:
    st.error("🔴 The backend is unreachable. Please try again shortly.")
    st.stop()

status_code = system_status.get("code")

if status_code == "503-ORACLE":
    st.error(
        "🔴 **503 — Database unavailable.** "
        + system_status.get(
            "message",
            "The backend could not reach the Oracle database after multiple attempts.",
        )
        + " Event data cannot be loaded right now. Please try again shortly."
    )
elif status_code == "503-MONGO":
    st.warning(
        "🟠 **503 — Profile service unavailable.** "
        + system_status.get(
            "message",
            "The backend could not reach the MongoDB database after multiple attempts.",
        )
        + " Events may still be visible, but your tags and saved preferences "
        "might not load or save correctly until this is resolved."
    )
elif system_status.get("status") == "ERROR":
    last_update = system_status.get("timestamp_of_last_update", "an unknown time")
    st.error(
        "Due to technical difficulties, this dashboard has not been updated since "
        f"{last_update}."
    )

# ── Preferences just changed: the pool is being rebuilt ────────────────────
# Shown from the moment preferences are saved until the rebuild lands, so the
# dashboard never silently shows articles chosen by the old preferences. It
# clears itself when the fingerprint moves, and the green message below then
# announces the new set.
if recompute_pending(live_gold_version):
    render_recompute_notice()

# ── "Your articles changed" nudge (green = good news) ──────────────────────
# live_gold_version is polled cheaply every rerun; st.session_state.gold_version
# is the version the currently-shown events were rendered against (set in the
# data-fetch block below). If they differ, new gold has arrived for this user.
elif (
    live_gold_version is not None
    and st.session_state.get("gold_version") not in (None, live_gold_version)
):
    st.success(
        "The set of articles that might interest you has been updated! "
        "Please refresh the page."
    )

# ── Header ─────────────────────────────────────────────────────────────────
header_left, header_right = st.columns([3, 1])
with header_left:
    st.caption("Events are filtered according to your registered territories and supply-chain keywords.")
    render_retention_notice()
with header_right:
    manual_refresh = st.button("Refresh now")

st.info(
    "Future developments of the stories presented here may later be affected by factors "
    "entirely unrelated to supply chains, which may thus not feature in this briefing."
)

# ── Metrics ────────────────────────────────────────────────────────────────
pipeline_status = get_gold_layer_status(user_id)
metrics = st.columns(4)
metrics[0].metric("User", profile.get("display_name") or user_id)
metrics[1].metric("Monitored territories", len(profile.get("territories", [])))
metrics[2].metric("Keywords", sum(len(v) for v in (profile.get("keywords") or {}).values()))
metrics[3].metric("Data status", pipeline_status)

# ── Profile update ─────────────────────────────────────────────────────────
# NOTE: the setup page is only routable until a profile exists, so there is no
# link to it here. See the note in the reply — preference editing needs a home.

# ── Briefing controls ──────────────────────────────────────────────────────
st.subheader("Briefing controls")
col1, col2 = st.columns([2, 1])
with col1:
    briefing_days = st.slider("Show risks from the last N days", 1, 90,
                              min(profile.get("briefing_days", 90), 90))
with col2:
    show_older = st.toggle("Include older-risk section", value=False)

selected_countries = st.multiselect(
    "Geographic focus",
    options=profile.get("territories", []),
    default=profile.get("territories", []),
)

# ── Data fetch (rate-limited) ──────────────────────────────────────────────
now = time.time()
if "last_data_fetch" not in st.session_state:
    st.session_state.last_data_fetch = 0

should_refresh = manual_refresh or (now - st.session_state.last_data_fetch >= DATA_REFRESH_SECONDS)

if should_refresh:
    st.session_state.cached_events       = get_events(user_id, briefing_days=briefing_days)
    # A true band: older than the main briefing window, up to the lookback limit,
    # so the "Older news" tab never repeats what the main briefing already lists.
    st.session_state.cached_older_events = get_events(
        user_id,
        max_age_days=profile.get("older_news_days", 180),
        min_age_days=briefing_days,
        exclude_archived=True,
    ) if show_older else []
    st.session_state.cached_summary      = get_events_summary(user_id)
    st.session_state.last_data_fetch     = now
    st.session_state.gold_version        = live_gold_version   # events now match this version
    if manual_refresh:
        st.success("Data refreshed.")

events       = st.session_state.get("cached_events", [])
older_events = st.session_state.get("cached_older_events", [])
summary      = st.session_state.get("cached_summary", [])

# ── Map ────────────────────────────────────────────────────────────────────
map_col, sidebar_col = st.columns([2, 1])
with map_col:
    render_heatmap(summary, selected_countries)
with sidebar_col:
    st.subheader("Radar status")
    st.write(f"Main briefing: last `{briefing_days}` days")
    st.write(
        f"Older-risk lookback: `{briefing_days}`–`{profile.get('older_news_days', 180)}` days"
    )
    st.write(f"Briefing events: `{len(events)}`")
    st.write("Red window → `Needs action from us`")
    st.write("Yellow window → `Look out for developments`")

# ── Briefing ───────────────────────────────────────────────────────────────
st.subheader("Radar Briefing")

# An empty briefing has two very different causes, and they must not look alike:
# the gold layer has never been built yet, or it has been built and nothing
# matched this user's filters.
if not events:
    if gold_never_built(system_status):
        render_first_build_notice()
    else:
        render_no_matches_notice()

render_briefing(events, selected_countries=selected_countries, older_events=older_events if show_older else [])

# ── Polling loop ───────────────────────────────────────────────────────────
time.sleep(STATUS_POLL_SECONDS)
st.rerun()
