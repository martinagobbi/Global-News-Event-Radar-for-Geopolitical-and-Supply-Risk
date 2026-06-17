import time

import streamlit as st

from components.briefing import render_briefing
from components.heatmap import render_heatmap
from configuration.countries import COUNTRY_OPTIONS
from configuration.sectors import RISK_CATEGORY_OPTIONS
from data.gold_layer import (
    get_archived_events,
    get_events,
    get_events_summary,
    get_gold_layer_status,
    get_system_status,
)
from data.user_store import get_current_user, get_user_profile, is_first_login, save_user_profile


STATUS_POLL_SECONDS  = 30
DATA_REFRESH_SECONDS = 900   # 15 minutes — aligned with ingestion cadence


st.title("Dashboard")

user_id = get_current_user()

if is_first_login(user_id):
    st.warning("First-time access detected. Complete the initial setup before opening the dashboard.")
    st.page_link("pages/onboarding.py", label="Open setup", icon="🧭")
    st.stop()

profile = get_user_profile(user_id)

# ── Pipeline status (always re-fetched, cheap) ─────────────────────────────
system_status = get_system_status()
if system_status.get("status") == "ERROR":
    last_update = system_status.get("timestamp_of_last_update", "an unknown time")
    st.error(
        "Due to technical difficulties, this dashboard has not been updated since "
        f"{last_update}."
    )

# ── Header ─────────────────────────────────────────────────────────────────
header_left, header_right = st.columns([3, 1])
with header_left:
    st.caption("Events are filtered according to your registered countries and risk categories.")
with header_right:
    manual_refresh = st.button("Refresh now")

st.info(
    "Future developments of the stories presented here may later be affected by factors "
    "entirely unrelated to supply chains, which may thus not feature in this briefing."
)

# ── Metrics ────────────────────────────────────────────────────────────────
pipeline_status = get_gold_layer_status(user_id)
metrics = st.columns(4)
metrics[0].metric("User", profile.get("display_name", user_id))
metrics[1].metric("Monitored countries", len(profile.get("countries", [])))
metrics[2].metric("Risk categories", len(profile.get("risk_categories", [])))
metrics[3].metric("Data status", pipeline_status)

# ── Profile update ─────────────────────────────────────────────────────────
with st.expander("Update monitoring perimeter"):
    with st.form("update_profile_form"):
        updated_countries = st.multiselect(
            "Countries to monitor",
            options=COUNTRY_OPTIONS,
            default=[c for c in profile.get("countries", []) if c in COUNTRY_OPTIONS],
        )
        updated_categories = st.multiselect(
            "Relevant risk categories",
            options=RISK_CATEGORY_OPTIONS,
            default=[c for c in profile.get("risk_categories", []) if c in RISK_CATEGORY_OPTIONS],
        )
        update_submitted = st.form_submit_button("Save")
    if update_submitted:
        save_user_profile({**profile, "countries": updated_countries, "risk_categories": updated_categories})
        st.session_state.last_data_fetch = 0
        st.success("Monitoring perimeter updated.")
        st.rerun()

# ── Briefing controls ──────────────────────────────────────────────────────
st.subheader("Briefing controls")
col1, col2 = st.columns([2, 1])
with col1:
    briefing_days = st.slider("Show risks from the last N days", 1, 30, min(profile.get("briefing_days", 30), 30))
with col2:
    show_older = st.toggle("Include older-risk section", value=False)

selected_countries = st.multiselect(
    "Geographic focus",
    options=profile.get("countries", []),
    default=profile.get("countries", []),
)

# ── Data fetch (rate-limited) ──────────────────────────────────────────────
now = time.time()
if "last_data_fetch" not in st.session_state:
    st.session_state.last_data_fetch = 0

should_refresh = manual_refresh or (now - st.session_state.last_data_fetch >= DATA_REFRESH_SECONDS)

if should_refresh:
    st.session_state.cached_events       = get_events(user_id, briefing_days=briefing_days)
    st.session_state.cached_older_events = get_events(user_id, max_age_days=profile.get("older_news_days", 90), exclude_archived=True) if show_older else []
    st.session_state.cached_summary      = get_events_summary(user_id)
    st.session_state.last_data_fetch     = now
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
    st.write(f"Older-risk lookback: `{profile.get('older_news_days', 90)}` days")
    st.write(f"Briefing events: `{len(events)}`")
    st.write("Red window → `Needs action from us`")
    st.write("Yellow window → `Look out for developments`")

# ── Briefing ───────────────────────────────────────────────────────────────
st.subheader("Radar Briefing")
render_briefing(events, selected_countries=selected_countries, older_events=older_events if show_older else [])

# ── Polling loop ───────────────────────────────────────────────────────────
time.sleep(STATUS_POLL_SECONDS)
st.rerun()