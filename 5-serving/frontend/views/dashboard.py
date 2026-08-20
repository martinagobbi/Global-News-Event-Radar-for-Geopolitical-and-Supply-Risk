import time
from datetime import datetime, timezone

import streamlit as st

from auth import require_auth

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
from data.user_store import get_user_profile, is_first_login
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

default_briefing_days = 30

use_neutral_spinner()
st.title("Radar View")

user_id = require_auth()

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

if status_code == "503-POSTGRES":
    st.error(
        "🔴 **503 — Database unavailable.** "
        + system_status.get(
            "message",
            "The backend could not reach the PostgreSQL database after multiple attempts.",
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
        "Due to technical difficulties, this dashboard has not been updated with the latest events since "
        f"{last_update} (UTC)."
    )

# ── Preferences just changed: the pool is being rebuilt ────────────────────
if recompute_pending(live_gold_version):
    render_recompute_notice()

# ── "Your articles changed" nudge (green = good news) ──────────────────────
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
    render_retention_notice(preferences="follows")
with header_right:
    manual_refresh = st.button("Refresh now")

# ── Silver watermark ───────────────────────────────────────────────────────
_SLICE = 15 * 60
_SEED_LAST_SLICE = "20260727171500"
_watermark = system_status.get("silver_watermark")
if _watermark:
    try:
        _wm_dt = datetime.strptime(_watermark, "%Y%m%d%H%M%S").replace(
            tzinfo=timezone.utc)
        _now = datetime.now(timezone.utc)
        _expected = _now.replace(minute=(_now.minute // 15) * 15,
                                 second=0, microsecond=0)
        _lag = max(0, int((_expected - _wm_dt).total_seconds() // _SLICE))
        _light = "🟢" if _lag <= 1 else ("🟡" if _lag == 2 else "🔴")
        _behind = ("up to date" if _lag == 0 else
                   f"{_lag} slice{'s' if _lag > 1 else ''} behind")
        st.caption(
            f"{_light} Newest data held: **{_wm_dt:%Y-%m-%d %H:%M} UTC** "
            f"({_behind}). Latest GDELT slice now: {_expected:%Y-%m-%d %H:%M} UTC."
        )
        if _lag >= 8:
            if _watermark <= _SEED_LAST_SLICE:
                st.caption(
                    "This is the shipped seed — live ingestion has not delivered "
                    "a slice yet. The first arrives within ~15 minutes of the "
                    "pipeline starting, and this line moves to today when it does."
                )
            else:
                st.caption(
                    f"Live data reached {_wm_dt:%Y-%m-%d %H:%M} UTC and then "
                    "stopped. Ingestion, parsing, validation and processing are "
                    "all worth checking — a machine that slept, or a processing "
                    "layer that cannot publish, both look like this."
                )
    except ValueError:
        st.caption(f"Silver watermark: `{_watermark}` (unrecognised format)")
else:
    st.caption(
        "Newest data held: not recorded yet — shown once the pipeline "
        "next publishes."
    )

st.info(
    "Future developments of the stories presented here may later be affected by factors "
    "entirely unrelated to supply chains, which may thus not feature in this briefing."
)

# ── Data fetch (rate-limited) ──────────────────────────────────────────────
now = time.time()
if "last_data_fetch" not in st.session_state:
    st.session_state.last_data_fetch = 0

should_refresh = manual_refresh or (now - st.session_state.last_data_fetch >= DATA_REFRESH_SECONDS)

if should_refresh:
    st.session_state.cached_events       = get_events(user_id)
    st.session_state.cached_summary      = get_events_summary(user_id)
    st.session_state.last_data_fetch     = now
    st.session_state.gold_version        = live_gold_version
    if manual_refresh:
        st.success("Data refreshed.")

events       = st.session_state.get("cached_events", [])
summary      = st.session_state.get("cached_summary", [])

# ── Map ────────────────────────────────────────────────────────────────────
st.subheader("Heatmap")
render_heatmap(
    summary,
    profile.get("territories", []),
)

legend_col, status_col = st.columns(2)

with legend_col:
    st.subheader("Map legend")
    st.markdown(
        """
        **Points** — monitored geographic locations.  
        Larger points indicate more events.

        **Heatmap** — concentration of events.  
        Brighter areas indicate a higher concentration of events.

        *Colours indicate event concentration, not risk severity.*
        """
    )

with status_col:
    briefing_days = profile.get(
        "briefing_days",
        default_briefing_days,
    )

    st.subheader("Radar status")

    st.markdown(
        f"""
        **Main briefing:** last {briefing_days} days  
        *(editable in Preferences)*

        **Events:** {len(events)}
        """
    )

# ── Briefing ───────────────────────────────────────────────────────────────
st.subheader("Radar Briefing")

if not events:
    if gold_never_built(system_status):
        render_first_build_notice()
    else:
        render_no_matches_notice()

# The older_events parameter is now removed
render_briefing(events, selected_countries=profile.get("territories", []))

# ── Polling loop ───────────────────────────────────────────────────────────
time.sleep(STATUS_POLL_SECONDS)
st.rerun()