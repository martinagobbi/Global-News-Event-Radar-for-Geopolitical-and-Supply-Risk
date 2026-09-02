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

import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

@st.cache_resource
def get_app_start_time():
    """
    Records the time the app first started running. 
    Because it's cached as a resource, this only executes once for the 
    lifetime of the Streamlit server / Docker container.
    """
    return time.time()

STATUS_POLL_SECONDS  = 30
DATA_REFRESH_SECONDS = 900   # 15 minutes — aligned with ingestion cadence

default_briefing_days = 60

use_neutral_spinner()
st.title("Radar View")

user_id = require_auth()

try:
    if is_first_login(user_id):
        st.warning("First-time access detected. Complete the initial setup before opening the dashboard.")
        st.stop()

    profile = get_user_profile(user_id)
    briefing_days = int(profile.get("briefing_days") or default_briefing_days)
    territory_options = get_territory_options()

    user_tz = profile.get("timezone", "Europe/Rome")
    tz = ZoneInfo(user_tz)

    # ── Pipeline / store status (always re-fetched, cheap) ─────────────────────
    system_status = get_system_status()
    live_gold_version = get_events_version(user_id)
except BackendUnavailable:
    st.error("🔴 The backend is unreachable. Please try again shortly.")
    st.stop()

status_code = system_status.get("code")
gold_changed = (
    live_gold_version is not None
    and st.session_state.get("gold_version") not in (None, live_gold_version)
)

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
        f"🟠 **503 — Please wait {STATUS_POLL_SECONDS} seconds for the profile service to become available.** "
        + system_status.get(
            "message",
            "The backend could not reach the MongoDB database after multiple attempts.",
        )
        + " Events may still be visible, but your tags and saved preferences "
        "might not load or save correctly until this is resolved."
    )

# The part below was for debugging purposes. It is now unreliable, and too generic at any rate. Commenting out.
#elif system_status.get("status") == "ERROR":
#    last_update_raw = system_status.get("timestamp_of_last_update")
#    if last_update_raw:
#        try:
#            last_update_dt = datetime.fromisoformat(str(last_update_raw).replace("Z", "+00:00")).astimezone(tz)
#            last_update_str = f"{last_update_dt:%Y-%m-%d %H:%M} {last_update_dt:%Z}"
#        except ValueError:
#            last_update_str = str(last_update_raw)
#    else:
#        last_update_str = "an unknown time"
#
#    st.error(
#        "The system has been having technical difficulties since "
#        f"{last_update_str}."
#        " (This does not necessarily mean that newly-ingested data will not appear on this dashboard.)"
#    )

# ── Preferences just changed: the pool is being rebuilt ────────────────────
if recompute_pending(live_gold_version):
    render_recompute_notice()

# ── Gold changes trigger the normal fetch below, which also regroups cards. ──
elif gold_changed:
    st.success("The set of articles that might interest you has been updated.")

# ── Header ─────────────────────────────────────────────────────────────────
header_left, header_right = st.columns([3, 1])
events = st.session_state.get("cached_events", [])
with header_left:
    st.caption(f"Events are filtered according to your registered territories and supply-chain keywords. The **{len(events)}** events from the last {briefing_days} days are shown.")
    render_retention_notice(preferences="follows")
with header_right:
    manual_refresh = st.button("Refresh now")

# ── Silver watermark ───────────────────────────────────────────────────────
_SLICE = 15 * 60
_SEED_FIRST_SLICE = "20260627171500"
_SEED_LAST_SLICE  = "20260727171500"
_watermark = system_status.get("silver_watermark")
if _watermark:
    try:
        _wm_dt = datetime.strptime(_watermark, "%Y%m%d%H%M%S").replace(
            tzinfo=timezone.utc)
        _now = datetime.now(timezone.utc)
        _expected = _now.replace(minute=(_now.minute // 15) * 15,
                                 second=0, microsecond=0)
        _lag = max(0, int((_expected - _wm_dt).total_seconds() // _SLICE))

        if _SEED_FIRST_SLICE <= _watermark <= _SEED_LAST_SLICE:
            # Calculate elapsed time in minutes since the container/app started
            elapsed_minutes = (time.time() - get_app_start_time()) / 60

            if elapsed_minutes > 30:
                st.caption(
                    "🔴 **Test seed data**. "
                    "This is the shipped data for testing. Live ingestion "
                    "has not delivered a slice yet. Live data arrives every "
                    "~15 minutes (the rate at which GDELT publishes this data) "
                    "starting when this pipeline starts, and this line moves to "
                    "today when it does."
                )
            elif elapsed_minutes > 15:
                st.caption(
                    "🟡 **Test seed data**. "
                    "This is the shipped data for testing. Live ingestion "
                    "has not delivered a slice yet. Live data arrives every "
                    "~15 minutes (the rate at which our source GDELT publishes this data) "
                    "starting when this pipeline starts, and this line moves to "
                    "today when it does."
                )
            else:
                st.caption(
                    "🟢 **Test seed data**. "
                    "This is the shipped data for testing. Live ingestion "
                    "has not delivered a slice yet. Live data arrives every "
                    "~15 minutes (the rate at which GDELT publishes this data) "
                    "starting when this pipeline starts, and this line moves to "
                    "today when it does."
                )
        else:
            _light = "🟢" if _lag <= 1 else ("🟡" if _lag == 2 else "🔴")
            _behind = ((_lag == 0)*"up to date" +
                       (_lag == 1)*"latest slice scheduled to be displayable in a few minutes" +
                       (_lag > 1)*f"{_lag} slices behind. The pipeline may have been turned off for a while, or part of GDELT may be down. In either case, please wait longer, between 15 minutes and an hour.")

            _wm_local = _wm_dt.astimezone(tz)
            _expected_local = _expected.astimezone(tz)
            _tz_name = _wm_local.strftime("%Z")

            st.caption(
                f"{_light} Newest data held: **{_wm_local:%Y-%m-%d %H:%M} {_tz_name}** ({_behind})."
            )
            st.caption(
                " You should see the timestamp above being updated every 15 minutes: the rate at which"
                " GDELT (our source) publishes the latest news."
                #" GDELT publishes new data every quarter of an hour (:00, :15, :30, :45)."
                #f" Latest GDELT slice now: {_expected_local:%Y-%m-%d %H:%M} {_tz_name}."
            )
            if _lag >= 8:
                st.caption(
                    f"Live data reached {_wm_local:%Y-%m-%d %H:%M} {_tz_name} and then "
                    "stopped. Ingestion, parsing, validation and processing are "
                    "all worth checking — a machine that slept, or a processing "
                    "layer that cannot publish, both look like this."
                )
    except ValueError:
        st.caption(f"Silver watermark: `{_watermark}` (unrecognised format)")
else:
    st.caption(
        "Newest data held: not recorded yet — shown once the pipeline "
        "next publishes, in max 15 to 30 minutes, only possible after GDELT publishes new data."
    )

st.info(
    "Future developments of the stories presented here may later be affected by factors "
    "entirely unrelated to supply chains, which may thus not feature in this briefing."
)

# ── Data fetch (rate-limited) ──────────────────────────────────────────────
now = time.time()
if "last_data_fetch" not in st.session_state:
    st.session_state.last_data_fetch = 0

first_load = "cached_events" not in st.session_state

should_refresh = (
    manual_refresh
    or first_load
    or gold_changed
    or (now - st.session_state.last_data_fetch >= DATA_REFRESH_SECONDS)
)

if should_refresh:
    st.session_state.cached_events = get_events(user_id, briefing_days=briefing_days,)
    st.session_state.cached_summary = get_events_summary(user_id, briefing_days=briefing_days)
    st.session_state.last_data_fetch     = now
    st.session_state.gold_version        = live_gold_version
    if manual_refresh:
        st.success("Data refreshed.")

summary      = st.session_state.get("cached_summary", [])

st.subheader("Heatmap")

with st.expander("View heatmap", expanded=False):
    render_heatmap(
        summary,
        profile.get("territories", []),
    )
st.markdown(
    """
    Points are monitored geographic locations. Larger points indicate more events. The heat signature's colour changes accordingly.
    """
)

st.subheader("Table of Events")

if not events:
    if gold_never_built(system_status):
        render_first_build_notice()
    else:
        render_no_matches_notice()

render_briefing(events, selected_countries=profile.get("territories", []))

# ── Polling loop ───────────────────────────────────────────────────────────
time.sleep(STATUS_POLL_SECONDS)
st.rerun()
