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
    render_retention_notice(preferences="follows")
with header_right:
    manual_refresh = st.button("Refresh now")

# ── Silver watermark ───────────────────────────────────────────────────────
# max(DATEADDED) in silver as of the last publish: the newest GDELT slice this
# briefing was actually built from. Shown permanently, not only when something is
# wrong — the "technical difficulties" banner appears only once the pipeline is
# 45+ minutes behind (SILVER_STALE_SECONDS), and below that threshold a frozen
# pipeline looks exactly like a healthy one.
#
# Reported as a LAG IN SLICES against the clock, not as a raw age, because a raw
# age cannot be judged without knowing the cadence. Two separate 15-minute cycles
# are involved and they are not in phase:
#
#   * GDELT names slices on exact quarter hours, and publishes slightly early —
#     measured 2026-08-16, lastupdate.txt pointed at 18:00:00 while the clock read
#     17:58:29. So the newest id in existence can lead floor(now) by a slice.
#   * OUR poller runs every 15 minutes FROM STARTUP, so it is not aligned to the
#     quarter hour at all and can take a further full cycle to notice a slice.
#
# Both push the same way, so being one slice behind is the normal resting state,
# not a fault. floor(now) is used as the reference precisely because it can only
# UNDERSTATE the lag — it never invents a problem that is not there.
_SLICE = 15 * 60
# The last slice the committed seed covers — kept in step with SEED_LAST_SLICE in
# bootstrap/silver_snapshot.sh. Anything later can only have arrived live, which
# is what separates "never started" from "started and stopped" below.
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
        # A large lag has two completely different causes, and saying the wrong
        # one is worse than saying nothing:
        #
        #   * the store holds ONLY the committed seed, which is what a fresh
        #     clone looks like before its first live slice; or
        #   * live slices did arrive and then STOPPED — the machine slept, the
        #     network dropped, or the processing layer wedged.
        #
        # They are told apart by the watermark itself, not by how big the lag is.
        # The seed ends at a fixed slice (SEED_LAST_SLICE in
        # bootstrap/silver_snapshot.sh); anything after it can only have come
        # from live ingestion. Judging by lag alone got this wrong on 2026-08-17,
        # telling the user "live ingestion has not delivered a slice yet" while
        # they were looking at live data from 23:00 the previous night.
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
        # Never let an unparseable value hide the raw one; it is diagnostic.
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

# ── Metrics ────────────────────────────────────────────────────────────────
#pipeline_status = get_gold_layer_status(user_id)
#metrics = st.columns(4)
#metrics[0].metric("User", user_id)
#metrics[1].metric("Monitored territories", len(profile.get("territories", [])))
#metrics[2].metric("Keywords", sum(len(v) for v in (profile.get("keywords") or {}).values()))
#metrics[3].metric("Data status", pipeline_status)

# ── Profile update ─────────────────────────────────────────────────────────
# NOTE: the setup page is only routable until a profile exists, so there is no
# link to it here. See the note in the reply — preference editing needs a home.

# ── Data fetch (rate-limited) ──────────────────────────────────────────────
now = time.time()
if "last_data_fetch" not in st.session_state:
    st.session_state.last_data_fetch = 0

should_refresh = manual_refresh or (now - st.session_state.last_data_fetch >= DATA_REFRESH_SECONDS)

if should_refresh:
    st.session_state.cached_events       = get_events(user_id)
    st.session_state.cached_older_events = get_events(
        user_id,
        max_age_days=profile.get("older_news_days", 180),
        min_age_days=profile.get("briefing_days", default_briefing_days),
        exclude_archived=True,
    )
    st.session_state.cached_summary      = get_events_summary(user_id)
    st.session_state.last_data_fetch     = now
    st.session_state.gold_version        = live_gold_version
    if manual_refresh:
        st.success("Data refreshed.")

events       = st.session_state.get("cached_events", [])
older_events = st.session_state.get("cached_older_events", [])
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
        🔴 **Points** — monitored geographic locations.  
        Larger points indicate more events.

        🔥 **Heatmap** — concentration of events.  
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

        **Older-risk lookback:** {briefing_days} to \
{profile.get("older_news_days", 180)} days ago  
        *(editable in Preferences)*

        **Briefing events:** {len(events)}
        """
    )
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

render_briefing(events, selected_countries=profile.get("territories", []), older_events=older_events)

# ── Polling loop ───────────────────────────────────────────────────────────
time.sleep(STATUS_POLL_SECONDS)
st.rerun()
