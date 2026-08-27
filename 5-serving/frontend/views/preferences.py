import streamlit as st

from auth import require_auth

from configuration.countries import get_territory_options
# from configuration.sectors import RISK_CATEGORY_OPTIONS  # replaced by the keyword form
from components.branding import use_neutral_spinner
from components.keyword_form import render_keyword_questions
from components.recompute_notice import mark_recompute_pending, render_recompute_notice
from data.api_client import BackendUnavailable
from data.gold_layer import get_events_version
from data.user_store import get_user_profile, is_first_login, save_user_profile
from zoneinfo import available_timezones


use_neutral_spinner()

st.title("User setup")
st.caption("Configure your supply chain monitoring perimeter.")

user_id = require_auth()

try:
    already_registered = not is_first_login(user_id)
    profile = get_user_profile(user_id)
    territory_options = get_territory_options()
except BackendUnavailable:
    st.error("🔴 The backend is unreachable. Please try again shortly.")
    st.stop()


st.write(
    "Please choose the territories you want to monitor and describe your supply chain by answering "
    "the questions below. These settings can be updated at any time from the dashboard. "
    "When answering, you may wish to consider every part of your supply chain: "
    "sourcing, manufacturing, storage, and delivery. "
    "You may also wish to consider whether companies involved have a principal place of business "
    "or a country of incorporation that is different from those already involved in your "
    "supply chain. "
    "As well as other territories, there exists one entry per country."
)


# Top Save Profile button
if already_registered:
    st.write(
        '**PLEASE REMEMBER TO CLICK "SAVE PROFILE" TO SAVE ANY EDITS YOU MAKE ON THIS PAGE.**'
    )

save_top = st.button(
    "Save profile",
    type="primary",
    key="save_profile_top",
)


st.caption("---------------------------------------------------------")


st.subheader("Territories to monitor")

st.caption(
    "If any part of your supply chain is in or is affected by any one of these "
    "territories, feel free to include it. Please remember that in “Radar View”, "
    "you will only see news regarding events on these territories."
)

# The list includes sovereign countries AND autonomous territories.
# Stored under the "territories" profile key — the contract that
# 4-processing/countries.py reads via codes_for_names().
monitored_territories = st.multiselect(
    "Territories to monitor",
    options=territory_options,
    default=[
        c
        for c in profile.get("territories", [])
        if c in territory_options
    ],
    label_visibility="collapsed",
)


st.caption("---------------------------------------------------------")


st.subheader("Your supply chain")

st.caption(
    "Please add one item at a time. To ignore a question, you may leave it empty."
)

st.caption(
    "**IMPORTANT**: For legal reasons, we can scrape article titles, not article bodies. "
    "In the fields below, **please add words that you believe are very likely to appear "
    "in the titles of articles that might interest you**. However, there is no downside "
    "to adding esoteric words as well. You can add up to 1000 words per question."
)

keywords = render_keyword_questions(
    profile,
    prefix="onboard",
)


st.caption("---------------------------------------------------------")


st.subheader("Default briefing window (days)")

briefing_days = st.slider(
    "",
    1,
    90,
    min(profile.get("briefing_days", 60), 90),
    key="briefing_days",
)

st.caption(
    "How far back the Radar View reaches by default. "
    "60 days keeps a freshly loaded 30-day backfill visible for months."
)


st.caption("---------------------------------------------------------")


st.subheader("Optional older-risk lookback (days)")

older_news_days = st.slider(
    "",
    31,
    365,
    max(profile.get("older_news_days", 180), 31),
    key="older_news_days",
)

st.caption(
    "Upper limit of the separate “Older news” tab. That tab shows events OLDER "
    "than the briefing window above, up to this limit — so keep it larger than "
    "the briefing window, or the tab will be empty."
)


st.caption("---------------------------------------------------------")


st.subheader("Time Zone")

st.caption(
    "Select your local time zone for dates and timestamps across the dashboard."
)

all_tzs = sorted(available_timezones())

DEFAULT_TZ = "Europe/Rome"

current_tz = profile.get("timezone", DEFAULT_TZ)

default_idx = (
    all_tzs.index(current_tz)
    if current_tz in all_tzs
    else all_tzs.index(DEFAULT_TZ)
)

selected_timezone = st.selectbox(
    "Preferred time zone",
    options=all_tzs,
    index=default_idx,
    key="preferred_timezone",
)


st.caption("---------------------------------------------------------")


# Bottom Save Profile button
if already_registered:
    st.write(
        '**PLEASE REMEMBER TO CLICK "SAVE PROFILE" TO SAVE ANY EDITS YOU MAKE ON THIS PAGE.**'
    )

save_bottom = st.button(
    "Save profile",
    type="primary",
    key="save_profile_bottom",
)


if save_top or save_bottom:

    # Read the gold fingerprint BEFORE saving:
    # the rebuild is triggered by the save itself, so a fingerprint taken
    # afterwards could already be the new one and the notice would never appear.
    try:
        version_before_save = get_events_version(user_id)
    except BackendUnavailable:
        version_before_save = None

    try:
        save_user_profile(
            {
                "user_id": user_id,
                "territories": monitored_territories,
                "keywords": keywords,
                "briefing_days": briefing_days,
                "older_news_days": older_news_days,
                "status": "registered",
                "timezone": selected_timezone,
            }
        )

    except BackendUnavailable:
        st.error(
            "🔴 Couldn't save — the profile database is temporarily unavailable. "
            "Your account was not created. Please try again shortly."
        )
        st.stop()

    st.success("Profile saved.")

    # The save has triggered a rebuild of this user's article pool;
    # say so here and keep saying it on the dashboard until the new pool lands.
    mark_recompute_pending(version_before_save)
    render_recompute_notice()

    st.page_link(
        "views/dashboard.py",
        label="Open Radar View",
    )