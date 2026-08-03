import streamlit as st

from configuration.countries import get_territory_options
# from configuration.sectors import RISK_CATEGORY_OPTIONS  # replaced by the keyword form
from components.branding import use_neutral_spinner
from components.keyword_form import render_keyword_questions
from data.api_client import BackendUnavailable
from data.user_store import get_current_user, get_user_profile, is_first_login, save_user_profile


use_neutral_spinner()
st.title("User setup")
st.caption("Configure your supply chain monitoring perimeter.")

user_id = get_current_user()

try:
    already_registered = not is_first_login(user_id)
    profile = get_user_profile(user_id)
    territory_options = get_territory_options()
except BackendUnavailable:
    st.error("🔴 The backend is unreachable. Please try again shortly.")
    st.stop()

if already_registered:
    st.info("This user is already registered. You can update the monitoring perimeter below.")

st.write(
    "Please choose the territories you want to monitor and describe your supply chain by answering "
    "the questions below. These settings can be updated at any time from the dashboard."
    "When answering, you may wish to consider every part of your supply chain: "
    "Sourcing, manufacturing, storage, and delivery."
    "You may also wish to consider whether companies involved have a principal place of business "
    "or a country of incorporation that is different from those already involved in your "
    "supply chain. "
    "As well as other territories, there exists one entry per country."
)

st.markdown("**Territories to monitor**")
st.caption(
    "If any part of your supply chain is in or is affected by any one of these "
    "territories, feel free to include it. Please remember that in “Radar View”, "
    "you will only see news regarding events on these territories"
)
# The list includes sovereign countries AND autonomous territories. Stored under
# the "territories" profile key — the contract that 4-processing/countries.py
# reads via codes_for_names().
monitored_territories = st.multiselect(
    "Territories to monitor",
    options=territory_options,
    default=[c for c in profile.get("territories", []) if c in territory_options],
    label_visibility="collapsed",
)

st.subheader("Your supply chain")
st.caption("Add one item at a time. Leave a question empty to ignore it.")
keywords = render_keyword_questions(profile, prefix="onboard")

briefing_days = st.slider(
    "Default briefing window (days)", 1, 90, min(profile.get("briefing_days", 90), 90)
)
st.caption(
    "How far back the Radar View reaches by default. 90 days keeps a freshly "
    "loaded 30-day backfill visible for months."
)
older_news_days = st.slider(
    "Optional older-risk lookback (days)", 31, 365,
    max(profile.get("older_news_days", 180), 31)
)
st.caption(
    "Upper limit of the separate “Older news” tab. That tab shows events OLDER "
    "than the briefing window above, up to this limit — so keep it larger than "
    "the briefing window, or the tab will be empty."
)

if st.button("Save profile", type="primary"):
    try:
        save_user_profile({
            "user_id":         user_id,
            "territories":     monitored_territories,
            "keywords":        keywords,
            "briefing_days":   briefing_days,
            "older_news_days": older_news_days,
            "status":          "registered",
        })
    except BackendUnavailable:
        st.error(
            "🔴 Couldn't save — the profile database is temporarily unavailable. "
            "Your account was not created. Please try again shortly."
        )
        st.stop()
    st.success("Profile saved.")
    st.page_link("pages/dashboard.py", label="Open dashboard", icon="📊")
