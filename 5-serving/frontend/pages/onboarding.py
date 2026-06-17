import streamlit as st

from configuration.countries import COUNTRY_OPTIONS
from configuration.sectors import RISK_CATEGORY_OPTIONS
from data.user_store import get_current_user, get_user_profile, is_first_login, save_user_profile


st.title("User setup")
st.caption("Configure your supply chain monitoring perimeter.")

user_id = get_current_user()

if not is_first_login(user_id):
    st.info("This user is already registered. You can update the monitoring perimeter below.")

st.write(
    "Choose the countries and risk categories that define your monitoring perimeter. "
    "These settings can be updated at any time from the dashboard."
)

profile = get_user_profile(user_id)

with st.form("onboarding_form"):
    display_name = st.text_input("Display name", value=profile.get("display_name", ""))
    monitored_countries = st.multiselect(
        "Countries to monitor",
        options=COUNTRY_OPTIONS,
        default=[c for c in profile.get("countries", []) if c in COUNTRY_OPTIONS],
    )
    risk_categories = st.multiselect(
        "Relevant risk categories",
        options=RISK_CATEGORY_OPTIONS,
        default=[c for c in profile.get("risk_categories", []) if c in RISK_CATEGORY_OPTIONS],
    )
    briefing_days = st.slider(
        "Default briefing window (days)", 1, 30, profile.get("briefing_days", 30)
    )
    older_news_days = st.slider(
        "Optional older-risk lookback (days)", 31, 180, profile.get("older_news_days", 90)
    )
    submitted = st.form_submit_button("Save profile")

if submitted:
    save_user_profile({
        "user_id":         user_id,
        "display_name":    display_name,
        "countries":       monitored_countries,
        "risk_categories": risk_categories,
        "briefing_days":   briefing_days,
        "older_news_days": older_news_days,
        "status":          "registered",
    })
    st.success("Profile saved.")
    st.page_link("pages/dashboard.py", label="Open dashboard", icon="📊")