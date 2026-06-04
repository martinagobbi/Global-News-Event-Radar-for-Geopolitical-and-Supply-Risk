import streamlit as st

from configuration.countries import COUNTRY_OPTIONS
from configuration.sectors import RISK_CATEGORY_OPTIONS
from data.gold_layer import trigger_gold_layer_computation
from data.user_store import get_current_user, get_user_profile, is_first_login, save_user_profile


st.title("User setup")
st.caption("Initial configuration for the user-specific gold layer.")

user_id = get_current_user()
first_login = is_first_login(user_id)

if not first_login:
    st.info("This user is already registered. You can update the monitoring perimeter below.")

st.write(
    "Choose the countries and risk categories that define the supply chain monitoring perimeter. "
    "These settings can be updated later without changing the briefing logic itself."
)

profile = get_user_profile(user_id)
default_countries = profile.get("countries") or ["Italy", "Germany", "United States"]
default_categories = profile.get("risk_categories") or [
    "Recent supply-side transit-related accidents",
    "Civil movements",
    "Major supply-side accidents or breakdowns",
]

with st.form("onboarding_form"):
    display_name = st.text_input("Display name", value=profile.get("display_name", "Demo User"))
    monitored_countries = st.multiselect(
        "Countries to monitor",
        options=COUNTRY_OPTIONS,
        default=[country for country in default_countries if country in COUNTRY_OPTIONS],
    )
    risk_categories = st.multiselect(
        "Relevant risk categories",
        options=RISK_CATEGORY_OPTIONS,
        default=[category for category in default_categories if category in RISK_CATEGORY_OPTIONS],
    )
    briefing_days = st.slider("Default briefing window (days)", 1, 30, profile.get("briefing_days", 30), 1)
    older_news_days = st.slider("Optional older-risk lookback (days)", 31, 180, profile.get("older_news_days", 90), 1)
    submitted = st.form_submit_button("Save profile and refresh gold layer")

if submitted:
    profile = {
        "user_id": user_id,
        "display_name": display_name,
        "countries": monitored_countries,
        "risk_categories": risk_categories,
        "briefing_days": briefing_days,
        "older_news_days": older_news_days,
        "status": "registered",
    }
    save_user_profile(profile)
    trigger_gold_layer_computation(user_id)
    st.success("Profile saved and gold layer refresh requested.")
    st.page_link("pages/2_dashboard.py", label="Open dashboard", icon="📊")