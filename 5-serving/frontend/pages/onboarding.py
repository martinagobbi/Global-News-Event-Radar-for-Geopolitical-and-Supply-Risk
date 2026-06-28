import streamlit as st

from configuration.countries import COUNTRY_OPTIONS
# from configuration.sectors import RISK_CATEGORY_OPTIONS  # replaced by the keyword form
from components.keyword_form import render_keyword_questions
from data.user_store import get_current_user, get_user_profile, is_first_login, save_user_profile


st.title("User setup")
st.caption("Configure your supply chain monitoring perimeter.")

user_id = get_current_user()

if not is_first_login(user_id):
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

profile = get_user_profile(user_id)

display_name = st.text_input("Display name", value=profile.get("display_name", ""))
# The list includes sovereign countries AND autonomous territories. Stored under
# the "territories" profile key — the contract that 4-processing/countries.py
# reads via codes_for_names().
monitored_territories = st.multiselect(
    "Territories to monitor",
    options=COUNTRY_OPTIONS,
    default=[c for c in profile.get("territories", []) if c in COUNTRY_OPTIONS],
)

st.subheader("Your supply chain")
st.caption("Add one item at a time. Leave a question empty to ignore it.")
keywords = render_keyword_questions(profile, prefix="onboard")

briefing_days = st.slider(
    "Default briefing window (days)", 1, 30, profile.get("briefing_days", 30)
)
older_news_days = st.slider(
    "Optional older-risk lookback (days)", 31, 180, profile.get("older_news_days", 90)
)

if st.button("Save profile", type="primary"):
    save_user_profile({
        "user_id":         user_id,
        "display_name":    display_name,
        "territories":     monitored_territories,
        "keywords":        keywords,
        "briefing_days":   briefing_days,
        "older_news_days": older_news_days,
        "status":          "registered",
    })
    st.success("Profile saved.")
    st.page_link("pages/dashboard.py", label="Open dashboard", icon="📊")
