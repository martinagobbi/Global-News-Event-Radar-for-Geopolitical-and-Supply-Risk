import streamlit as st

from components.briefing import render_briefing
from components.heatmap import render_heatmap
from configuration.countries import COUNTRY_OPTIONS
from configuration.sectors import RISK_CATEGORY_OPTIONS
from data.gold_layer import (
    get_briefing_events,
    get_dashboard_summary,
    get_gold_layer_status,
    get_older_events,
    get_system_status,
    trigger_gold_layer_computation,
)
from data.user_store import get_current_user, get_user_profile, is_first_login, save_user_profile


st.title("Dashboard")

user_id = get_current_user()

if is_first_login(user_id):
    st.warning("First-time access detected. Complete the initial setup before opening the dashboard.")
    st.page_link("pages/onboarding.py", label="Open setup", icon="🧭")
    st.stop()

profile = get_user_profile(user_id)
status = get_gold_layer_status(user_id)
system_status = get_system_status(user_id)

if system_status.get("status") == "ERROR":
    st.error(
        "Due to technical difficulties, this dashboard has not been updated since "
        f"{system_status.get('timestamp_of_last_update', 'an unknown time')}."
    )
header_left, header_right = st.columns([3, 1])
with header_left:
    st.caption(
        "The gold layer is user-specific: events reflect the registered countries and risk categories."
    )
with header_right:
    if st.button("Refresh gold layer"):
        trigger_gold_layer_computation(user_id)
        st.success("Gold layer refresh requested.")

st.info(
    "Future developments of the stories presented here may later be affected by factors entirely unrelated to supply chains, "
    "which may thus not feature in this briefing."
)

metrics = st.columns(4)
metrics[0].metric("User", profile.get("display_name", user_id))
metrics[1].metric("Monitored countries", len(profile.get("countries", [])))
metrics[2].metric("Risk categories", len(profile.get("risk_categories", [])))
metrics[3].metric("Gold layer status", status)

with st.expander("Update monitoring perimeter"):
    st.write("Update registered countries and risk categories. The radar logic remains controlled by the gold layer.")
    with st.form("update_profile_form"):
        updated_countries = st.multiselect(
            "Countries to monitor",
            options=COUNTRY_OPTIONS,
            default=[country for country in profile.get("countries", []) if country in COUNTRY_OPTIONS],
        )
        updated_categories = st.multiselect(
            "Relevant risk categories",
            options=RISK_CATEGORY_OPTIONS,
            default=[
                category
                for category in profile.get("risk_categories", [])
                if category in RISK_CATEGORY_OPTIONS
            ],
        )
        update_submitted = st.form_submit_button("Save and refresh gold layer")
    if update_submitted:
        updated_profile = {
            **profile,
            "countries": updated_countries,
            "risk_categories": updated_categories,
            "status": "registered",
        }
        save_user_profile(updated_profile)
        trigger_gold_layer_computation(user_id)
        st.success("Monitoring perimeter updated.")
        st.rerun()

st.subheader("Briefing controls")
control_col1, control_col2 = st.columns([2, 1])
with control_col1:
    briefing_days = st.slider(
        "Show risks from the last N days",
        min_value=1,
        max_value=30,
        value=min(profile.get("briefing_days", 30), 30),
        step=1,
    )
with control_col2:
    show_older_risks = st.toggle("Include older-risk section", value=False)

selected_countries = st.multiselect(
    "Geographic focus",
    options=profile.get("countries", []),
    default=profile.get("countries", []),
)

events = get_briefing_events(user_id, days=briefing_days)
older_events = get_older_events(user_id, profile.get("older_news_days", 90)) if show_older_risks else []
tagged_events = get_briefing_events(user_id, days=30)
summary = get_dashboard_summary(user_id)

map_col, summary_col = st.columns([2, 1])
with map_col:
    render_heatmap(summary, selected_countries)
with summary_col:
    st.subheader("Radar status")
    st.write(f"Main briefing: last `{briefing_days}` days")
    st.write(f"Optional older-risk lookback: `{profile.get('older_news_days', 90)}` days")
    st.write(f"Briefing events: `{len(events)}`")
    st.write("Red window: events tagged `Needs action from us`")
    st.write("Yellow window: events tagged `Look out for developments`")
    st.write("Archive: events tagged `Not important / Archive`")

st.subheader("Radar Briefing")
render_briefing(
    events,
    selected_countries=selected_countries,
    older_events=older_events,
    tagged_events=tagged_events,
)