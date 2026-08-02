import streamlit as st

from components.event_card import render_event_card
from data.api_client import BackendUnavailable
from data.gold_layer import get_tagged_events
from data.user_store import get_current_user, is_first_login


st.title("Looking out for developments")

user_id = get_current_user()

try:
    if is_first_login(user_id):
        st.warning("This list becomes available after first-time setup.")
        st.stop()

    st.caption(
        "Events you flagged with 'Look out for developments' in the Radar View. "
        "This list is yours alone."
    )
    events = get_tagged_events(user_id, "monitor")
except BackendUnavailable:
    st.error("🔴 The backend is unreachable. Please try again shortly.")
    st.stop()

if not events:
    st.info("No events flagged for monitoring yet.")
else:
    for event in events:
        render_event_card(event)
