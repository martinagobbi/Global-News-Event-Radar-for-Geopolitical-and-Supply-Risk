import streamlit as st

from auth import require_auth

from components.event_card import render_event_card
from components.retention_notice import render_retention_notice
from data.api_client import BackendUnavailable
from data.gold_layer import get_tagged_events
from data.user_store import is_first_login


st.title("Needs action")

user_id = require_auth()

try:
    if is_first_login(user_id):
        st.warning("This list becomes available after first-time setup.")
        st.stop()

    st.caption(
        "Events you flagged with 'Needs action from us' in the Radar View. "
        "This list is yours alone."
    )
    render_retention_notice(preferences="pinned")
    events = get_tagged_events(user_id, "requires_action")
except BackendUnavailable:
    st.error("🔴 The backend is unreachable. Please try again shortly.")
    st.stop()

if not events:
    st.info("No events flagged as needing action yet.")
else:
    for event in events:
        render_event_card(event)
