import streamlit as st

from components.event_card import render_event_card
from data.gold_layer import get_archived_events
from data.user_store import get_current_user, is_first_login


st.title("Archive")

user_id = get_current_user()

if is_first_login(user_id):
    st.warning("The archive becomes available after first-time setup.")
    st.page_link("pages/onboarding.py", label="Open setup", icon="🧭")
    st.stop()

st.caption("Events removed from the main Radar Briefing with the 'Not important / Archive' tag.")

archived = get_archived_events(user_id)

if not archived:
    st.info("No archived events yet.")
else:
    for event in archived:
        render_event_card(event)