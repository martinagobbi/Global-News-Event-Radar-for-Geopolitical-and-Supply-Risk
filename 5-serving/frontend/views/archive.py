import os

import streamlit as st

from auth import require_auth

from components.branding import use_neutral_spinner
from components.event_card import render_event_card
from components.retention_notice import render_retention_notice
from data.gold_layer import get_archived_events
from data.user_store import is_first_login


use_neutral_spinner()
st.title("Archive")

user_id = require_auth()

if is_first_login(user_id):
    st.warning("The archive becomes available after first-time setup.")
    st.stop()

# Mirrors 5-serving/backend/postgres_store.ARCHIVE_MAX_AGE_DAYS, which is what
# actually enforces the cutoff — this copy only decides what the page SAYS. Both
# read the same variable with the same default, so overriding it means setting it
# on the frontend and backend containers alike; if only one is set the page keeps
# working and merely quotes the wrong number.
ARCHIVE_MAX_AGE_DAYS = int(os.getenv("ARCHIVE_MAX_AGE_DAYS", "180"))

st.caption(
    "Events removed from the Radar View with the 'Archive: Not important' tag, "
    f"from up to {ARCHIVE_MAX_AGE_DAYS} days ago."
)
render_retention_notice(preferences="follows", max_age_days=ARCHIVE_MAX_AGE_DAYS)

archived = get_archived_events(user_id)

if not archived:
    st.info(
        "Either no events match the current filters, or you need to wait a "
        "few minutes for them to arrive."
    )
else:
    for event in archived:
        render_event_card(event, context="archive")
