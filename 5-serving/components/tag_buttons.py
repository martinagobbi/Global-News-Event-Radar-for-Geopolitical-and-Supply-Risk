import streamlit as st

from data.user_store import get_current_user, set_event_tag


def render_tag_buttons(global_event_id: str) -> None:
    col1, col2, col3 = st.columns(3)
    user_id = get_current_user()

    if col1.button("Needs action from us", key=f"needs_action_{global_event_id}"):
        set_event_tag(user_id, global_event_id, "requires_action")
        st.success("Event tagged as needing action.")

    if col2.button("Look out for developments", key=f"monitor_{global_event_id}"):
        set_event_tag(user_id, global_event_id, "monitor")
        st.success("Event tagged for monitoring.")

    if col3.button("Not important / Archive", key=f"archive_{global_event_id}"):
        set_event_tag(user_id, global_event_id, "archive")
        st.success("Event moved out of the Radar's Briefing.")