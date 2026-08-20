import streamlit as st

from data.user_store import get_current_user, set_event_tag, remove_event_tag


def render_tag_buttons(global_event_id: str, context: str = "main", user_tag: str | None = None) -> None:
    user_id = get_current_user()

    # Define all possible button keys for this specific event card
    k_red = f"{context}_needs_action_{global_event_id}"
    k_remove_red = f"{context}_undo_needs_action_{global_event_id}"
    k_yellow = f"{context}_monitor_{global_event_id}"
    k_remove_yellow = f"{context}_undo_monitor_{global_event_id}"
    k_arc = f"{context}_archive_{global_event_id}"
    k_unarc = f"{context}_unarchive_{global_event_id}"

    # If ANY button on this card was clicked, flag the card as disabled
    is_clicked = any(st.session_state.get(k) for k in [k_red, k_yellow, k_arc, k_unarc, k_remove_red, k_remove_yellow])

    # Determine which visual layout of buttons to show
    layout = context
    if context == "main":
        if user_tag == "requires_action":
            layout = "red"
        elif user_tag == "monitor":
            layout = "yellow"

    # 1. Process actions using session state BEFORE rendering the buttons.
    # (Archive actions never reach here; they are intercepted in event_card.py)
    if context == "main":
        if layout == "main":
            if st.session_state.get(k_red):
                set_event_tag(user_id, global_event_id, "requires_action")
                st.success("Event copied to the \"Needs action from us\" page.")
            elif st.session_state.get(k_yellow):
                set_event_tag(user_id, global_event_id, "monitor")
                st.success("Event copied to the \"Looking out for developments\" page.")
                
        elif layout == "red":
            if st.session_state.get(k_remove_red):
                remove_event_tag(user_id, global_event_id)
                st.success("Event untagged.")
            elif st.session_state.get(k_yellow):
                set_event_tag(user_id, global_event_id, "monitor")
                st.success("Event tag changed to \"Look out for developments\".")
                
        elif layout == "yellow":
            if st.session_state.get(k_red):
                set_event_tag(user_id, global_event_id, "requires_action")
                st.success("Event tag changed to \"Needs action from us\".")
            elif st.session_state.get(k_remove_yellow):
                remove_event_tag(user_id, global_event_id)
                st.success("Event untagged.")

    # 2. Draw the buttons based on the computed layout
    col1, col2, col3 = st.columns(3)

    if layout == "main":
        col1.button("🔴 Apply tag: Needs action from us", key=k_red, disabled=is_clicked, use_container_width=True)
        col2.button("🟡 Apply tag: Look out for developments", key=k_yellow, disabled=is_clicked, use_container_width=True)
        col3.button("Archive: Not important", key=k_arc, disabled=is_clicked, use_container_width=True)

    elif layout == "red":
        col1.button("🔴 Untag", key=k_remove_red, disabled=is_clicked, use_container_width=True)
        col2.button("🟡 Untag, then apply tag:\n\"Look out for developments\"", key=k_yellow, disabled=is_clicked, use_container_width=True)
        col3.button("Untag, then archive", key=k_arc, disabled=is_clicked, use_container_width=True)

    elif layout == "yellow":
        col1.button("🔴 Untag, then apply tag:\n\"Needs action from us\"", key=k_red, disabled=is_clicked, use_container_width=True)
        col2.button("🟡 Untag", key=k_remove_yellow, disabled=is_clicked, use_container_width=True)
        col3.button("Untag, then archive", key=k_arc, disabled=is_clicked, use_container_width=True)

    elif layout == "archive":
        col1.button("🔴 Unarchive, then apply tag:\n\"Needs action from us\"", key=k_red, disabled=is_clicked, use_container_width=True)
        col2.button("🟡 Unarchive, then apply tag:\n\"Look out for developments\"", key=k_yellow, disabled=is_clicked, use_container_width=True)
        col3.button("Unarchive", key=k_unarc, disabled=is_clicked, use_container_width=True)