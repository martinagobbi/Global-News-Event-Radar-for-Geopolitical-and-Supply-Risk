import streamlit as st

from data.user_store import get_current_user, set_event_tag, remove_event_tag


def _update_session_cache(global_event_ids: list[str], new_tag: str | None) -> None:
    """Keep Streamlit's cached events in sync with tag changes immediately."""
    for cache_key in ("cached_events", "cached_older_events", "cached_historical_events"):
        if cache_key in st.session_state:
            for event in st.session_state[cache_key]:
                if set(event.get("event_ids", [event.get("global_event_id")])) & set(global_event_ids):
                    event["user_tag"] = new_tag


def render_tag_buttons(global_event_ids: list[str], context: str = "main", user_tag: str | None = None) -> None:
    user_id = get_current_user()
    global_event_id = str(global_event_ids[0])

    # Define 12 distinct keys across layouts to prevent Streamlit React DOM duplication
    k_main_red = f"{context}_apply_red_{global_event_id}"
    k_main_yellow = f"{context}_apply_yellow_{global_event_id}"
    k_main_arc = f"{context}_archive_{global_event_id}"

    k_red_untag = f"{context}_red_untag_{global_event_id}"
    k_red_yellow = f"{context}_red_to_yellow_{global_event_id}"
    k_red_arc = f"{context}_red_archive_{global_event_id}"

    k_yellow_red = f"{context}_yellow_to_red_{global_event_id}"
    k_yellow_untag = f"{context}_yellow_untag_{global_event_id}"
    k_yellow_arc = f"{context}_yellow_archive_{global_event_id}"

    k_arc_red = f"{context}_arc_to_red_{global_event_id}"
    k_arc_yellow = f"{context}_arc_to_yellow_{global_event_id}"
    k_arc_unarc = f"{context}_unarchive_{global_event_id}"

    all_keys = [
        k_main_red, k_main_yellow, k_main_arc,
        k_red_untag, k_red_yellow, k_red_arc,
        k_yellow_red, k_yellow_untag, k_yellow_arc,
        k_arc_red, k_arc_yellow, k_arc_unarc,
    ]

    is_clicked = any(st.session_state.get(k) for k in all_keys)

    # Determine visual layout
    layout = context
    if context == "main":
        if user_tag == "requires_action":
            layout = "red"
        elif user_tag == "monitor":
            layout = "yellow"

    # 1. Process actions for main dashboard
    if context == "main":
        if layout == "main":
            if st.session_state.get(k_main_red):
                set_event_tag(user_id, global_event_ids, "requires_action")
                _update_session_cache(global_event_ids, "requires_action")
                layout = "red"
                st.success("Event copied to the \"Needs action from us\" page.")
            elif st.session_state.get(k_main_yellow):
                set_event_tag(user_id, global_event_ids, "monitor")
                _update_session_cache(global_event_ids, "monitor")
                layout = "yellow"
                st.success("Event copied to the \"Looking out for developments\" page.")

        elif layout == "red":
            if st.session_state.get(k_red_untag):
                remove_event_tag(user_id, global_event_ids)
                _update_session_cache(global_event_ids, None)
                layout = "main"
                st.success("Event moved out of \"Needs action from us\" (still remains on this page).")
            elif st.session_state.get(k_red_yellow):
                set_event_tag(user_id, global_event_ids, "monitor")
                _update_session_cache(global_event_ids, "monitor")
                layout = "yellow"
                st.success("Event moved out of \"Needs action from us\" and into \"Looking out for developments\" (still remains on this page).")

        elif layout == "yellow":
            if st.session_state.get(k_yellow_red):
                set_event_tag(user_id, global_event_ids, "requires_action")
                _update_session_cache(global_event_ids, "requires_action")
                layout = "red"
                st.success("Event moved out of \"Looking out for developments\" and into \"Needs action from us\" (still remains on this page).")
            elif st.session_state.get(k_yellow_untag):
                remove_event_tag(user_id, global_event_ids)
                _update_session_cache(global_event_ids, None)
                layout = "main"
                st.success("Event moved out of \"Looking out for developments\" (still remains on this page).")

    # 2. Render buttons based on computed layout
    col1, col2, col3 = st.columns(3)

    if layout == "main":
        col1.button("🔴 Apply tag: Needs action from us", key=k_main_red, disabled=is_clicked, use_container_width=True)
        col2.button("🟡 Apply tag: Look out for developments", key=k_main_yellow, disabled=is_clicked, use_container_width=True)
        col3.button("Archive: Not important", key=k_main_arc, disabled=is_clicked, use_container_width=True)

    elif layout == "red":
        col1.button("🔴 Untag", key=k_red_untag, disabled=is_clicked, use_container_width=True)
        col2.button("🟡 Untag, then apply tag:\n\"Look out for developments\"", key=k_red_yellow, disabled=is_clicked, use_container_width=True)
        col3.button("Untag, then archive", key=k_red_arc, disabled=is_clicked, use_container_width=True)

    elif layout == "yellow":
        col1.button("🔴 Untag, then apply tag:\n\"Needs action from us\"", key=k_yellow_red, disabled=is_clicked, use_container_width=True)
        col2.button("🟡 Untag", key=k_yellow_untag, disabled=is_clicked, use_container_width=True)
        col3.button("Untag, then archive", key=k_yellow_arc, disabled=is_clicked, use_container_width=True)

    elif layout == "archive":
        col1.button("🔴 Unarchive, then apply tag:\n\"Needs action from us\"", key=k_arc_red, disabled=is_clicked, use_container_width=True)
        col2.button("🟡 Unarchive, then apply tag:\n\"Look out for developments\"", key=k_arc_yellow, disabled=is_clicked, use_container_width=True)
        col3.button("Unarchive", key=k_arc_unarc, disabled=is_clicked, use_container_width=True)