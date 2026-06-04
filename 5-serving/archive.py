import streamlit as st

from data.gold_layer import get_archived_events
from data.user_store import get_current_user, is_first_login


st.title("Archive")

user_id = get_current_user()

if is_first_login(user_id):
    st.warning("The archive becomes available after first-time setup.")
    st.page_link("pages/1_onboarding.py", label="Open setup", icon="🧭")
    st.stop()

archived_events = get_archived_events(user_id)

st.caption("Events removed from the main Radar's Briefing with the Not important / Archive tag.")

if not archived_events:
    st.info("No archived events yet.")
else:
    for event in archived_events:
        with st.expander(f"{event['card_title']} ({event['country']})"):
            st.write(event["event_summary"])
            st.write(f"GlobalEventID: `{event['global_event_id']}`")
            st.write(f"Available articles: `{len(event['articles'])}`")
            if event.get("top_article_url"):
                st.link_button("Open top source", event["top_article_url"])