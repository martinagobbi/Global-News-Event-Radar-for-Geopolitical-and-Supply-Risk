from __future__ import annotations

import streamlit as st

from components.tag_buttons import render_tag_buttons
from data.user_store import get_current_user, remove_event_tag, set_event_tag

# How many articles are listed on the card without the reader opening anything.
# The backend already caps the full list at 20, which is what the expander shows.
PREVIEW_ARTICLES = 3


def _tag_badge(tag: str | None) -> str:
    if tag == "requires_action":
        return ':red[You tagged with "Needs action from us"]'
    if tag == "monitor":
        return ':orange[You tagged with "Look out for developments"]'
    if tag == "archive":
        return ":green[Archived]"
    return "You did not apply a tag"


def render_event_card(event: dict, context: str = "main") -> None:
    """Render a single event card."""
    global_event_id = event["global_event_id"]
    user_id = get_current_user()

    # Intercept archive/unarchive actions to hide the card body and display only the message
    if context == "main" and st.session_state.get(f"main_archive_{global_event_id}"):
        set_event_tag(user_id, global_event_id, "archive")
        with st.container(border=True):
            st.success("Event moved out of this Radar View page and into the \"Archive: Not important\" page.")
        return

    if context == "red":
        if st.session_state.get(f"{context}_undo_needs_action_{global_event_id}"):
            remove_event_tag(user_id, global_event_id)
            with st.container(border=True):
                st.success("Event moved out of this page (is still in Radar View).")
            return
        
        if st.session_state.get(f"{context}_monitor_{global_event_id}"):
            set_event_tag(user_id, global_event_id, "monitor")
            with st.container(border=True):
                st.success("Event moved out of this page (is still in Radar View), and copied to the \"Looking out for developments\" page.")
            return

        if st.session_state.get(f"main_archive_{global_event_id}"):
            set_event_tag(user_id, global_event_id, "archive")
            with st.container(border=True):
                st.success("Event moved out of this page and into the \"Archive: Not important\" page.")
            return

    if context == "yellow":
        if st.session_state.get(f"{context}_needs_action_{global_event_id}"):
            set_event_tag(user_id, global_event_id, "requires_action")
            with st.container(border=True):
                st.success("Event moved out of this page (is still in Radar View) and copied to the \"Needs action from us\" page.")
            return

        if st.session_state.get(f"{context}_undo_monitor_{global_event_id}"):
            remove_event_tag(user_id, global_event_id)
            with st.container(border=True):
                st.success("Event moved out of this page (is still in Radar View).")
            return

        if st.session_state.get(f"main_archive_{global_event_id}"):
            set_event_tag(user_id, global_event_id, "archive")
            with st.container(border=True):
                st.success("Event moved out of this page and into the \"Archive: Not important\" page.")
            return

    if context == "archive":
        if st.session_state.get(f"{context}_needs_action_{global_event_id}"):
            set_event_tag(user_id, global_event_id, "requires_action")
            with st.container(border=True):
                st.success("Event back in Radar View, moved out of this page, and copied to the \"Needs action from us\" page.")
            return

        if st.session_state.get(f"{context}_monitor_{global_event_id}"):
            set_event_tag(user_id, global_event_id, "monitor")
            with st.container(border=True):
                st.success("Event back in Radar View, moved out of this page, and copied to the \"Looking out for developments\" page.")
            return

        if st.session_state.get(f"{context}_unarchive_{global_event_id}"):
            remove_event_tag(user_id, global_event_id)
            with st.container(border=True):
                st.success("Event back in Radar View and moved out of this page.")
            return

    articles: list[dict] = event.get("articles", [])

    cameo_label = event.get("cameo_label")
    if cameo_label:
        cameo_label = cameo_label.removesuffix(", not specified below")

    card_title = (
        articles[0]["mention_identifier"]
        if articles and not articles[0]["mention_identifier"].startswith("No article title")
        else event.get("card_title", f'"{event["cameo_label"]}" type of event (ID: {event["global_event_id"]})')
    )

    with st.container(border=True):
        st.markdown(f"### {card_title}")
        st.caption(
            f"Event `{event['global_event_id']}` | "
            f"{event['country']} | "
            f"{_tag_badge(event.get('user_tag'))}"
        )

        if event.get("inrawtext_filtered"):
            st.info(
                "ℹ️ Only articles explicitly identified by GDELT as covering this event "
                "are shown. Articles where GDELT merely inferred a connection have been "
                "excluded to reduce noise and paywall risk."
            )
        elif articles and all(a.get("in_raw_text") == 0 for a in articles):
            st.warning(
                "⚠️ Sources on this card are inferred associations and may not directly "
                "report on a relevant event."
            )

        meta = st.columns(2)
        meta[0].metric("Number of articles", len(articles))
        meta[1].metric("Goldstein score", event["goldstein"])

        date_to_show = str(event["event_date"]).removesuffix(" 00:00:00")

        if cameo_label:
            st.write(f'Event type that makes this of interest: "{cameo_label}"')

        st.write(f"Event date: {date_to_show}")

        if articles:
            st.write("**Articles (sorted by Confidence score, then Tone)**")
            for i, a in enumerate(articles[:PREVIEW_ARTICLES]):
                if i == 0:
                    st.caption("---------------------------------------------------------")
                st.markdown(f"- [{a['mention_identifier']}]({a['url']})")
                st.caption(
                    f"**Confidence** that this article is related to this event: **{a['confidence']}%**       |"
                    f"       **Tone** (−100: max negative; +100: max positive): **{a['mention_doc_tone']}**"
                )
                st.caption("---------------------------------------------------------")

            if len(articles) > PREVIEW_ARTICLES:
                article_labels = {
                    f"{i + 1}. {a['mention_identifier']}": a
                    for i, a in enumerate(articles)
                }
                with st.expander(f"All {len(articles)} articles (they are more than {PREVIEW_ARTICLES})"):
                    selected_label = st.selectbox(
                        "Choose an article to open",
                        options=list(article_labels.keys()),
                        key=f"article_selector_{event['global_event_id']}",
                    )
                    selected = article_labels[selected_label]
                    st.link_button("Open selected article", selected["url"])
                    st.caption(
                        f"Confidence that this article is related to this event: {selected['confidence']}%       |"
                        f"       Tone (−100: max negative; +100: max positive): {selected['mention_doc_tone']}"
                    )
        else:
            st.info("No related articles available for this event.")

        render_tag_buttons(event["global_event_id"], context=context, user_tag=event.get("user_tag"))