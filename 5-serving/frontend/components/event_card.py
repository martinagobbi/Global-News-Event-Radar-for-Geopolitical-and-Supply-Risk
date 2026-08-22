from __future__ import annotations

import streamlit as st

from components.tag_buttons import render_tag_buttons
from data.user_store import get_current_user, remove_event_tag, set_event_tag

PREVIEW_ARTICLES = 3


def _update_session_cache(global_event_ids: list[str], new_tag: str | None) -> None:
    for cache_key in ("cached_events", "cached_older_events", "cached_historical_events"):
        if cache_key in st.session_state:
            for event in st.session_state[cache_key]:
                if set(event.get("event_ids", [event.get("global_event_id")])) & set(global_event_ids):
                    event["user_tag"] = new_tag


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
    event_ids = [str(event_id) for event_id in event.get("event_ids", [global_event_id])]
    user_id = get_current_user()

    # Intercept card-hiding actions using exact keys
    if context == "main":
        if any(st.session_state.get(f"main_{k}_{global_event_id}") for k in ["archive", "red_archive", "yellow_archive"]):
            set_event_tag(user_id, event_ids, "archive")
            _update_session_cache(event_ids, "archive")
            with st.container(border=True):
                st.success("Event moved out of this Radar View page and into the \"Archive: Not important\" page.")
            return

    if context == "red":
        if st.session_state.get(f"red_red_untag_{global_event_id}"):
            remove_event_tag(user_id, event_ids)
            _update_session_cache(event_ids, None)
            with st.container(border=True):
                st.success("Event moved out of this page (is still in Radar View).")
            return

        if st.session_state.get(f"red_red_to_yellow_{global_event_id}"):
            set_event_tag(user_id, event_ids, "monitor")
            _update_session_cache(event_ids, "monitor")
            with st.container(border=True):
                st.success("Event moved out of this page (is still in Radar View), and copied to the \"Looking out for developments\" page.")
            return

        if st.session_state.get(f"red_red_archive_{global_event_id}"):
            set_event_tag(user_id, event_ids, "archive")
            _update_session_cache(event_ids, "archive")
            with st.container(border=True):
                st.success("Event moved out of this page and into the \"Archive: Not important\" page.")
            return

    if context == "yellow":
        if st.session_state.get(f"yellow_yellow_to_red_{global_event_id}"):
            set_event_tag(user_id, event_ids, "requires_action")
            _update_session_cache(event_ids, "requires_action")
            with st.container(border=True):
                st.success("Event moved out of this page (is still in Radar View) and copied to the \"Needs action from us\" page.")
            return

        if st.session_state.get(f"yellow_yellow_untag_{global_event_id}"):
            remove_event_tag(user_id, event_ids)
            _update_session_cache(event_ids, None)
            with st.container(border=True):
                st.success("Event moved out of this page (is still in Radar View).")
            return

        if st.session_state.get(f"yellow_yellow_archive_{global_event_id}"):
            set_event_tag(user_id, event_ids, "archive")
            _update_session_cache(event_ids, "archive")
            with st.container(border=True):
                st.success("Event moved out of this page and into the \"Archive: Not important\" page.")
            return

    if context == "archive":
        if st.session_state.get(f"archive_arc_to_red_{global_event_id}"):
            set_event_tag(user_id, event_ids, "requires_action")
            _update_session_cache(event_ids, "requires_action")
            with st.container(border=True):
                st.success("Event back in Radar View, moved out of this page, and copied to the \"Needs action from us\" page.")
            return

        if st.session_state.get(f"archive_arc_to_yellow_{global_event_id}"):
            set_event_tag(user_id, event_ids, "monitor")
            _update_session_cache(event_ids, "monitor")
            with st.container(border=True):
                st.success("Event back in Radar View, moved out of this page, and copied to the \"Looking out for developments\" page.")
            return

        if st.session_state.get(f"archive_unarchive_{global_event_id}"):
            remove_event_tag(user_id, event_ids)
            _update_session_cache(event_ids, None)
            with st.container(border=True):
                st.success("Event back in Radar View and moved out of this page.")
            return

    articles: list[dict] = event.get("articles", [])

    cameo_label = event.get("cameo_label")
    if cameo_label:
        cameo_label = cameo_label.removesuffix(", not specified below")

    # 1. Search for the first article that has a valid title
    card_title = None
    for article in articles:
        if not article.get("mention_identifier", "").startswith("(No article title"):
            card_title = article["mention_identifier"]
            break  # Stop at the first valid title we find

    # 2. Fall back to the formatted event string if no valid titles were found
    if not card_title:
        # Use the cleaned cameo_label we defined just above (or fallback to the raw one if None)
        display_label = cameo_label or event.get("cameo_label", "Unknown")
        card_title = f'"{display_label}" type of event'

    with st.container(border=True):
        st.markdown(f"### {card_title}")
        st.caption(
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

        num_articles = len(articles)
        articles_display = "≥20" if num_articles >= 20 else str(num_articles)

        meta = st.columns(2)
        meta[0].metric("Number of articles", articles_display)
        meta[1].metric("Goldstein score", event["goldstein"])

        date_to_show = str(event["event_date"]).removesuffix(" 00:00:00")

        if cameo_label:
            st.write(f'Event type that makes this of interest: "{cameo_label}"')

        st.write(f"Event date: {date_to_show}")

        if articles:
            st.write("Articles below are sorted by:")
            st.write("- Confidence score (percentage of confidence that the article is related to this event)")
            st.write("- Tone score (−100: max negative; +100: max positive)")
            for i, a in enumerate(articles[:PREVIEW_ARTICLES]):
                if i == 0:
                    st.caption("---------------------------------------------------------")
                st.markdown(f"- [{a['mention_identifier']}]({a['url']})")
                st.caption(
                    f"Confidence: {a['confidence']}%       |"
                    f"       Tone: {a['mention_doc_tone']}"
                )
                st.caption("---------------------------------------------------------")

            if len(articles) > PREVIEW_ARTICLES:
                article_labels = {
                    f"{i + 1}. {a['mention_identifier']}": a
                    for i, a in enumerate(articles)
                }
                with st.expander(f"All {articles_display} articles (they are more than {PREVIEW_ARTICLES})"):
                    selected_label = st.selectbox(
                        "Choose an article to open",
                        options=list(article_labels.keys()),
                        key=f"article_selector_{event['global_event_id']}",
                    )
                    selected = article_labels[selected_label]
                    st.link_button("Open selected article", selected["url"])
                    st.caption(
                        f"Confidence: {a['confidence']}%       |"
                        f"       Tone: {a['mention_doc_tone']}"
                    )
        else:
            st.info("No related articles available for this event.")

        render_tag_buttons(event_ids, context=context, user_tag=event.get("user_tag"))

        events = " | ".join(event_ids)
        st.caption(f"GDELT's Global Event ID(s): `{events}`")