from __future__ import annotations

import streamlit as st

from components.tag_buttons import render_tag_buttons


def _tag_badge(tag: str | None) -> str:
    if tag == "requires_action":
        return ":red[Needs action from us]"
    if tag == "monitor":
        return ":orange[Look out for developments]"
    if tag == "archive":
        return ":green[Archived]"
    return "Untagged"


def render_event_card(event: dict) -> None:
    """
    Render a single event card.

    The backend has already applied:
      - InRawText filter (only InRawText=1 articles if any exist)
      - Ordering: Confidence DESC, abs(MentionDocTone) ASC
      - 20-article cap

    articles[0] is therefore the highest-confidence article and its
    mention_identifier is used as the card title.
    """
    articles: list[dict] = event.get("articles", [])

    # Title = mention_identifier of articles[0] (highest confidence after backend sort)
    card_title = (
        articles[0]["mention_identifier"]
        if articles
        else event.get("card_title", f"Event {event['global_event_id']}")
    )
    top_url = event.get("top_article_url") or (articles[0]["url"] if articles else None)

    with st.container(border=True):
        st.markdown(f"### {card_title}")
        st.caption(
            f"Event `{event['global_event_id']}` | "
            f"{event['country']} | "
            f"Risk score `{event['risk_score']}` | "
            f"{_tag_badge(event.get('user_tag'))}"
        )

        # InRawText disclaimer — flag set by the backend
        if event.get("inrawtext_filtered"):
            st.info(
                "ℹ️ Only articles explicitly identified by GDELT as covering this event "
                "are shown. Articles where GDELT merely inferred a connection have been "
                "excluded to reduce noise and paywall risk."
            )
        elif articles and all(a.get("in_raw_text") == 0 for a in articles):
            st.warning(
                "⚠️ No article for this event was explicitly read by GDELT. "
                "Sources below are inferred associations and may not directly "
                "report on this event."
            )

        meta = st.columns(4)
        meta[0].metric("Articles", len(articles))
        meta[1].metric("Top confidence", articles[0]["confidence"] if articles else "N/A")
        meta[2].metric("Goldstein", event["goldstein"])
        meta[3].metric("CAMEO", event["cameo_code"])

        st.write(f"Risk category: `{event.get('risk_category', 'Not classified')}`")
        st.write(f"Event date: `{event['event_date']}`")

        if top_url:
            st.link_button("Open top source", top_url)

        # Article selector — user clicks to choose which article to open
        if articles:
            article_labels = {
                f"{i + 1}. {a['mention_identifier']}": a
                for i, a in enumerate(articles)
            }
            with st.expander(f"Related articles ({len(articles)})"):
                selected_label = st.selectbox(
                    "Choose an article to open",
                    options=list(article_labels.keys()),
                    key=f"article_selector_{event['global_event_id']}",
                )
                selected = article_labels[selected_label]
                st.link_button("Open selected article", selected["url"])
                st.caption(
                    f"Confidence `{selected['confidence']}` | "
                    f"Tone `{selected['mention_doc_tone']}` | "
                    f"InRawText `{selected['in_raw_text']}`"
                )
        else:
            st.info("No related articles available for this event.")

        render_tag_buttons(event["global_event_id"])