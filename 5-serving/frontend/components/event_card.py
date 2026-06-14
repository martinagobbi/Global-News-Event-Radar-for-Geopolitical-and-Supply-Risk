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
    with st.container(border=True):
        st.markdown(f"### {event['card_title']}")
        st.caption(
            f"Event `{event['global_event_id']}` | {event['country']} | Risk score `{event['risk_score']}` | {_tag_badge(event.get('user_tag'))}"
        )
        st.write(event["event_summary"])

        top_confidence = event["articles"][0]["confidence"] if event["articles"] else "N/A"
        meta = st.columns(4)
        meta[0].metric("Articles", len(event["articles"]))
        meta[1].metric("Top confidence", top_confidence)
        meta[2].metric("Goldstein", event["goldstein"])
        meta[3].metric("CAMEO", event["cameo_code"])

        st.write(f"Risk category: `{event.get('risk_category', 'Not classified')}`")
        st.write(f"Event date: `{event['event_date']}`")

        if event.get("top_article_url"):
            st.link_button("Open top source", event["top_article_url"])

        if not event["articles"]:
            st.info("No related articles are available for this event.")
            render_tag_buttons(event["global_event_id"])
            return

        article_labels = {
            f"{idx + 1}. {article['mention_identifier']}": article
            for idx, article in enumerate(event["articles"])
        }
        with st.expander("Related articles"):
            selected_label = st.selectbox(
                "Choose an article related to this event",
                options=list(article_labels.keys()),
                key=f"article_selector_{event['global_event_id']}",
            )
            selected_article = article_labels[selected_label]
            st.link_button("Open selected article", selected_article["url"])
            st.caption(
                f"Confidence `{selected_article['confidence']}` | MentionDocTone `{selected_article['mention_doc_tone']}` | InRawText `{selected_article['in_raw_text']}`"
            )

        render_tag_buttons(event["global_event_id"])