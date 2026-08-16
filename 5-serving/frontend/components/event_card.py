from __future__ import annotations

import streamlit as st

from components.tag_buttons import render_tag_buttons


# How many articles are listed on the card without the reader opening anything.
# The backend already caps the full list at 20, which is what the expander shows.
PREVIEW_ARTICLES = 3


def _tag_badge(tag: str | None) -> str:
    if tag == "requires_action":
        return ":red[Needs action from us]"
    if tag == "monitor":
        return ":orange[Look out for developments]"
    if tag == "archive":
        return ":green[Archived]"
    return "You did not apply a tag"


def render_event_card(event: dict, context: str = "main") -> None:
    """
    Render a single event card.

    The backend has already applied:
      - InRawText filter (only InRawText=1 articles if any exist)
      - Ordering: Confidence DESC, abs(MentionDocTone) ASC
      - 20-article cap

    articles[0] is therefore the highest-confidence article and its
    mention_identifier is used as the card title.

    The top PREVIEW_ARTICLES are listed on the card itself and the remainder sit
    behind an expander, so a reader can judge an event's coverage at a glance
    instead of having to open every card to find out what is in it.
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
                "⚠️ Sources on this card are inferred associations and may not directly "
                "report on a relevant event."
            )

        meta = st.columns(2)
        meta[0].metric("Number of articles", len(articles))
        #meta[1].metric("Top confidence", articles[0]["confidence"] if articles else "N/A")
        meta[1].metric("Goldstein score", event["goldstein"])
        #meta[3].metric("General event", event["cameo_label"])

        #st.write(f"Risk category: `{event.get('risk_category', 'Not classified')}`") # If we ever want to bring risk_category back, just uncomment this and the related thing in briefing.py

        date_to_show = str(event['event_date']).removesuffix(" 00:00:00")
        st.write(f"Event type that makes this of interest: " + '"' + event["cameo_label"].removesuffix(", not specified below") + '"')
        st.write(f"Event date: " + date_to_show)

        #if top_url:
            #st.link_button("Open top source", top_url)

        # The first few articles are listed on the card itself, so the reader sees
        # actual coverage without opening anything. The rest — the backend caps the
        # list at 20 — stay behind the expander.
        if articles:
            st.write("**Articles (sorted by Confidence score, then Tone)**")
            i = 0
            for a in articles[:PREVIEW_ARTICLES]:
                first_article_in_preview = True if i == 0 else False
                if first_article_in_preview:
                    st.caption("---------------------------------------------------------")
                st.markdown(f"- [{a['mention_identifier']}]({a['url']})")
                st.caption(
                    f"**Confidence** that this article is related to this event: **{a['confidence']}%**       |"
                    f"       **Tone** (−100: max negative; +100: max positive): **{a['mention_doc_tone']}**"
                )
                st.caption("---------------------------------------------------------")
                i += 1
            i = 0

            # Only worth an expander when it would actually reveal something.
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

        render_tag_buttons(event["global_event_id"], context=context)
