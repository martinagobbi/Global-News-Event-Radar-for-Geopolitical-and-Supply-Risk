from __future__ import annotations

import streamlit as st

from components.tag_buttons import render_tag_buttons
from data.user_store import get_current_user, remove_event_tag, set_event_tag

# ── Nullable measures on a card ──────────────────────────────────────────────
# Confidence score, Goldstein score and tone are Nullable end to end now:
# validation nulls a value it cannot trust rather than discarding the row
# or inventing a 0. A card must therefore be able to say "not available" —
# an f-string would print the bare word "None", which reads like
# a real measurement.
_NO_TONE = "[No tone score available]"
_NO_CONFIDENCE = "[No confidence score available]"
_NO_GOLDSTEIN = "[No Goldstein score available]"


def _tone_text(value) -> str:
    """The tone as shown on a card, or an explicit not-available marker."""
    if value is None:
        return _NO_TONE
    text = str(value).strip()
    if text == "" or text.lower() in ("none", "nan", "null"):
        return _NO_TONE
    return text


def _confidence_text(value) -> str:
    """Confidence as `NN%`, or an explicit not-available marker."""
    if value is None:
        return _NO_CONFIDENCE
    text = str(value).strip()
    if text == "" or text.lower() in ("none", "nan", "null"):
        return _NO_CONFIDENCE
    return f"{text}%"

def _goldstein_text(value) -> str:
    """Goldstein score, or an explicit not-available marker."""
    if value is None:
        return _NO_GOLDSTEIN
    text = str(value).strip()
    if text == "" or text.lower() in ("none", "nan", "null"):
        return _NO_GOLDSTEIN
    return text

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


def _link_label(article: dict) -> str:
    """
    The clickable text for one article link: the headline, then the publisher.

    `mention_source_name` is GDELT's MentionSourceName carried through silver and
    gold (e.g. "bbc.co.uk"), so a reader can see where a story comes from before
    clicking — the headline alone often does not say. Absent or empty (an older
    gold row written before the column existed, or a mention GDELT gave no source
    for) simply yields the headline unchanged, rather than an empty "()".
    """
    label = str(article.get("mention_identifier", "") or "")
    source = str(article.get("mention_source_name", "") or "").strip()
    return f"{label} ({source})" if source else label


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
        meta[1].metric("Goldstein score", _goldstein_text(event.get("goldstein")))

        date_to_show = str(event["event_date"]).removesuffix(" 00:00:00")

        if cameo_label:
            st.write(f'GDELT-detected event type that might make this of interest: "{cameo_label}"')

        st.write(f"Event date: {date_to_show}")

        if articles:
            st.write("Articles below are sorted by:")
            st.write("- Confidence score (percentage of confidence that the article is related to this event)")
            st.write("- Tone score (−100: max negative; +100: max positive)")
            for i, a in enumerate(articles[:PREVIEW_ARTICLES]):
                if i == 0:
                    st.caption("---------------------------------------------------------")
                st.markdown(f"- [{_link_label(a)}]({a['url']})")
                st.caption(
                    f"Confidence: {_confidence_text(a.get('confidence'))}       |"
                    f"       Tone: {_tone_text(a.get('mention_doc_tone'))}"
                )
                st.caption("---------------------------------------------------------")

            if len(articles) > PREVIEW_ARTICLES:
                article_labels = {
                    f"{i + 1}. {_link_label(a)}": a
                    for i, a in enumerate(articles)
                }
                with st.expander(f"All {articles_display} articles (showing this because they are more than {PREVIEW_ARTICLES})"):
                    selected_label = st.selectbox(
                        "Choose an article to open",
                        options=list(article_labels.keys()),
                        key=f"article_selector_{event['global_event_id']}",
                    )
                    selected = article_labels[selected_label]
                    st.link_button("Open this article", selected["url"])
                    st.caption(
                        f"Confidence: {_confidence_text(selected.get('confidence'))}       |"
                        f"       Tone: {_tone_text(selected.get('mention_doc_tone'))}"
                    )
        else:
            st.info("No related articles available for this event.")

        render_tag_buttons(event_ids, context=context, user_tag=event.get("user_tag"))

        events = " | ".join(event_ids)
        st.caption(f"GDELT's Global Event ID(s): `{events}`")