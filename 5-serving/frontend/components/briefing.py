from __future__ import annotations

import streamlit as st

from components.event_card import render_event_card

from datetime import datetime

def _render_event_table(events: list[dict]) -> None:
    rows = [
        {
            "Event": event["card_title"],
            "Country": event["country"],
            #"Risk category": event.get("risk_category", "Not classified"), # If we ever want to bring risk_category back, just uncomment this and the related thing in event_card.py
            "Date": datetime.strptime(str(event["event_date"])[:10], "%Y-%m-%d").date(),
            "Tag you applied": event.get("user_tag") or "You did not apply a tag",
            "Top source": event.get("top_article_url"),
        }
        for event in events
    ]

    st.dataframe(
        rows,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Top source": st.column_config.LinkColumn(
                "Top source",
                display_text="Open",
            ),
        },
    )


def render_briefing(
    events: list[dict],
    selected_countries: list[str],
    older_events: list[dict] | None = None,
) -> None:

    def _filter(evs: list[dict]) -> list[dict]:
        if not selected_countries:
            return evs
        return [e for e in evs if e["country"] in selected_countries]

    briefing_events = _filter(events)
    red_events = [e for e in briefing_events if e.get("user_tag") == "requires_action"]
    yellow_events = [e for e in briefing_events if e.get("user_tag") == "monitor"]
    older = _filter(older_events or [])

    tabs = st.tabs(
        ["Main briefing", "Red window", "Yellow window", "Older news"]
    )

    with tabs[0]:
        if briefing_events:
            _render_event_table(briefing_events)
            st.divider()

            for event in briefing_events:
                render_event_card(event, context="main")

        else:
            st.info("No events match the current filters.")

    with tabs[1]:
        if red_events:
            for event in red_events:
                render_event_card(event, context="red")
        else:
            st.info("The user tagged no events as needing action.")

    with tabs[2]:
        if yellow_events:
            for event in yellow_events:
                render_event_card(event, context="yellow")
        else:
            st.info("The user tagged no events for monitoring.")

    with tabs[3]:
        if older:
            for event in older:
                render_event_card(event, context="older")
        else:
            st.info("No older risks available for the selected lookback window.")