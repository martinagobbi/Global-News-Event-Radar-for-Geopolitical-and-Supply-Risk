import streamlit as st

from components.event_card import render_event_card


def _render_event_rows(events: list[dict]) -> None:
    rows = [
        {
            "Event": event["card_title"],
            "Country": event["country"],
            "Risk category": event.get("risk_category", "Not classified"),
            "Risk score": event["risk_score"],
            "Date": event["event_date"],
            "Tag": event.get("user_tag") or "Untagged",
            "Top source": event.get("top_article_url"),
        }
        for event in events
    ]
    st.dataframe(
        rows,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Top source": st.column_config.LinkColumn("Top source", display_text="Open"),
        },
    )


def render_briefing(
    events: list[dict],
    selected_countries: list[str],
    older_events: list[dict] | None = None,
    tagged_events: list[dict] | None = None,
) -> None:
    filtered_events = [
        event for event in events if not selected_countries or event["country"] in selected_countries
    ]
    filtered_tagged_events = [
        event
        for event in (tagged_events or events)
        if not selected_countries or event["country"] in selected_countries
    ]
    filtered_older_events = [
        event
        for event in (older_events or [])
        if not selected_countries or event["country"] in selected_countries
    ]

    tabs = st.tabs(["Main briefing", "Red window", "Yellow window", "Older news"])
    with tabs[0]:
        if filtered_events:
            _render_event_rows(filtered_events)
            st.divider()
            for event in filtered_events:
                render_event_card(event)
        else:
            st.info("No events are available for the current filters.")
    with tabs[1]:
        red_events = [event for event in filtered_tagged_events if event.get("user_tag") == "requires_action"]
        if red_events:
            for event in red_events:
                render_event_card(event)
        else:
            st.info("No events are tagged as needing action.")
    with tabs[2]:
        yellow_events = [event for event in filtered_tagged_events if event.get("user_tag") == "monitor"]
        if yellow_events:
            for event in yellow_events:
                render_event_card(event)
        else:
            st.info("No events are tagged for monitoring.")
    with tabs[3]:
        if filtered_older_events:
            for event in filtered_older_events:
                render_event_card(event)
        else:
            st.info("No older risks are available for the selected lookback window.")