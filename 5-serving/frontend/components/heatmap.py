import pandas as pd
import pydeck as pdk
import streamlit as st


def render_heatmap(summary: list[dict], selected_countries: list[str]) -> None:
    filtered = [
        row
        for row in summary
        if not selected_countries or row["country"] in selected_countries
    ]

    if not filtered:
        st.info("No geographic risk data is available for the selected filter.")
        return

    frame = pd.DataFrame(filtered)

    frame["event_count"] = pd.to_numeric(
        frame["event_count"],
        errors="coerce",
    ).fillna(0)

    # Light geographic background
    map_style = None

    deck = pdk.Deck(
        map_provider="carto",
        map_style=map_style,
        initial_view_state=pdk.ViewState(
            latitude=20,
            longitude=10,
            zoom=1.1,
            pitch=0,
        ),
        views=[
            pdk.View(
                type="MapView",
                controller={
                    "scrollZoom": False,
                    "doubleClickZoom": False,
                },
            )
        ],
        layers=[
            pdk.Layer(
                "HeatmapLayer",
                data=frame,
                get_position=["longitude", "latitude"],
                get_weight="event_count",
                radius_pixels=60,
            ),
            pdk.Layer(
                "ScatterplotLayer",
                data=frame,
                get_position=["longitude", "latitude"],
                get_fill_color=[190, 30, 45, 160],
                get_radius=70000,
                pickable=False,
            ),
        ]
        
    )

    st.pydeck_chart(
        deck,
        use_container_width=True,
    )