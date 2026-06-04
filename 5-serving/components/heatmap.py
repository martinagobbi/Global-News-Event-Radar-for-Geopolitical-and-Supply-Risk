import pandas as pd
import pydeck as pdk
import streamlit as st


def render_heatmap(summary: list[dict], selected_countries: list[str]) -> None:
    filtered = [row for row in summary if not selected_countries or row["country"] in selected_countries]
    if not filtered:
        st.info("No geographic risk data is available for the selected filter.")
        return

    frame = pd.DataFrame(filtered)
    st.pydeck_chart(
        pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state=pdk.ViewState(latitude=20, longitude=10, zoom=1.1, pitch=0),
            layers=[
                pdk.Layer(
                    "HeatmapLayer",
                    data=frame,
                    get_position=["longitude", "latitude"],
                    get_weight="risk_score",
                    radius_pixels=60,
                ),
                pdk.Layer(
                    "ScatterplotLayer",
                    data=frame,
                    get_position=["longitude", "latitude"],
                    get_fill_color=[190, 30, 45, 160],
                    get_radius=70000,
                    pickable=True,
                ),
            ],
            tooltip={"text": "{country}\nRisk score: {risk_score}\nEvents: {event_count}"},
        ),
        use_container_width=True,
    )