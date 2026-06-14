from pathlib import Path
import sys

import streamlit as st


ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from data.user_store import ensure_storage, get_current_user, is_first_login


st.set_page_config(
    page_title="Global News Event Radar for Geopolitical and Supply Risk",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)


def bootstrap() -> None:
    ensure_storage()
    user_id = st.query_params.get("user", "demo_logistics")
    st.session_state["current_user_id"] = user_id


def sidebar_status() -> None:
    user_id = get_current_user()
    first_login = is_first_login(user_id)

    st.sidebar.title("Global News Event Radar for Geopolitical and Supply Risk")
    st.sidebar.caption("This radar continuously ingests massive GDELT and document streams, filtering out media background noise to isolate high-probability events. Use this dashboard to track, map, and assess real-time developments threatening the operational stability of specific firms, sectors, and geographic regions.")
    st.sidebar.write(f"Current user: `{user_id}`")
    st.sidebar.write(
        "Status: first-time setup required" if first_login else "Status: registered user"
    )
    st.sidebar.info(
        "Registered users can open the dashboard immediately."
    )


bootstrap()
sidebar_status()

st.title("Global News Event Radar for Geopolitical and Supply Risk")
st.write(
    "This app separates first-time setup from daily monitoring. New users register their monitoring perimeter first; "
    "registered users can go straight to the briefing."
)

col1, col2 = st.columns(2)
with col1:
    st.page_link("pages/onboarding.py", label="Open setup", icon="🧭")
with col2:
    st.page_link("pages/dashboard.py", label="Open dashboard", icon="📊")

st.divider()
st.subheader("Application flow")
st.markdown(
    """
    - `Phase 1`: first-time registration, monitoring countries, and risk categories.
    - `Phase 2`: background computation of the user-specific gold layer.
    - `Phase 3`: dashboard reads from the precomputed gold layer and shows heatmap, briefing, and tags.
    """
)