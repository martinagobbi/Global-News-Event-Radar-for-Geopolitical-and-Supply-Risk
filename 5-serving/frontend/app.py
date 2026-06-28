from pathlib import Path
import sys

import streamlit as st


ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from data.user_store import get_current_user, is_first_login


st.set_page_config(
    page_title="Global News Event Radar for Geopolitical and Supply Risk",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)


def bootstrap() -> None:
    user_id = st.query_params.get("user", "demo_logistics")
    st.session_state["current_user_id"] = user_id


def sidebar_status() -> None:
    user_id = get_current_user()
    first_login = is_first_login(user_id)
    st.sidebar.title("Global News Event Radar")
    st.sidebar.caption(
        "Continuously ingests GDELT streams, filtering media noise to isolate "
        "high-probability events threatening supply chain stability."
    )
    st.sidebar.write(f"Current user: `{user_id}`")
    st.sidebar.write("Status: " + ("first-time setup required" if first_login else "registered user"))


bootstrap()
sidebar_status()

st.title("Global News Event Radar for Geopolitical and Supply Risk")
st.write(
    "New users complete the monitoring perimeter setup first. "
    "Registered users can go straight to the briefing dashboard."
)

col1, col2 = st.columns(2)
with col1:
    st.page_link("pages/onboarding.py", label="Open setup", icon="🧭")
with col2:
    st.page_link("pages/dashboard.py", label="Open dashboard", icon="📊")

st.divider()
st.subheader("Application flow")
st.markdown("""
- **Phase 1** — Register monitoring perimeter (territories, supply-chain keywords).
- **Phase 2** — Processing pipeline queries GDELT, stores user-article associations in Oracle.
- **Phase 3** — Dashboard reads from Oracle via backend API and renders event cards.
""")