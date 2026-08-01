from pathlib import Path
import sys

import streamlit as st


ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from auth import IDLE_TIMEOUT_SECONDS, current_user, is_authenticated, logout
from components.branding import use_neutral_spinner


st.set_page_config(
    page_title="Global News Event Radar for Geopolitical and Supply Risk",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

use_neutral_spinner()

authenticated = is_authenticated()

if not authenticated:
    # Sole page: no sidebar navigation is rendered for a single-page nav.
    page = st.navigation([st.Page("pages/login.py", title="Sign in")])
else:
    user_id = current_user()
 
    st.sidebar.title("Global News Event Radar")
    st.sidebar.caption(
        "Continuously ingests GDELT streams, filtering media noise to isolate "
        "high-probability events threatening supply chain stability."
    )
    st.sidebar.write(f"Signed in as `{user_id}`")
    if st.sidebar.button("Sign out"):
        logout()
        st.rerun()
    st.sidebar.caption(
        f"Inactive users are logged out after {IDLE_TIMEOUT_SECONDS // 60} minutes."
    )

    page = st.navigation([
        st.Page("pages/dashboard.py",   title="Radar view", icon="📊", default=True),
        st.Page("pages/onboarding.py",  title="Preferences", icon="🧭"),
        st.Page("pages/archive.py",     title="Archive",     icon="🗂️"),
    ])
 
page.run()
