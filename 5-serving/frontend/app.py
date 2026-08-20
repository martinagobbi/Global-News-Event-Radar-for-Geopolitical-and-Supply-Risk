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
    layout="wide",
    page_icon="favicon.png",
    initial_sidebar_state="expanded",
)

use_neutral_spinner()

authenticated = is_authenticated()

if not authenticated:
    # Sole page: no sidebar navigation is rendered for a single-page nav.
    page = st.navigation([st.Page("views/login.py", title="Sign in")])
else:
    user_id = current_user()
 
    st.sidebar.title("Global News Event Radar")
    st.sidebar.caption(
        "Ingests new GDELT data every 15 minutes, filtering media "
        "noise to isolate events threatening your supply chain stability."
    )
    st.sidebar.write(f"Signed in as `{user_id}`")
    
    # Place Sign Out and Preferences side-by-side using columns
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("Sign out", use_container_width=True):
            logout()
            st.rerun()
    with col2:
        if st.button("Preferences", use_container_width=True):
            st.switch_page("views/preferences.py")
            
    st.sidebar.caption(
        f"Inactive users are logged out after {IDLE_TIMEOUT_SECONDS // 60} minutes."
    )

    # Preferences stays in the sidebar list to remain routable[cite: 1]. 
    # Use the CSS snippet in components/branding.py to visually hide it from the auto-generated menu.
    page = st.navigation([
        st.Page("views/dashboard.py",     title="Radar View", default=True),
        st.Page("views/needs_action.py",  title="Needs action from us"),
        st.Page("views/monitoring.py",    title="Looking out for developments"),
        st.Page("views/older_events.py",  title="Historical Radar View"),
        st.Page("views/archive.py",       title="Archive: Not important"),
        st.Page("views/preferences.py",   title="Preferences"),
    ])
 
page.run()