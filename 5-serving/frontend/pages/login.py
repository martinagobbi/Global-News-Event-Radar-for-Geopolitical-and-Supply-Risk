import streamlit as st

from auth import IDLE_TIMEOUT_SECONDS, login, verify

st.title("Global News Event Radar")
st.caption("Sign in to open your supply-chain briefing.")

# Explains why the user was bounced back here, when that's what happened.
if st.session_state.pop("auth_notice", None) == "idle_timeout":
    st.info(
        f"You were signed out after {IDLE_TIMEOUT_SECONDS // 60} minutes of "
        "inactivity. Please sign in again."
    )

with st.form("login_form"):
    user_id = st.text_input("Username")
    password = st.text_input("Password", type="password")
    submitted = st.form_submit_button("Sign in", type="primary")

if submitted:
    if verify(user_id.strip(), password):
        login(user_id.strip())
        st.rerun()          # re-runs app.py, which now builds the full nav
    else:
        # Deliberately does not say which field was wrong.
        st.error("Incorrect username or password.")

st.divider()
st.caption(
    f"Inactive users are logged out after {IDLE_TIMEOUT_SECONDS // 60} minutes."
)
st.caption(
    "Test accounts only — short passwords, listed in the project README. "
    "Password changes are not available for test accounts."
)
