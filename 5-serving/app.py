import streamlit as st
import json
import os
import requests
from pathlib import Path

# Configuration
USER_PREFS_DIR = "/data/user_preferences"
PROCESSED_DIR = "/data/processed"
PROCESSING_API = "http://processing:8001"

os.makedirs(USER_PREFS_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

def get_user_prefs(user_id: str) -> dict:
    """Load user preferences from JSON file."""
    prefs_file = Path(USER_PREFS_DIR) / f"{user_id}_prefs.json"
    if prefs_file.exists():
        with open(prefs_file, 'r') as f:
            return json.load(f)
    return {
        "theme": "light",
        "notifications": True,
        "data_format": "table"
    }

def save_user_prefs(user_id: str, prefs: dict):
    """Save user preferences to JSON file."""
    prefs_file = Path(USER_PREFS_DIR) / f"{user_id}_prefs.json"
    with open(prefs_file, 'w') as f:
        json.dump(prefs, f, indent=2)

def trigger_processing(user_id: str) -> bool:
    """Call processing API to generate user-specific dataset."""
    try:
        response = requests.post(f"{PROCESSING_API}/process/{user_id}", timeout=30)
        if response.status_code == 200:
            return True
        else:
            st.error(f"Processing API error: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to processing layer. Make sure it's running.")
        return False
    except Exception as e:
        st.error(f"Error calling processing API: {str(e)}")
        return False

# Initialize session state
if "user_id" not in st.session_state:
    st.session_state.user_id = "default_user"

st.set_page_config(page_title="BDT Dashboard", layout="wide")

st.title("📊 BDT Data Pipeline Dashboard")

# Sidebar: User identity and preferences
with st.sidebar:
    st.header("User Settings")
    user_id = st.text_input("Enter your user ID:", value=st.session_state.user_id)
    st.session_state.user_id = user_id
    
    user_prefs = get_user_prefs(user_id)
    
    st.subheader("Preferences")
    theme = st.radio("Theme:", ["light", "dark"], index=0 if user_prefs["theme"] == "light" else 1)
    notifications = st.checkbox("Enable notifications", value=user_prefs["notifications"])
    data_format = st.selectbox("Data format:", ["table", "summary", "json"], 
                                index=["table", "summary", "json"].index(user_prefs["data_format"]))
    
    if st.button("Save Preferences"):
        user_prefs = {
            "theme": theme,
            "notifications": notifications,
            "data_format": data_format
        }
        save_user_prefs(user_id, user_prefs)
        
        # Trigger processing for this user
        st.info("Processing data for your preferences...")
        if trigger_processing(user_id):
            st.success(f"Preferences saved and data processed for {user_id}")
        else:
            st.warning(f"Preferences saved, but processing failed. Try again later.")

# Main dashboard content
st.header(f"Welcome, {st.session_state.user_id}!")

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Pipeline Status", "Running", delta="✓")
with col2:
    st.metric("Last Run", "Just now")
with col3:
    st.metric("Data Points", "1,234")

st.divider()

# Check if user-specific processed data exists
processed_data_path = Path(PROCESSED_DIR) / f"processed_{st.session_state.user_id}.csv"
if processed_data_path.exists():
    st.subheader("Your Processed Data")
    import pandas as pd
    df = pd.read_csv(processed_data_path)
    
    if user_prefs["data_format"] == "table":
        st.dataframe(df)
    elif user_prefs["data_format"] == "summary":
        st.bar_chart(df.set_index(df.columns[0]))
    else:
        st.json(df.to_dict(orient="records"))
else:
    st.info("No processed data available. Set your preferences and save to generate your data.")

st.divider()

# Debug info
with st.expander("Debug Info"):
    st.write(f"**User ID:** {st.session_state.user_id}")
    st.write(f"**User Preferences:** {user_prefs}")
    st.write(f"**User Data Path:** {processed_data_path}")
    st.write(f"**Processing API:** {PROCESSING_API}")
