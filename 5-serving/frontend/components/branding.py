import streamlit as st

# Replaces Streamlit's default top-right "running man" status animation with a
# neutral spinning circle: hide whatever icon Streamlit draws in the status
# widget, then draw a CSS spinner in its place. The status widget is only visible
# while the script is running, so the spinner shows only during a rerun.
_RUNNING_INDICATOR_CSS = """
<style>
[data-testid="stStatusWidget"] img,
[data-testid="stStatusWidget"] svg,
[data-testid="stStatusWidget"] canvas {
    display: none !important;
}
[data-testid="stStatusWidget"] > div::before {
    content: "";
    box-sizing: border-box;
    display: inline-block;
    width: 1.1rem;
    height: 1.1rem;
    margin-right: 0.4rem;
    border: 2px solid rgba(130, 130, 130, 0.35);
    border-top-color: rgba(130, 130, 130, 0.95);
    border-radius: 50%;
    animation: radar-spin 0.7s linear infinite;
    vertical-align: middle;
}
@keyframes radar-spin { to { transform: rotate(360deg); } }
</style>
"""


def use_neutral_spinner() -> None:
    """Swap Streamlit's default 'running man' running indicator for a neutral
    spinning circle. Call once per page (in app.py, after set_page_config)."""
    st.markdown(_RUNNING_INDICATOR_CSS, unsafe_allow_html=True)
