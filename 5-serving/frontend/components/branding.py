import streamlit as st

# Replace Streamlit's cycling "running man" status animation (an easter egg it
# shows on long-running scripts) with a plain loading wheel. The Stop button,
# which appears on hover, is left untouched.
#
# Streamlit renders TWO nested test-ids here: stStatusWidgetRunningIcon is the
# slot, stStatusWidgetRunningManIcon is the animated stick figure inside it. We
# hide the figure and draw our own spinner in the slot.
_RUNNING_INDICATOR_CSS = """
<style>
/* Hide the cycling stick-figure poses */
[data-testid="stStatusWidgetRunningManIcon"] { display: none !important; }

/* Turn the running-icon slot into a host for our own spinner */
[data-testid="stStatusWidgetRunningIcon"] {
    position: relative !important;
    min-width: 1.5rem;
    min-height: 1.5rem;
}
[data-testid="stStatusWidgetRunningIcon"]::after {
    content: "";
    position: absolute;
    inset: 0;
    margin: auto;
    width: 1.1rem;
    height: 1.1rem;
    box-sizing: border-box;
    border: 2px solid rgba(128, 128, 128, 0.25);
    border-top-color: rgba(128, 128, 128, 0.9);
    border-radius: 50%;
    animation: radar-spin 0.7s linear infinite;
    pointer-events: none;
}
/* On hover the Stop button takes over, so drop the wheel out of the way */
[data-testid="stStatusWidget"]:hover
    [data-testid="stStatusWidgetRunningIcon"]::after { display: none; }
@keyframes radar-spin { to { transform: rotate(360deg); } }

/* Colour-code the triage links in the sidebar navigation. Streamlit builds
   the nav href from the page's script name (or root for the default page). */
[data-testid="stSidebarNav"] a[href$="/needs_action"] span,
[data-testid="stSidebarNav"] a[href*="needs_action"] span {
    color: #8B0000 !important;   /* dark red */
    font-weight: 600;
}
[data-testid="stSidebarNav"] a[href$="/monitoring"] span,
[data-testid="stSidebarNav"] a[href*="monitoring"] span {
    color: #9A7D0A !important;   /* dark yellow */
    font-weight: 600;
}
[data-testid="stSidebarNav"] a[href="/"] span,
[data-testid="stSidebarNav"] a[href$="/dashboard"] span,
[data-testid="stSidebarNav"] a[href*="dashboard"] span {
    color: #64B5F6 !important;   /* light blue */
    font-weight: 600;
}
[data-testid="stSidebarNav"] a[href$="/archive"] span,
[data-testid="stSidebarNav"] a[href*="archive"] span {
    color: #424242 !important;   /* dark grey */
    font-weight: 600;
}

/* Hide preferences from sidebar */
[data-testid="stSidebarNav"] a[href$="/preferences"],
[data-testid="stSidebarNav"] a[href*="preferences"] {
    display: none !important;
}
</style>
"""

def use_neutral_spinner() -> None:
    """Swap Streamlit's default 'running man' running indicator for a neutral
    loading wheel, and colour the two triage links in the sidebar. Called from
    app.py so it applies to every page."""
    st.markdown(_RUNNING_INDICATOR_CSS, unsafe_allow_html=True)
