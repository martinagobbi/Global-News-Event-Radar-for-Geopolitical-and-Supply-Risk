"""
The retention notice shown on every page that lists event cards.

The processing layer deletes events whose most recent article is more than ten
years old, from silver and gold alike (see 4-processing/retention.py). A card
disappearing is therefore normal behaviour rather than a fault, and every page
that can show one says so.

Kept in one module because the wording appears on four pages — Radar View,
Archive, Needs action and Looking out for developments — and four copies of a
sentence drift apart.
"""

import streamlit as st

RETENTION_NOTICE = (
    "Any events whose most recent articles are older than 10 years "
    "are automatically removed."
)

# st.caption is Streamlit's small-print style, and is what the surrounding
# explanatory lines on these pages already use; the extra markup shrinks this one
# further so it reads as a footnote to them rather than as another instruction.
_STYLE = "font-size:0.72rem; opacity:0.65;"


def render_retention_notice() -> None:
    """Render the notice, directly beneath a page's title and body text."""
    st.caption(f"<span style='{_STYLE}'>{RETENTION_NOTICE}</span>",
               unsafe_allow_html=True)
