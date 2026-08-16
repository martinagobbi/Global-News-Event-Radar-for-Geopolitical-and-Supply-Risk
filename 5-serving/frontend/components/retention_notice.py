"""
The small-print notice shown on every page that lists event cards.

Two different things can make a card disappear, and a user has no way to tell
them apart from the outside, so both are stated wherever cards are shown:

1. **Age.** The processing layer deletes events whose most recent article is more
   than 365 days old, from silver and gold alike (4-processing/retention.py).
   This applies on every page, without exception.

2. **Preferences.** Gold is rebuilt per user from the territories and keywords
   they registered, and the orphan sweep then removes article rows no user still
   references (4-processing/postgres_writer.delete_orphan_articles). This does
   NOT apply everywhere, which is exactly why the wording has to differ per page:

   * Radar View and Archive FOLLOW preferences. An event that stops matching
     leaves both — archiving something means it is not important, so there is
     nothing worth keeping the row for once it no longer matches either.
   * Needs action and Monitoring are PINNED. Those events are protected from the
     sweep (mongo_reader.PROTECTED_TAGS), because the user has committed to
     following that story and losing it because they edited a keyword would lose
     their work.

Passing `preferences=` is required rather than defaulted, so a new page has to
state which of the two behaviours it actually has instead of silently inheriting
the wrong sentence.
"""

import streamlit as st

_AGE = ("Any events whose most recent articles are older than 365 days "
        "are automatically removed.")

_FOLLOWS = ("Events that no longer match the territories and keywords you "
            "registered are also removed from this page — thus, update "
            "your preferences to change what appears here.")

_PINNED = ("Events you file here stay, regardless of any later change to the "
           "territories and keywords you registered.")

# st.caption is Streamlit's small-print style, and is what the surrounding
# explanatory lines on these pages already use; the extra markup shrinks this one
# further so it reads as a footnote to them rather than as another instruction.
_STYLE = "font-size:0.72rem; opacity:0.65;"


def render_retention_notice(*, preferences: str) -> None:
    """
    Render the notice beneath a page's title and body text.

    `preferences` is "follows" on pages whose contents track the user's
    registered territories and keywords (Radar View, Archive), and "pinned" on
    pages whose contents survive a preference change (Needs action, Monitoring).
    """
    if preferences not in ("follows", "pinned"):
        raise ValueError(f"preferences must be 'follows' or 'pinned', got {preferences!r}")
    text = f"{_AGE} {_FOLLOWS if preferences == 'follows' else _PINNED}"
    st.caption(f"<span style='{_STYLE}'>{text}</span>", unsafe_allow_html=True)
