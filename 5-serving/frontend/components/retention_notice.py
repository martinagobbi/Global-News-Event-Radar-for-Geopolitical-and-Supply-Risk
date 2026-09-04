"""
The small-print notice shown on every page that lists event cards.

Two different things can make a card disappear, and a user has no way to tell
them apart from the outside, so both are stated wherever cards are shown:

1. **Age.** The processing layer deletes events whose most recent article is more
   than RETENTION_DAYS (185) old, from silver and gold alike
   (4-processing/retention.py). This applies on every page, without exception.

   The Archive additionally stops LISTING a card at 180 days
   (5-serving/backend/postgres_store.ARCHIVE_MAX_AGE_DAYS), so an archived event
   ages out on the same clock as the Radar and Historical views instead of
   outliving them. That is a display cutoff, deliberately five days short of the
   deletion above: the entry leaves the page first, and the rows behind it are
   reclaimed afterwards. Pass `max_age_days=` to state it.

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

# 185 is 4-processing/retention.py's RETENTION_DAYS. Restated here rather than
# imported: the frontend cannot reach the processing package, and the number is
# an env-tunable default on that side, so treat this as prose that has to be
# re-checked if RETENTION_DAYS moves.
_AGE = ("Any events whose most recent articles are older than 185 days "
        "are automatically removed.")

# Used where the page itself stops listing cards before that deletion — today
# only the Archive, at ARCHIVE_MAX_AGE_DAYS.
_AGE_CAPPED = ("Events older than {days} days no longer appear here, and any "
               "events whose most recent articles are older than 185 days are "
               "removed from the system altogether.")

_FOLLOWS = ("Events that no longer match the territories and keywords you "
            "registered are also removed from this page — thus, update "
            "your preferences to change what appears here.")

_PINNED = ("Events you file here stay, regardless of any later change to the "
           "territories and keywords you registered.")

# st.caption is Streamlit's small-print style, and is what the surrounding
# explanatory lines on these pages already use; the extra markup shrinks this one
# further so it reads as a footnote to them rather than as another instruction.
_STYLE = "font-size:0.72rem; opacity:0.65;"


def render_retention_notice(*, preferences: str, max_age_days: int | None = None) -> None:
    """
    Render the notice beneath a page's title and body text.

    `preferences` is "follows" on pages whose contents track the user's
    registered territories and keywords (Radar View, Archive), and "pinned" on
    pages whose contents survive a preference change (Needs action, Monitoring).

    `max_age_days` is set only by a page that stops listing cards BEFORE the
    system-wide deletion does — the Archive, at ARCHIVE_MAX_AGE_DAYS. Left None,
    the notice describes the deletion alone, which is correct for every page
    whose own window is already stated in its header caption.
    """
    if preferences not in ("follows", "pinned"):
        raise ValueError(f"preferences must be 'follows' or 'pinned', got {preferences!r}")
    age = _AGE if max_age_days is None else _AGE_CAPPED.format(days=max_age_days)
    text = f"{age} {_FOLLOWS if preferences == 'follows' else _PINNED}"
    st.caption(f"<span style='{_STYLE}'>{text}</span>", unsafe_allow_html=True)
