"""
The "your briefing is being rebuilt" notice.

When a user saves new territories or keywords, the processing layer notices the
change through the MongoDB change stream and rebuilds that user's article pool.
The rebuild starts within about a second and normally finishes in a few more, but
it is not instant, and until it lands the dashboard still shows articles selected
by the PREVIOUS preferences. Without a notice that looks like the save was
ignored.

How "still running" is decided
------------------------------
The backend exposes a cheap fingerprint of a user's gold set
(`/users/{id}/events-version`). On save, the fingerprint current at that moment is
remembered. While the live fingerprint still equals it, the rebuild has not landed
yet and the notice is shown. As soon as it differs, the pool has been rebuilt: the
notice is cleared and the dashboard's existing green "your articles were updated"
message takes over.

This is why the notice stays for the whole rebuild rather than disappearing on a
timer — it is tied to the work actually completing, not to elapsed time.
"""

from __future__ import annotations

import time

import streamlit as st

# Upper bound on how long the notice may stay up, whatever the fingerprint does.
# The rebuild takes seconds, so this is not the normal exit — it exists because
# the fingerprint changes if and only if the SET OF ARTICLES changes. Editing
# preferences in a way that selects exactly the same articles (adding a keyword
# that matches nothing, say) produces an identical fingerprint, and without this
# bound the notice would sit there for the rest of the session claiming a rebuild
# was still running that had in fact finished immediately.
MAX_WAIT_SECONDS = 90

# The one place the wording lives, so both pages cannot drift apart.
NOTICE_ICON = "🔄"
NOTICE_TITLE = "Updating your briefing."
NOTICE_BODY = (
    "Your new preferences are being applied to the set of articles that might "
    "interest you. This usually takes a few seconds — you will be told when the "
    "new set is ready."
)

_PENDING_KEY = "recompute_pending_version"


def mark_recompute_pending(version: str | None) -> None:
    """
    Record that a rebuild was just triggered, against the gold fingerprint that
    was current at the time. Call this immediately after saving a profile.
    """
    st.session_state[_PENDING_KEY] = (version, time.monotonic())


def recompute_pending(live_version: str | None) -> bool:
    """
    True while the rebuild triggered by the last save has not yet landed.

    Two ways out, because there are two ways a rebuild can end:
      * the fingerprint moves — the article set changed, and the dashboard's green
        message takes over from here;
      * MAX_WAIT_SECONDS passes — the rebuild finished without changing which
        articles were selected, so the fingerprint never moves (see above).

    A fingerprint that cannot be read (the gold store unreachable) does not clear the
    notice early: the page already shows a database error in that case, and
    dropping this one would imply the rebuild had finished when it is unknown.
    """
    entry = st.session_state.get(_PENDING_KEY)
    if entry is None:
        return False

    pending_version, started = entry
    if live_version is not None and pending_version != live_version:
        del st.session_state[_PENDING_KEY]
        return False
    if time.monotonic() - started > MAX_WAIT_SECONDS:
        del st.session_state[_PENDING_KEY]
        return False
    return True


def render_recompute_notice() -> None:
    """Show the notice, in the same shape as the other status banners."""
    st.info(f"{NOTICE_ICON} **{NOTICE_TITLE}** {NOTICE_BODY}")


# ── First build, and the genuinely-empty case ────────────────────────────────
# These two look identical on screen — an empty briefing — but mean opposite
# things, so they are distinguished rather than left to the reader to guess.

FIRST_BUILD_TITLE = "First-time setup in progress."
FIRST_BUILD_BODY = (
    "Your account was created moments ago and its article pool is still being "
    "built. This happens once, takes about a minute, and the page refreshes itself."
)

NO_MATCHES_BODY = (
    "No articles matched your territories and keywords in this period. Both "
    "filters are narrow by design; widen the briefing window or add keywords."
)


def gold_never_built(system_status: dict) -> bool:
    """
    True when the processing layer has not yet completed its first gold build.

    `timestamp_of_last_update` stays NULL until a recompute finishes and writes
    `pipeline_status`, so a null there means "no gold has ever been produced" —
    not "the gold is empty for you". That distinction is the whole point: on a
    fresh installation the dashboard is reachable seconds before the first build
    lands, and an unexplained empty briefing looks like a broken deployment.
    """
    if not system_status:
        return False
    # A gold-store outage is reported separately and must not be mistaken for this.
    if system_status.get("code"):
        return False
    return system_status.get("timestamp_of_last_update") in (None, "", "None")


def render_first_build_notice() -> None:
    st.info(f"⏳ **{FIRST_BUILD_TITLE}** {FIRST_BUILD_BODY}")


def render_no_matches_notice() -> None:
    st.info(f"ℹ️ {NO_MATCHES_BODY}")
