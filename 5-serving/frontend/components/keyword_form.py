from __future__ import annotations

import streamlit as st

from configuration.keywords import (
    KEYWORD_QUESTIONS,
    KEYWORDS_SHOWN_BEFORE_FOLD,
    MAX_KEYWORDS_PER_QUESTION,
)


def _state_key(prefix: str, key: str) -> str:
    return f"{prefix}__kw__{key}"


def _init_state(profile: dict, prefix: str) -> None:
    """Seed the per-question session lists from the stored profile (once)."""
    stored = profile.get("keywords") or {}
    for key, _ in KEYWORD_QUESTIONS:
        sk = _state_key(prefix, key)
        if sk not in st.session_state:
            st.session_state[sk] = [
                str(v).strip() for v in (stored.get(key) or []) if str(v).strip()
            ]


def _render_row(items: list[str], i: int, value: str, prefix: str, key: str) -> None:
    """One entry with its remove button."""
    r1, r2 = st.columns([6, 1])
    r1.write(f"• {value}")
    if r2.button("Remove", key=f"{prefix}_rm_{key}_{i}"):
        items.pop(i)
        st.rerun()


def _render_items(items: list[str], prefix: str, key: str) -> None:
    """
    List the entries, folding everything past KEYWORDS_SHOWN_BEFORE_FOLD into an
    expander. With the cap at 1000, rendering a row and a button for every entry
    on every rerun is what makes the page slow, so the tail is only built when the
    user opens it.
    """
    for i, value in enumerate(items[:KEYWORDS_SHOWN_BEFORE_FOLD]):
        _render_row(items, i, value, prefix, key)

    rest = items[KEYWORDS_SHOWN_BEFORE_FOLD:]
    if rest:
        with st.expander(f"Show the remaining {len(rest)}"):
            for offset, value in enumerate(rest):
                _render_row(items, KEYWORDS_SHOWN_BEFORE_FOLD + offset, value, prefix, key)


def render_keyword_questions(profile: dict, prefix: str = "onboard") -> dict:
    """
    Render the five supply-chain keyword questions. Each lets the user add one
    word at a time (capped at MAX_KEYWORDS_PER_QUESTION), with removable entries.
    Leading/trailing spaces are stripped and empty entries dropped on add.

    Entries must be a SINGLE word. The processing layer splits every keyword into
    tokens and requires all of them to appear in an article, so a phrase such as
    "silicon wafers" is a much narrower filter than users expect it to be —
    narrow enough that it matched nothing at all across a 30-day corpus. Adding
    "silicon" and "wafers" separately is both clearer and what the matcher acts on.

    Returns {question_key: [keywords]} reflecting the current state.
    """
    _init_state(profile, prefix)
    result: dict[str, list[str]] = {}

    for key, label in KEYWORD_QUESTIONS:
        sk = _state_key(prefix, key)
        items: list[str] = st.session_state[sk]

        st.markdown(f"**{label}**")

        # Add one field at a time; the form clears so the user can type the next.
        with st.form(f"{prefix}_add_{key}", clear_on_submit=True):
            c1, c2 = st.columns([5, 1])
            new_value = c1.text_input(
                label,
                label_visibility="collapsed",
                placeholder="Add one word, then press Add",
            )
            added = c2.form_submit_button("Add", use_container_width=True)
        if added:
            value = (new_value or "").strip()
            if not value:
                pass  # drop empty
            elif len(value.split()) > 1:
                st.warning(
                    f"Enter one word per item. Add each word of '{value}' separately —"
                    " an article has to contain them all either way, and separate"
                    " entries also match when the words are not next to each other."
                )
            elif len(items) >= MAX_KEYWORDS_PER_QUESTION:
                st.warning(f"Limit of {MAX_KEYWORDS_PER_QUESTION} items reached for this question.")
            elif value in items:
                st.info(f"'{value}' is already in the list.")
            else:
                items.append(value)

        if items:
            _render_items(items, prefix, key)
        else:
            st.caption("No items added yet.")
        st.caption(f"{len(items)}/{MAX_KEYWORDS_PER_QUESTION}")

        result[key] = list(items)

    return result
