from __future__ import annotations

import streamlit as st

from configuration.keywords import KEYWORD_QUESTIONS, MAX_KEYWORDS_PER_QUESTION


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


def render_keyword_questions(profile: dict, prefix: str = "onboard") -> dict:
    """
    Render the five supply-chain keyword questions. Each lets the user add one
    field at a time (capped at MAX_KEYWORDS_PER_QUESTION), with removable entries.
    Leading/trailing spaces are stripped and empty entries dropped on add.

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
                placeholder="Add one item, then press Add",
            )
            added = c2.form_submit_button("Add", use_container_width=True)
        if added:
            value = (new_value or "").strip()
            if not value:
                pass  # drop empty
            elif len(items) >= MAX_KEYWORDS_PER_QUESTION:
                st.warning(f"Limit of {MAX_KEYWORDS_PER_QUESTION} items reached for this question.")
            elif value in items:
                st.info(f"'{value}' is already in the list.")
            else:
                items.append(value)

        if items:
            for i, value in enumerate(items):
                r1, r2 = st.columns([6, 1])
                r1.write(f"• {value}")
                if r2.button("Remove", key=f"{prefix}_rm_{key}_{i}"):
                    items.pop(i)
                    st.rerun()
        else:
            st.caption("No items added yet.")
        st.caption(f"{len(items)}/{MAX_KEYWORDS_PER_QUESTION}")

        result[key] = list(items)

    return result
