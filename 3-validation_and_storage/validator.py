"""
3-validation_and_storage/validator.py
-------------------------

Core validation performed whenever a fresh pair of files (one events file and
one mentions file) is detected in latest_files.

Steps
-----
1. Load both tables (ZIP or CSV are handled transparently by gdelt.load_table).
2. Referential-integrity check on GLOBALEVENTID:
   every article (mentions row) must reference an event that exists either in
   the current events file OR already in the gdelt_events store. Rows whose
   GLOBALEVENTID matches neither are dropped, and the mentions file in
   latest_files is rewritten without them.
3. Append both (cleaned) tables to the wide-column store.
4. Trigger the events dedup so the most-recent DATEADDED wins.

Returns a small summary dict for logging.
"""

import logging
from pathlib import Path

from gdelt import EVENT_ID, classify, load_table, save_table

logger = logging.getLogger("validation.validator")


def _split_pair(paths):
    """Return (events_path, mentions_path) given the two file paths."""
    events_path = mentions_path = None
    for p in paths:
        kind = classify(p)
        if kind == "events":
            events_path = Path(p)
        elif kind == "mentions":
            mentions_path = Path(p)
    return events_path, mentions_path


def _event_id_series(df):
    """GLOBALEVENTID column coerced to a clean integer Series (bad -> 0)."""
    import pandas as pd
    return pd.to_numeric(df[EVENT_ID], errors="coerce").fillna(0).astype("int64")


def validate_pair(paths, storage) -> dict:
    """
    Validate and ingest one events+mentions pair.

    Parameters
    ----------
    paths   : the two file paths currently in latest_files
    storage : storage.Storage — used both to look up already-stored event ids
              and to append the validated tables

    Returns
    -------
    dict summary: counts of rows seen / dropped / appended.
    """
    events_path, mentions_path = _split_pair(paths)
    if events_path is None or mentions_path is None:
        raise ValueError("validate_pair requires one events file and one mentions file")

    events_df = load_table(events_path)
    mentions_df = load_table(mentions_path)

    # ── GLOBALEVENTID referential integrity ──────────────────────────────────
    event_ids_here = set(_event_id_series(events_df).tolist())
    mention_ids = _event_id_series(mentions_df)

    # Only the ids NOT already in the current events file need a store lookup.
    to_lookup = set(mention_ids.tolist()) - event_ids_here
    event_ids_stored = storage.existing_event_ids(to_lookup)
    valid_ids = event_ids_here | event_ids_stored

    keep_mask = mention_ids.isin(valid_ids)
    dropped = int((~keep_mask).sum())
    mentions_clean = mentions_df[keep_mask].copy()

    if dropped:
        # Rewrite the file in latest_files so the table itself is cleaned.
        save_table(mentions_clean, mentions_path)
        logger.info("Dropped %d unmatched mention rows from %s",
                    dropped, mentions_path.name)

    # ── Append to the wide-column store ───────────────────────────────────────
    n_events = storage.append_events(events_df)
    n_mentions = storage.append_mentions(mentions_clean)

    return {
        "events_file": events_path.name,
        "mentions_file": mentions_path.name,
        "events_appended": n_events,
        "mentions_seen": len(mentions_df),
        "mentions_dropped": dropped,
        "mentions_appended": n_mentions,
    }
