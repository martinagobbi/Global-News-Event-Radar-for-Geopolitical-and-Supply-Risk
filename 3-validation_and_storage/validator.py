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
3. Enrich the surviving mentions with Newspaper3k (article_title,
   article_keywords, enriched) — only the post-filter survivors are scraped,
   bounded by a time budget (ENRICH_TIMEOUT_SECONDS).
4. Append both (cleaned + enriched) tables to the wide-column store.
5. Trigger the events dedup so the most-recent DATEADDED wins.

Returns a small summary dict for logging.
"""

import logging
import os
from pathlib import Path

from enrichment import enrich_dataframe
from gdelt import EVENT_ID, classify, load_table, save_table

logger = logging.getLogger("validation.validator")

# Enrichment configuration (the logic moved here from the parsing layer).
ENRICH_WORKERS = int(os.getenv("MENTION_ENRICH_WORKERS", "8"))
ENRICH_NLP     = os.getenv("MENTION_ENRICH_NLP", "1") == "1"
ENRICH_TIMEOUT = int(os.getenv("ENRICH_TIMEOUT_SECONDS", "600"))


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
    Validate and ingest one slice: an events file, a mentions file, or both.

    A PARTIAL slice is legitimate. The ingestion layer releases whatever it
    retrieved once a slice's deadline passes, and each half still reaches silver
    on its own:

      * events only   — `gdelt_events` is a ReplacingMergeTree keyed on
        GLOBALEVENTID with DATEADDED as the version, so appending a re-published
        event UPDATES the stored copy. The mentions stages are skipped.
      * mentions only — the referential-integrity check already resolves against
        "this events file OR the store", so with no events file every id is
        looked up in ClickHouse and any mention whose event is already stored
        survives. Mentions for unknown events are dropped, exactly as they would
        be in a full slice.

    Parameters
    ----------
    paths   : one or two file paths currently in latest_files
    storage : storage.Storage — used both to look up already-stored event ids
              and to append the validated tables

    Returns
    -------
    dict summary: counts of rows seen / dropped / appended.
    """
    events_path, mentions_path = _split_pair(paths)
    if events_path is None and mentions_path is None:
        raise ValueError("validate_pair requires at least one events or mentions file")

    events_df = load_table(events_path) if events_path is not None else None
    mentions_df = load_table(mentions_path) if mentions_path is not None else None

    n_events = dropped = n_mentions = 0
    mentions_clean = None

    # ── Append events ─────────────────────────────────────────────────────────
    if events_df is not None:
        n_events = storage.append_events(events_df)

    # ── GLOBALEVENTID referential integrity, then enrich and append ───────────
    if mentions_df is not None:
        # With no events file this is empty, so every id goes to the store lookup.
        event_ids_here = (set(_event_id_series(events_df).tolist())
                          if events_df is not None else set())
        mention_ids = _event_id_series(mentions_df)

        # Only the ids NOT already in the current events file need a store lookup.
        to_lookup = set(mention_ids.tolist()) - event_ids_here
        event_ids_stored = storage.existing_event_ids(to_lookup)
        valid_ids = event_ids_here | event_ids_stored

        keep_mask = mention_ids.isin(valid_ids)
        dropped = int((~keep_mask).sum())
        mentions_clean = mentions_df[keep_mask].copy()

        # Only the post-filter survivors are scraped; bounded by ENRICH_TIMEOUT.
        enrich_dataframe(
            mentions_clean,
            url_column="MentionIdentifier",
            max_workers=ENRICH_WORKERS,
            do_nlp=ENRICH_NLP,
            time_budget_s=ENRICH_TIMEOUT,
        )

        if dropped:
            # Rewrite the file in latest_files so the table itself is cleaned.
            save_table(mentions_clean, mentions_path)
            logger.info("Dropped %d unmatched mention rows from %s",
                        dropped, mentions_path.name)

        n_mentions = storage.append_mentions(mentions_clean)

    kinds = "+".join(k for k, v in (("events", events_path),
                                    ("mentions", mentions_path)) if v is not None)
    return {
        "slice_kinds": kinds,
        "events_file": events_path.name if events_path is not None else None,
        "mentions_file": mentions_path.name if mentions_path is not None else None,
        "events_appended": n_events,
        "mentions_seen": len(mentions_df) if mentions_df is not None else 0,
        "mentions_dropped": dropped,
        "mentions_appended": n_mentions,
        "mentions_enriched": (int(mentions_clean["enriched"].sum())
                              if n_mentions and mentions_clean is not None else 0),
    }
