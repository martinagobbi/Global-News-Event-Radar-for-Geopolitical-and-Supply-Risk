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

from datetime import datetime

import pandas as pd      # check_confidence() uses to_numeric for the range test

from enrichment import enrich_dataframe
from gdelt import EVENT_ID, classify, load_table, save_table, PermanentError

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


def _event_id_series(df, path=None):
    """
    GLOBALEVENTID as a clean STRING Series. Never coerced, never type-enforced.

    This is the join key for referential integrity, and it is deliberately text
    all the way through the pipeline. It was previously `pd.to_numeric(...)`,
    which is wrong twice over: it silently mapped every non-numeric id to 0 (or,
    in the tripwire version, rejected the whole slice), and it assumes GDELT will
    never introduce a letter. Comparing ids as strings costs nothing here — the
    lookup is set membership, not arithmetic — and cannot be invalidated by a
    change in GDELT's id format.

    Blank ids are excluded from the returned set rather than compared as "": a
    row with no id is removed upstream by _drop_rows_missing(), so nothing should
    reach here, and an empty string must never match another empty string as if
    the two rows were about the same event.
    """
    import pandas as pd
    if df is None or EVENT_ID not in df.columns:
        return pd.Series(dtype="object")
    values = df[EVENT_ID].astype(str).str.strip()
    return values[values != ""]


# ── Row-level cleaning ───────────────────────────────────────────────────────
# Two different responses to bad data, and which one applies is a property of the
# COLUMN, not of how bad the value is:
#
#   REMOVE THE ROW  — the value is the row's identity. Without it the row cannot
#                     be joined, addressed or de-duplicated, so there is nothing
#                     to keep. GLOBALEVENTID (both tables) and MentionIdentifier
#                     (the article URL) are the only two.
#   NULL THE FIELD  — the value is an attribute. The row is still a real event or
#                     article; one measurement is simply unknown, and "unknown"
#                     is a state the schema can now represent.
#
# Neither is a PermanentError. A slice with odd values is not a malformed slice —
# it is a normal slice describing a messy world, and the pipeline's job is to
# carry that messiness forward honestly rather than to refuse the whole batch.
# PermanentError is reserved for files whose SHAPE is wrong: a column-count
# mismatch or an unclassifiable name, where nothing can be trusted at all.
def _drop_rows_missing(df, column: str, kind: str, path=None):
    """Remove rows whose `column` is empty. Returns (df, n_dropped)."""
    if df is None or column not in df.columns:
        return df, 0
    present = df[column].astype(str).str.strip() != ""
    n_dropped = int((~present).sum())
    if n_dropped:
        logger.warning("%s: dropped %d row(s) with no %s — a row without it "
                       "cannot be identified", getattr(path, "name", kind),
                       n_dropped, column)
    return df[present].copy(), n_dropped


def _null_out(df, column: str, is_valid, path=None, why: str = ""):
    """
    Replace every value in `column` that fails `is_valid` with None.

    Uses None rather than "" so the value arrives at ClickHouse as a genuine
    NULL. The distinction matters: "" is a known-empty string, NULL is "not
    provided", and only the latter is excluded from aggregates like avg().
    """
    if df is None or column not in df.columns:
        return df, 0
    original = df[column]
    cleaned = original.map(lambda v: v if is_valid(v) else None)
    n_nulled = int(original.notna().sum() - cleaned.notna().sum())
    if n_nulled:
        logger.info("%s: nulled %d %s value(s)%s",
                    getattr(path, "name", "<file>"), n_nulled, column,
                    f" ({why})" if why else "")
    df = df.copy()
    df[column] = cleaned
    return df, n_nulled


# ── Attribute cleaning: null, never reject ───────────────────────────────────
# Both functions below used to raise PermanentError and dead-letter the whole
# slice. They no longer do, and the reason is worth stating: an out-of-range
# Confidence or an unparseable DATEADDED describes ONE field of ONE row. Setting
# it aside meant discarding ~1,400 good mentions over a single bad value, and it
# also fired three full enrichment passes first, because a deterministic input
# error was being routed through a retry budget meant for transient faults.
#
# PermanentError is now reserved for a file whose SHAPE is wrong — wrong column
# count, or an unclassifiable name — where no row can be trusted.
CONFIDENCE_MIN = 0
CONFIDENCE_MAX = 100


def _valid_confidence(value) -> bool:
    """True only for a number within 0-100. Empty, None and junk are all False."""
    if value is None:
        return False
    text = str(value).strip()
    if text == "" or text.lower() in ("nan", "none", "null"):
        return False
    try:
        return CONFIDENCE_MIN <= float(text) <= CONFIDENCE_MAX
    except (TypeError, ValueError):
        return False


def clean_confidence(mentions_df, path=None):
    """Null out every Confidence that is not a number in 0-100."""
    return _null_out(mentions_df, "Confidence", _valid_confidence, path,
                     why="not a percentage in 0-100")


# ── DATEADDED ────────────────────────────────────────────────────────────────
# A GDELT slice id is YYYYMMDDHHMMSS, but the DATE is the part that carries
# meaning: it places the event in time and drives `age_days` and retention.
# A truncated id that still names a real day is therefore ACCEPTED and padded to
# midnight, while anything without a full year-month-day is nulled.
#
# Accepted:  20260823101500 (full)   20260823 (date only)
#            2026082310     (+hour)  202608231015 (+minute)
# Nulled:    202608 (no day)  2026 (no month/day)  "" / None / junk
#            20261323101500 (month 13 — parses as a number, is not a date)
DATEADDED_FULL = "%Y%m%d%H%M%S"
_DATEADDED_FORMS = [                 # (width, format) — longest first
    (14, "%Y%m%d%H%M%S"),
    (12, "%Y%m%d%H%M"),
    (10, "%Y%m%d%H"),
    (8,  "%Y%m%d"),
]


def _normalise_dateadded(value):
    """
    Return a full 14-digit slice id, or None if the date part is not usable.

    Shorter forms are zero-padded to midnight, which is why the pad is applied
    AFTER strptime confirms the prefix is a real date: padding first would turn
    '202613' into '20261300000000' and hide an impossible month behind a
    well-formed string.
    """
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() in ("nan", "none", "null") or not text.isdigit():
        return None
    for width, fmt in _DATEADDED_FORMS:
        if len(text) == width:
            try:
                datetime.strptime(text, fmt)
            except (TypeError, ValueError):
                return None
            return text.ljust(14, "0")
    return None                       # any other width is not a slice id


def clean_dateadded(events_df, path=None):
    """
    Null out unusable DATEADDED values and pad the usable-but-short ones.

    Note this is the WATERMARK column: `max(DATEADDED)` decides whether gold is
    rebuilt. A null here is safe — it is simply not a maximum — whereas the old
    `_to_uint()` fallback wrote 0, which is a value, sorts below every real slice
    and silently froze the watermark. Nulling is what makes that unreachable.
    """
    if events_df is None or "DATEADDED" not in events_df.columns:
        return events_df, 0
    original = events_df["DATEADDED"]
    cleaned = original.map(_normalise_dateadded)
    n_nulled = int(original.notna().sum() - cleaned.notna().sum())
    if n_nulled:
        logger.warning("%s: nulled %d DATEADDED value(s) with no usable date",
                       getattr(path, "name", "<events>"), n_nulled)
    events_df = events_df.copy()
    events_df["DATEADDED"] = cleaned
    return events_df, n_nulled


def _valid_number(value) -> bool:
    """True for anything parseable as a float. Used for GoldsteinScale."""
    if value is None:
        return False
    text = str(value).strip()
    if text == "" or text.lower() in ("nan", "none", "null"):
        return False
    try:
        float(text)
        return True
    except (TypeError, ValueError):
        return False


def clean_goldstein(events_df, path=None):
    """Null out a missing or unparseable Goldstein score."""
    return _null_out(events_df, "GoldsteinScale", _valid_number, path,
                     why="missing or unparseable")



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
        raise PermanentError("validate_pair requires at least one events or mentions file")

    events_df = load_table(events_path) if events_path is not None else None
    mentions_df = load_table(mentions_path) if mentions_path is not None else None

    # ── Clean BEFORE the first append ───────────────────────────────────────
    # Position still matters, though for a weaker reason than before. These no
    # longer raise, so there is no partial-write hazard to avoid; but they must
    # run ahead of append_events() so what reaches silver is the cleaned frame.
    #
    # Order within the block is deliberate: rows are DROPPED first, then the
    # survivors have attributes nulled. Doing it the other way round would spend
    # work nulling fields on rows about to be discarded, and would make the
    # logged counts misleading (an attribute nulled on a row that then vanished).
    events_df, ev_no_id = _drop_rows_missing(events_df, EVENT_ID, "events", events_path)
    mentions_df, mn_no_id = _drop_rows_missing(mentions_df, EVENT_ID, "mentions", mentions_path)
    mentions_df, mn_no_url = _drop_rows_missing(
        mentions_df, "MentionIdentifier", "mentions", mentions_path)

    events_df, _ = clean_dateadded(events_df, events_path)
    events_df, _ = clean_goldstein(events_df, events_path)
    mentions_df, _ = clean_confidence(mentions_df, mentions_path)

    rows_dropped = ev_no_id + mn_no_id + mn_no_url

    n_events = dropped = n_mentions = 0
    mentions_clean = None

    # ── Append events ─────────────────────────────────────────────────────────
    if events_df is not None:
        n_events = storage.append_events(events_df)

    # ── GLOBALEVENTID referential integrity, then enrich and append ───────────
    if mentions_df is not None:
        # With no events file this is empty, so every id goes to the store lookup.
        event_ids_here = (set(_event_id_series(events_df, events_path).tolist())
                          if events_df is not None else set())
        mention_ids = _event_id_series(mentions_df, mentions_path)

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