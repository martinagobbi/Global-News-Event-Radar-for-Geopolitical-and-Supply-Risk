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


def _event_id_series(df):
    """GLOBALEVENTID column coerced to a clean integer Series (bad -> 0)."""
    import pandas as pd
    return pd.to_numeric(df[EVENT_ID], errors="coerce").fillna(0).astype("int64")


# ── Confidence range check ───────────────────────────────────────────────────
# GDELT documents Confidence as a percentage, so 0–100 is the whole of its
# domain. ClickHouse cannot enforce that: the column is String, like 76 of the
# 80 columns across both silver tables (only GLOBALEVENTID, DATEADDED and
# `enriched` carry real types). Anything outside the range would be stored
# without complaint.
#
# Measured on the committed 30-day seed — 111,430 mentions — the field is
# entirely well behaved: zero empties, every value a plain integer, range 10–100,
# and only ten distinct values, all multiples of ten. So this check is not
# fixing an observed fault; it is a tripwire for a future change in the feed.
#
# ── The two judgement calls, and why they went the way they did ──────────────
# EMPTY IS ALLOWED. GDELT genuinely leaves fields blank — in the same mention row
# MentionDocTranslationInfo and Extras are both empty — so an absent Confidence
# is "not provided", not "wrong". Rejecting it would throw away good slices the
# first time GDELT omits the field.
#
# ANYTHING ELSE INVALID REJECTS THE WHOLE SLICE, by raising. The pair then goes
# through the normal path: three attempts, then dead-lettered with both files
# preserved for inspection. This is stricter than the referential-integrity rule
# a few lines below, which drops individual mentions whose event is missing —
# deliberately so. A missing event is an ordinary, expected consequence of slice
# boundaries; a Confidence of 3000 means the feed no longer matches what this
# code believes about it, and the sane response is to stop and be looked at
# rather than to quietly discard rows.
CONFIDENCE_MIN = 0
CONFIDENCE_MAX = 100
_MAX_REPORTED = 5          # keep the error message readable


def check_confidence(mentions_df, path=None) -> None:
    """
    Raise unless every non-empty Confidence parses as a number in 0–100.

    Empty strings pass (see above). Values are compared numerically, not by
    pattern, so a decimal such as "85.5" is accepted when in range: the rule
    asked for is a range check on a percentage, not an integer-format check.
    """
    if mentions_df is None or "Confidence" not in mentions_df.columns:
        return

    values = mentions_df["Confidence"].astype(str).str.strip()
    present = values[values != ""]
    if present.empty:
        return

    numeric = pd.to_numeric(present, errors="coerce")
    non_numeric = present[numeric.isna()]
    out_of_range = present[numeric.notna()
                           & ((numeric < CONFIDENCE_MIN) | (numeric > CONFIDENCE_MAX))]

    if non_numeric.empty and out_of_range.empty:
        return

    name = getattr(path, "name", path) or "<mentions>"
    parts = []
    if not out_of_range.empty:
        parts.append(f"{len(out_of_range)} outside {CONFIDENCE_MIN}–{CONFIDENCE_MAX} "
                     f"(e.g. {sorted(set(out_of_range))[:_MAX_REPORTED]})")
    if not non_numeric.empty:
        parts.append(f"{len(non_numeric)} non-numeric "
                     f"(e.g. {sorted(set(non_numeric))[:_MAX_REPORTED]})")
    raise PermanentError(
        f"Confidence is a percentage and must be {CONFIDENCE_MIN}–{CONFIDENCE_MAX}: "
        f"{name} has " + "; ".join(parts) +
        f", out of {len(present)} non-empty values. Refusing to store this slice."
    )


# ── DATEADDED validity check ─────────────────────────────────────────────────
# Same contract as check_confidence, for the same reason and with the same
# consequences: empty passes, anything else invalid rejects the WHOLE slice by
# raising, and the pair then retries three times before being dead-lettered.
#
# This one matters more than Confidence, because DATEADDED is the watermark.
# `max(DATEADDED)` is what the processing layer polls to decide that silver has
# grown, so a corrupt value does not merely store a wrong number — it decides
# whether the gold layer is rebuilt at all.
#
# It also closes a genuinely silent failure. The column is UInt64 in ClickHouse,
# but nothing invalid ever reaches ClickHouse to be rejected: storage._to_uint()
# converts anything unparseable to 0 first. Measured:
#     _to_uint('https://example.com/story') -> 0
#     _to_uint('85.5')                      -> 0
# A slice whose columns had shifted would therefore be STORED, every row with
# DATEADDED = 0. Zero is below every real slice id, so max(DATEADDED) would not
# move, the watermark would not advance, and gold would quietly stop updating
# while silver kept growing — the exact failure signature that is hardest to
# diagnose from the dashboard.
#
# Validity is "14 digits that parse as a real timestamp", which is what a GDELT
# slice id is (YYYYMMDDHHMMSS, e.g. 20260816133000). Length alone is too weak —
# it would accept 99999999999999 — and a bare range test cannot express "month
# 13 does not exist".
DATEADDED_FORMAT = "%Y%m%d%H%M%S"
DATEADDED_WIDTH  = 14      # a GDELT slice id is always exactly this wide


def check_dateadded(events_df, path=None) -> None:
    """
    Raise unless every non-empty DATEADDED is a valid YYYYMMDDHHMMSS timestamp.

    Empty passes, matching check_confidence: an absent value is "not provided",
    and rejecting it would discard good slices the first time GDELT omits it.
    """
    if events_df is None or "DATEADDED" not in events_df.columns:
        return

    values = events_df["DATEADDED"].astype(str).str.strip()
    present = values[values != ""]
    if present.empty:
        return

    # BOTH tests are needed, and strptime alone is not enough. `%Y` matches
    # greedily rather than exactly four digits, so a truncated id parses happily:
    # strptime("202608161330", "%Y%m%d%H%M%S") succeeds, silently reading a
    # 12-digit value as a date. The explicit width test is what rejects a
    # truncated or padded id; strptime is what rejects an impossible one such as
    # month 13. Verified: 12 digits passed strptime and is caught by the length
    # check; 20261316133000 passes the length check and is caught by strptime.
    bad = []
    for value in present.unique():
        if len(value) != DATEADDED_WIDTH or not value.isdigit():
            bad.append(value)
            continue
        try:
            datetime.strptime(value, DATEADDED_FORMAT)
        except (ValueError, TypeError):
            bad.append(value)

    if not bad:
        return

    n_bad = int(present.isin(bad).sum())
    name = getattr(path, "name", path) or "<events>"
    raise PermanentError(
        f"DATEADDED must be a 14-digit GDELT slice timestamp (YYYYMMDDHHMMSS): "
        f"{name} has {n_bad} invalid value(s) across {len(bad)} distinct form(s) "
        f"(e.g. {sorted(bad)[:_MAX_REPORTED]}), out of {len(present)} non-empty. "
        f"Refusing to store this slice — DATEADDED is the watermark, so a wrong "
        f"value here decides whether gold is rebuilt at all."
    )


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

    # Content check BEFORE the first append. Position matters: append_events()
    # runs below, ahead of the whole mentions block, so a check placed with the
    # mentions would let the events half of a rejected slice reach silver and
    # leave a partial write behind. Raising here means nothing was stored.
    check_confidence(mentions_df, mentions_path)
    check_dateadded(events_df, events_path)

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
