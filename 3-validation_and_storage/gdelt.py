"""
3-validation_and_storage/gdelt.py
---------------------

GDELT 2.0 schema definitions and file loading for the validation layer.

Two table layouts are handled, both tab-separated and header-less:

    * Events  ("*.export.CSV")    — 61 columns
    * Mentions ("*.mentions.CSV") — 16 columns

Files may arrive either as plain CSV or as ZIP archives (e.g.
"20260611091500.translation.export.CSV" or
"20260611091500.translation.export.CSV.zip"). load_table() transparently
handles both formats so every other module can work with a DataFrame.

The common key between the two tables is GLOBALEVENTID (column 0 in both).
"""

import io
import logging
import zipfile
from pathlib import Path

import pandas as pd

logger = logging.getLogger("validation.gdelt")

# ── Official GDELT 2.0 column names (order matters: index = file column) ──────
EVENT_COLUMNS = [
    "GLOBALEVENTID", "Day", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources", "NumArticles",
    "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code", "Actor1Geo_Lat",
    "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat",
    "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_ADM2Code", "ActionGeo_Lat",
    "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
]

MENTION_COLUMNS = [
    "GLOBALEVENTID", "EventTimeDate", "MentionTimeDate", "MentionType",
    "MentionSourceName", "MentionIdentifier", "SentenceID",
    "Actor1CharOffset", "Actor2CharOffset", "ActionCharOffset",
    "InRawText", "Confidence", "MentionDocLen", "MentionDocTone",
    "MentionDocTranslationInfo", "Extras",
    # ── Enrichment fields appended by the parsing layer (Newspaper3k) ─────────
    # A raw 16-column GDELT mentions file leaves these three empty (see
    # load_table's fillna); an enriched 19-column file fills them.
    "article_title", "article_keywords", "enriched",
]

# The single column shared by both tables.
EVENT_ID = "GLOBALEVENTID"


# ── File-type classification ─────────────────────────────────────────────────

def classify(path) -> str:
    """
    Return "events", "mentions", or "unknown" based on the file name.

    Works for plain CSV and ZIP names, and for GDELT's translation variants:
        *.export.CSV[.zip]    -> events
        *.mentions.CSV[.zip]  -> mentions
    """
    name = Path(path).name.lower()
    if "mentions" in name:
        return "mentions"
    if "export" in name:
        return "events"
    return "unknown"


# ── Field-width guard ────────────────────────────────────────────────────────
# Mirrors 2-parsing/parser.check_field_width. Duplicated rather than imported
# because parsing and validation are separate images with no shared package; the
# alternative is a shared library for ~30 lines, across a container boundary.
#
# RAW input widths — deliberately NOT len(EVENT_COLUMNS) / len(MENTION_COLUMNS).
# Mentions arrive 16 wide and are read against 19 names so the three enrichment
# columns pad in empty; conflating the two numbers would reject every file.
EXPECTED_FIELD_COUNT = {"events": 61, "mentions": 16}


def check_field_width(raw: bytes, expected: int, kind: str, path=None) -> None:
    """
    Raise unless the first non-blank line of `raw` has exactly `expected`
    tab-separated fields.

    An empty payload passes: a slice whose filter matched nothing is written as
    a 0-byte file, which is legitimate, and whose first line would otherwise
    count as a single field.

    This is a backstop, not the primary detector. Parsing rewrites each slice at
    its declared width, so a feed-wide change is normally caught there and is
    already invisible by the time it reaches here. What this catches is a file
    that reached the hand-off directory WITHOUT passing through parsing — placed
    by hand, replayed from dead_letter, or produced by an older build.
    """
    text = raw.decode("utf-8", errors="replace")
    for line in text.splitlines():
        if not line.strip():
            continue
        actual = len(line.split("\t"))
        name = getattr(path, "name", path) or "<bytes>"
        if actual > expected:
            raise ValueError(
                f"Looks like GDELT added more columns than expected to this "
                f"file! {kind} file {name} has {actual} fields, expected "
                f"{expected}. Refusing to load it: reading it positionally "
                f"would shift every column."
            )
        if actual < expected:
            raise ValueError(
                f"Looks like GDELT removed columns, or this file is truncated! "
                f"{kind} file {name} has {actual} fields, expected {expected}. "
                f"Refusing to load it."
            )
        return
    return                      # empty payload: nothing to check


def is_valid_pair(paths) -> bool:
    """True if the given two paths are exactly one events file and one mentions file."""
    kinds = sorted(classify(p) for p in paths)
    return kinds == ["events", "mentions"]


def is_processable(paths) -> bool:
    """
    True if the given files form a slice this layer can ingest: one events file,
    one mentions file, or one of each.

    Looser than is_valid_pair() because a slice may legitimately arrive partial —
    ingestion releases whatever it retrieved once the slice's retrieval deadline
    passes, and each half reaches silver on its own. What is still rejected is a
    file whose kind cannot be determined, or two files of the SAME kind, which
    would mean the hand-off directory holds something unexpected.
    """
    kinds = sorted(classify(p) for p in paths)
    return kinds in (["events"], ["mentions"], ["events", "mentions"])


# ── Loading (ZIP or CSV) ─────────────────────────────────────────────────────

def _read_bytes(path: Path) -> bytes:
    """
    Return the raw CSV bytes for a file, transparently extracting the first
    member if the file is a ZIP archive.
    """
    if path.suffix.lower() == ".zip" or zipfile.is_zipfile(path):
        with zipfile.ZipFile(path, "r") as zf:
            member = zf.namelist()[0]
            return zf.read(member)
    return path.read_bytes()


def load_table(path) -> pd.DataFrame:
    """
    Load a GDELT events or mentions file (CSV or ZIP) into a DataFrame whose
    columns carry the official GDELT names.

    Every value is read as a string to preserve raw fidelity; downstream code
    casts the few numeric keys (GLOBALEVENTID, DATEADDED) where needed.
    """
    path = Path(path)
    kind = classify(path)
    if kind == "events":
        columns = EVENT_COLUMNS
    elif kind == "mentions":
        columns = MENTION_COLUMNS
    else:
        raise ValueError(f"Cannot classify GDELT file: {path.name}")

    raw = _read_bytes(path)
    # Width check before the read. `on_bad_lines="skip"` does NOT cover this: it
    # drops individual malformed LINES, whereas a feed-wide column change makes
    # every line uniformly wider, which pandas absorbs by shifting names rather
    # than by reporting a bad line.
    #
    # The expected width is the RAW input width, which for mentions is 16 — NOT
    # len(columns), which is 19. The three extra names (article_title,
    # article_keywords, enriched) are enrichment columns deliberately padded in
    # by the fillna("") below, and comparing against 19 would reject every
    # normal mentions file.
    check_field_width(raw, EXPECTED_FIELD_COUNT[kind], kind, path)

    df = pd.read_csv(
        io.BytesIO(raw),
        sep="\t",
        header=None,
        names=columns,
        dtype=str,
        keep_default_na=False,   # keep empty strings, do not turn them into NaN
        low_memory=False,
        on_bad_lines="skip",
    )
    # Pad any missing trailing columns with "" rather than NaN. This is what lets
    # a raw 16-column mentions file be read against the 19-column enriched schema
    # (the 3 enrichment columns come back empty instead of the string "nan").
    df = df.fillna("")
    logger.info("Loaded %s file %s: %d rows", kind, path.name, len(df))
    return df


def save_table(df: pd.DataFrame, path) -> None:
    """
    Write a DataFrame back to its original location in the same format it was
    read (ZIP or CSV), tab-separated and header-less, matching GDELT's layout.

    Used to persist the cleaned mentions table after invalid rows are dropped.
    """
    path = Path(path)
    csv_bytes = df.to_csv(sep="\t", header=False, index=False).encode("utf-8")

    if path.suffix.lower() == ".zip":
        # Re-zip using the inner member name GDELT would use (strip ".zip").
        inner_name = path.with_suffix("").name
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(inner_name, csv_bytes)
    else:
        path.write_bytes(csv_bytes)
    logger.info("Rewrote cleaned table %s: %d rows", path.name, len(df))
