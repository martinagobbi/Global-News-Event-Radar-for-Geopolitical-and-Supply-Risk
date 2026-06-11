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


def is_valid_pair(paths) -> bool:
    """True if the given two paths are exactly one events file and one mentions file."""
    kinds = sorted(classify(p) for p in paths)
    return kinds == ["events", "mentions"]


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
