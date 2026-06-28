"""
4-processing/countries.py
-------------------------
Country/territory name -> (CAMEO, FIPS) code table for the per-user geographic
filter.

The data lives in the single shared source `country_codes.json` (next to this
file, and vendored identically into the frontend build context at
5-serving/frontend/configuration/). This module is a thin loader + helper, so the
picker (frontend) and the matcher (here) derive their names from the SAME artifact
and cannot drift. To check or change the mappings, edit `country_codes.json`.

A "cameo"/"fips" value is a string, None, or a list of strings (the Palestinian
territories carry several); codes_for_names() normalises either form.
"""
import json
from pathlib import Path
from typing import Iterable

_DATA_FILE = Path(__file__).with_name("country_codes.json")
COUNTRY_CODES: dict[str, dict] = json.loads(
    _DATA_FILE.read_text(encoding="utf-8")
)["countries"]
COUNTRY_OPTIONS = sorted(COUNTRY_CODES)


def _as_codes(value) -> list[str]:
    """Normalise a code field (str / None / list) into a list of codes."""
    if not value:
        return []
    return list(value) if isinstance(value, list) else [value]


def codes_for_names(names: Iterable[str]) -> tuple[set[str], set[str]]:
    """Map selected territory NAMES to (CAMEO codes, FIPS codes); skip unknowns/None."""
    cameo: set[str] = set()
    fips: set[str] = set()
    for name in names or []:
        rec = COUNTRY_CODES.get(name)
        if not rec:
            continue
        cameo.update(_as_codes(rec.get("cameo")))
        fips.update(_as_codes(rec.get("fips")))
    return cameo, fips
