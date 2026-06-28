"""
configuration/countries.py
--------------------------
Territory picker options for the onboarding / dashboard multiselect.

Loaded from the single shared data file `country_codes.json` — the same artifact
the processing layer uses for name -> CAMEO/FIPS codes, vendored into this build
context. Deriving the options from that file means the picker can never offer a
name the processing matcher (codes_for_names) doesn't recognise.
"""
import json
from pathlib import Path

_DATA_FILE = Path(__file__).with_name("country_codes.json")
COUNTRY_OPTIONS = sorted(
    json.loads(_DATA_FILE.read_text(encoding="utf-8"))["countries"]
)
