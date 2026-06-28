"""
src/processing/processor.py

"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# KEYWORD NORMALISATION (dashboard "keyword in URL" filter)
# ═══════════════════════════════════════════════════════════════════════════════
#
# A user keyword is matched case-insensitively as a substring of the article URL.
# Before matching, each keyword is normalised into a SET of 1/2/4 variants; the
# event/article matches if ANY variant is a substring of the lower-cased URL.
#
# Rules (in order):
#   0. Lower-case; remove leading AND trailing spaces; remove spaces directly
#      adjacent to a math/logic symbol; collapse remaining repeated spaces to one.
#   1. Ampersand (handled separately, NOT space-stripped) -> two branches:
#        (a) removed, (b) replaced with the spaced word " and " (never glued).
#   2. Math/logic symbol present -> two branches:
#        (a) each symbol -> '-', (b) each symbol removed.
#      '&' is excluded from this set; '!' and '|' are plain punctuation; a literal
#      '-' is a separator, never a minus.
#   3. Per variant: re-collapse spaces, spaces -> '-', strip remaining punctuation
#      (anything not a-z/0-9/'-'). Hyphens are NEVER collapsed or trimmed
#      (so "C++" -> "c--").

# Math/logic symbols (ampersand excluded — it has its own rule).
_MATH_LOGIC = "+*/=<>%^~±×÷≤≥≠√∑∏∞¬∧∨→↔"
_MATH_LOGIC_SET = set(_MATH_LOGIC)
# Strip whitespace on either side of any math/logic symbol.
_SPACE_AROUND_SYMBOL = re.compile(r"\s*([" + re.escape(_MATH_LOGIC) + r"])\s*")
_MULTISPACE = re.compile(r" +")


def _finish_variant(s: str) -> str:
    """Re-collapse spaces, spaces->hyphen, strip leftover punctuation."""
    s = _MULTISPACE.sub(" ", s).strip(" ")
    s = s.replace(" ", "-")
    return re.sub(r"[^a-z0-9-]", "", s)


def normalize_keyword(keyword: str) -> set[str]:
    """
    Expand a raw user keyword into the set of normalised variants to match
    against a URL. See the rules documented above for full behaviour.

    Examples
    --------
        "oil & gas"   -> {"oil-gas", "oil-and-gas"}
        "R&D"         -> {"rd", "r-and-d"}
        "C++"         -> {"c--", "c"}
        "price > cost!" -> {"price-cost", "pricecost"}
        "A & B + C"   -> {"a-b-c", "a-bc", "a-and-b-c", "a-and-bc"}
    """
    if keyword is None:
        return set()

    # ── Step 0 — common pre-clean ─────────────────────────────────────────────
    s = keyword.lower().strip()                 # leading + trailing spaces gone
    s = _SPACE_AROUND_SYMBOL.sub(r"\1", s)       # symbols hug their neighbours
    s = _MULTISPACE.sub(" ", s)

    # ── Step 1 — ampersand branches ───────────────────────────────────────────
    if "&" in s:
        amp_variants = [s.replace("&", ""), s.replace("&", " and ")]
    else:
        amp_variants = [s]

    # ── Step 2 — math/logic branches (per ampersand variant) ──────────────────
    branches: list[str] = []
    for v in amp_variants:
        if any(c in _MATH_LOGIC_SET for c in v):
            hyphened = "".join("-" if c in _MATH_LOGIC_SET else c for c in v)
            removed = "".join("" if c in _MATH_LOGIC_SET else c for c in v)
            branches.extend([hyphened, removed])
        else:
            branches.append(v)

    # ── Step 3 — finish + dedupe ──────────────────────────────────────────────
    out = {_finish_variant(b) for b in branches}
    out.discard("")
    return out


def normalize_keyword_enriched(keyword: str) -> str:
    """
    Light normalisation used when matching against the ENRICHED fields
    (article_keywords / article_title) — deliberately different from the URL
    normalisation above.

    The ONLY edits: strip leading/trailing spaces and collapse runs of internal
    spaces to a single space. Every symbol is kept as-is, nothing is turned into
    a hyphen, and case is preserved. Returns "" for an empty/blank keyword.
    """
    if not keyword:
        return ""
    return _MULTISPACE.sub(" ", keyword.strip())


def build_keyword_clause(
    keywords,
    url_column: str = "MentionIdentifier",
    title_column: str = "article_title",
    keywords_column: str = "article_keywords",
    enriched_column: str = "enriched",
):
    """
    Row-conditional keyword match for the enriched mentions table. A row matches
    if a keyword is found in the field appropriate to THAT row:

        enriched = 0                           -> search the URL
        enriched = 1 AND article_keywords = '' -> search lower(article_title)
        enriched = 1 AND article_keywords <> ''-> search article_keywords

    Two different normalisations are applied to the search words:
        * URL branch      -> normalize_keyword()          (lower-case, hyphenation,
                                                            &/math-logic branching)
        * enriched branch -> normalize_keyword_enriched() (trim + collapse spaces
                                                            only; symbols & case kept)

    URL matches use LIKE (backed by the ngrambf index on lower(url)); enriched
    matches use position() — an exact, wildcard-free substring search — so that
    symbols kept in the keyword are matched literally.

    Returns (sql_fragment, params); ("", {}) when there are no usable keywords.
    """
    if not keywords:
        return "", {}

    url_variants: set[str] = set()
    for kw in keywords:
        url_variants |= normalize_keyword(kw)

    enr_variants = {normalize_keyword_enriched(kw) for kw in keywords}
    enr_variants.discard("")

    params: dict = {}
    branches: list[str] = []

    # enriched = 0 → the URL (heavy/hyphen variants, case-insensitive via lower)
    if url_variants:
        likes = []
        for i, v in enumerate(sorted(url_variants)):
            key = f"kw_url_{i}"
            params[key] = f"%{v}%"
            likes.append(f"lower({url_column}) LIKE %({key})s")
        branches.append(f"({enriched_column} = 0 AND (" + " OR ".join(likes) + "))")

    # enriched = 1 → article_title when keywords are empty, else article_keywords
    if enr_variants:
        title_pos, kw_pos = [], []
        for i, v in enumerate(sorted(enr_variants)):
            key = f"kw_enr_{i}"
            params[key] = v  # literal substring; symbols & case preserved
            title_pos.append(f"position(lower({title_column}), %({key})s) > 0")
            kw_pos.append(f"position({keywords_column}, %({key})s) > 0")
        branches.append(
            f"({enriched_column} = 1 AND {keywords_column} = '' AND ("
            + " OR ".join(title_pos) + "))"
        )
        branches.append(
            f"({enriched_column} = 1 AND {keywords_column} != '' AND ("
            + " OR ".join(kw_pos) + "))"
        )

    return "(" + " OR ".join(branches) + ")", params


# ═══════════════════════════════════════════════════════════════════════════════
# CODE NORMALISATION
# ═══════════════════════════════════════════════════════════════════════════════

# FIPS → ISO mapping for known divergences.
# Allows users to supply either ISO or FIPS codes and still get correct matches.
FIPS_TO_ISO: dict[str, str] = {
    "EI": "IE",  # Ireland
    "UK": "GB",  # United Kingdom
    "GM": "DE",  # Germany
    "IV": "CI",  # Ivory Coast
    "SF": "ZA",  # South Africa
    "TW": "TW",  # Taiwan (same in both standards)
}

ISO_TO_FIPS: dict[str, str] = {v: k for k, v in FIPS_TO_ISO.items()}


def _normalise_codes(codes: set[str]) -> set[str]:
    """
    Expand a set of country codes by adding known variants (FIPS and ISO)
    so that matching works regardless of which standard the user supplied.
    """
    expanded = set(codes)
    for code in list(codes):
        upper = code.upper()
        expanded.add(upper)
        if upper in FIPS_TO_ISO:
            expanded.add(FIPS_TO_ISO[upper])
        if upper in ISO_TO_FIPS:
            expanded.add(ISO_TO_FIPS[upper])
    return expanded


# ═══════════════════════════════════════════════════════════════════════════════
# GEOGRAPHIC FILTER
# ═══════════════════════════════════════════════════════════════════════════════

def _event_country_codes(event: dict) -> set[str]:
    """
    Return all country codes present in a silver event.
    Checks both CAMEO (country_code) and FIPS (fips_country) fields,
    plus actor-level geo fields when available.
    """
    codes = set()
    for field in ("country_code", "fips_country",
                  "actor1_country", "actor2_country"):
        val = event.get(field, "")
        if val:
            codes.add(val.upper().strip())
    return codes


def matches_geography(
    event: dict,
    cameo_codes: Optional[set[str]] = None,
    fips_codes: Optional[set[str]] = None,
) -> bool:
    """
    Return True if the event touches at least one of the user's geographies.

    Logic:
        - If both cameo_codes and fips_codes are None (no filter),
          all events pass through.
        - Otherwise: the event must have at least one country code that
          matches any of the supplied codes (CAMEO OR FIPS).

    Parameters
    ----------
    event       : dict — silver event (output of to_silver_event)
    cameo_codes : set  — CAMEO country codes (e.g. {"US", "CH", "RS"})
    fips_codes  : set  — FIPS country codes  (e.g. {"US", "CH", "EI"})
    """
    if cameo_codes is None and fips_codes is None:
        return True

    allowed: set[str] = set()
    if cameo_codes:
        allowed.update(_normalise_codes(cameo_codes))
    if fips_codes:
        allowed.update(_normalise_codes(fips_codes))

    event_codes = _event_country_codes(event)
    return bool(event_codes & allowed)


# ═══════════════════════════════════════════════════════════════════════════════
# SILVER → GOLD
# ═══════════════════════════════════════════════════════════════════════════════

def silver_to_gold(
    silver_events: list[dict],
    cameo_codes: Optional[set[str]] = None,
    fips_codes: Optional[set[str]] = None,
) -> list[dict]:
    """
    Transform a list of silver events into the user-specific gold layer.

    Internal pipeline:
        1. Geographic filter (CAMEO country code AND/OR FIPS)
        2. Add "layer": "gold" field

    Parameters
    ----------
    silver_events   : list[dict] — parser output (to_silver_event)
    cameo_codes     : set[str]   — user's CAMEO country codes
    fips_codes      : set[str]   — user's FIPS country codes

    Returns
    -------
    list[dict] — gold events, each with "layer" = "gold"
    """
    gold: list[dict] = []

    for event in silver_events:
        # Step 1 — geographic filter
        if not matches_geography(event, cameo_codes, fips_codes):
            continue

        # Step 2 — copy event and mark the layer
        gold_event = dict(event)
        gold_event["layer"] = "gold"
        gold.append(gold_event)

    logger.info(
        "Processor: %d silver → %d gold (geo filter: CAMEO=%s, FIPS=%s)",
        len(silver_events), len(gold),
        cameo_codes or "none", fips_codes or "none",
    )
    return gold


# ═══════════════════════════════════════════════════════════════════════════════
# USER PROFILE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def build_user_geo_filter(user_profile: dict) -> tuple[set[str], set[str]]:
    """
    Extract CAMEO and FIPS country code sets from a user profile dict
    (as received from the ingestion layer / central database).

    Expected profile format:
        {
            "cameo_countries": ["US", "CN", "DE"],
            "fips_countries":  ["EI", "UK"],        # optional
            ...
        }

    Returns
    -------
    (cameo_codes, fips_codes) : tuple of two set[str]
                                Returns None for an empty set.
    """
    cameo = set(c.upper() for c in user_profile.get("cameo_countries", []))
    fips = set(c.upper() for c in user_profile.get("fips_countries", []))
    return cameo if cameo else None, fips if fips else None
