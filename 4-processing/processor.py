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


# Anything that is not a letter or a digit separates one token from the next.
_TOKEN_SPLIT = re.compile(r"[^a-z0-9]+")
# Below this length a token carries no meaning of its own ("in", "of", "and").
_MIN_TOKEN_LEN = 3


def tokenize_keyword_enriched(keyword: str) -> list[str]:
    """
    Split a user keyword into the tokens that must ALL be present for the keyword
    to match an enriched row.

    Matching a multi-word keyword as one contiguous string is far too strict for
    news text: "silicon wafers" matched 0 of 99,175 enriched rows, because a
    headline says "chip firm buys wafer plant", never the procurement phrase
    verbatim. Splitting into tokens and requiring all of them keeps the keyword's
    precision without demanding that the words be adjacent.

    Tokens shorter than three characters are dropped from MULTI-word keywords, so
    "burn-in boards" is not dragged down by "in"; a keyword that is itself short
    ("R&D" -> ["r", "d"]) keeps its tokens, since dropping them would leave
    nothing to match on.

    Examples
    --------
        "silicon wafers"  -> ["silicon", "wafers"]
        "burn-in boards"  -> ["burn", "boards"]
        "TSMC"            -> ["tsmc"]
        "R&D"             -> ["r", "d"]
    """
    if not keyword:
        return []

    toks = [t for t in _TOKEN_SPLIT.split(keyword.lower()) if t]
    if len(toks) > 1:
        longer = [t for t in toks if len(t) >= _MIN_TOKEN_LEN]
        if longer:
            toks = longer

    # Dedupe while preserving order, so the generated SQL is stable.
    seen: set[str] = set()
    out: list[str] = []
    for t in toks:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def stem_token(token: str) -> str:
    """
    Strip a single trailing 's' so that singular and plural forms collapse onto
    one another: "chips" -> "chip". "chip" alone matched 125 headlines that
    "chips" would have missed, so the two must not be treated as different words.

    The SAME rule is applied to the row's own tokens in SQL, so both sides meet in
    the middle rather than one side being expanded into variants — expanding would
    multiply an already large predicate count and exhaust ClickHouse's memory.

    Words of three characters or fewer are left alone, so "gas" does not become
    "ga". Over-stemming is harmless here precisely because it is symmetric.
    """
    if len(token) > _MIN_TOKEN_LEN and token.endswith("s"):
        return token[:-1]
    return token


# The row's text, lower-cased, split into words, and stemmed by the same rule as
# stem_token(). Evaluated once per row; ClickHouse reuses it across the keyword
# predicates rather than rebuilding it for each one.
#
# splitByRegexp('[^a-z0-9]+', …) rather than the cheaper splitByNonAlpha, because
# splitByNonAlpha does not treat non-ASCII punctuation as a separator: a headline
# reading "Novo Nordisk’s owner" (with a typographic apostrophe) tokenised to
# "nordisk’s", which stemmed to "nordisk’" and never matched the keyword
# "Nordisk". The pattern is character-for-character the one the Spark path uses,
# so the two engines tokenise identically.
_ROW_TOKENS_SQL = (
    "arrayMap(t -> if(length(t) > {min_len} AND endsWith(t, 's'),"
    " substring(t, 1, length(t) - 1), t),"
    " splitByRegexp('[^a-z0-9]+', lower(concat({title}, ' ', {keywords}))))"
)


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
