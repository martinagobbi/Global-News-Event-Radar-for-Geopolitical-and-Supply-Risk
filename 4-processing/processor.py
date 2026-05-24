"""
src/processing/processor.py

"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


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
# USER-SPECIFIC RISK SCORE (optional)
# ═══════════════════════════════════════════════════════════════════════════════

def apply_country_weight(
    base_score: float,
    country_code: str,
    country_weights: Optional[dict[str, float]] = None,
) -> float:
    """
    Multiply the base risk_score by a country-specific weight,
    if the user has defined critical countries for their supply chain.

    Example:
        country_weights = {"CN": 1.5, "RU": 2.0, "TW": 1.8}
        → an event in China will have score × 1.5

    The result is always clamped to [0.0, 10.0].
    """
    if not country_weights:
        return base_score
    weight = country_weights.get(country_code.upper(), 1.0)
    return round(min(base_score * weight, 10.0), 2)


# ═══════════════════════════════════════════════════════════════════════════════
# SILVER → GOLD
# ═══════════════════════════════════════════════════════════════════════════════

def silver_to_gold(
    silver_events: list[dict],
    cameo_codes: Optional[set[str]] = None,
    fips_codes: Optional[set[str]] = None,
    country_weights: Optional[dict[str, float]] = None,
    min_risk_score: float = 0.0,
) -> list[dict]:
    """
    Transform a list of silver events into the user-specific gold layer.

    Internal pipeline:
        1. Geographic filter (CAMEO country code AND/OR FIPS)
        2. Apply country weights (optional)
        3. Minimum risk score threshold (optional, default 0 = no filter)
        4. Add "layer": "gold" field

    Parameters
    ----------
    silver_events   : list[dict] — parser output (to_silver_event)
    cameo_codes     : set[str]   — user's CAMEO country codes
    fips_codes      : set[str]   — user's FIPS country codes
    country_weights : dict       — per-country multipliers (e.g. {"CN": 1.5})
    min_risk_score  : float      — minimum risk score threshold [0-10]

    Returns
    -------
    list[dict] — gold events, each with "layer" = "gold"
                 and a potentially re-weighted "risk_score"
    """
    gold: list[dict] = []

    for event in silver_events:
        # Step 1 — geographic filter
        if not matches_geography(event, cameo_codes, fips_codes):
            continue

        # Step 2 — copy event and apply country weight
        gold_event = dict(event)
        gold_event["risk_score"] = apply_country_weight(
            base_score=event.get("risk_score", 0.0),
            country_code=event.get("country_code", ""),
            country_weights=country_weights,
        )

        # Step 3 — minimum risk score threshold
        if gold_event["risk_score"] < min_risk_score:
            continue

        # Step 4 — mark the layer
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
