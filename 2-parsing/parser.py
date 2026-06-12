"""
2-parsing/parser.py
--------------------

"""

import math
import logging

logger = logging.getLogger(__name__)

# ── GDELT column names (61 columns, 0-based index) ───────────────────────────
GDELT_COLUMNS = [
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
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
    "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code", "Actor1Geo_Lat", "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat", "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_ADM2Code", "ActionGeo_Lat", "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
]

# ── F1: CAMEO event codes relevant to supply-chain risk ──────────────────────
# Source: Chukwuka et al. (2023), Sultana et al. (2024)
RELEVANT_EVENT_CODES = {
    # Economic sanctions
    "1721", "1722", "1723", "1724",
    # Trade embargo / commercial blockade
    "163", "1631", "1632", "1633",
    # Strikes and labour protests
    "141", "1411", "1412", "1413", "143", "145",
    # Physical blockades / infrastructure closures
    "191", "1911", "1912",
    # Infrastructure attacks
    "180", "182", "1821", "1822", "1823",
    # Trade threats / embargo threats
    "171", "172", "173",
    # Seizures / expropriations
    "175", "1751", "1752",
    # Armed conflict (infrastructure destruction)
    "193", "194", "195", "196",
}

RELEVANT_ROOT_CODES = {
    "14",   # Protest
    "15",   # Challenge use of force
    "17",   # Coerce
    "18",   # Assault
    "19",   # Fight
    "20",   # Use unconventional mass violence
}

# ── F2: relevant actor types and known groups ─────────────────────────────────
RELEVANT_TYPE_CODES = {
    "BUS",  # Business / corporations
    "GOV",  # Government
    "LAB",  # Labour / trade unions
    "MNC",  # Multinational corporations
    "IGO",  # Intergovernmental organisations
}

RELEVANT_KNOWN_GROUPS = {
    "OPEC", "WTO", "IMF", "WORLDBANK", "EU", "ASEAN",
    "NATO",  # relevant for military logistics blockades
    "UN",
}

# ── F3: alternative supply-chain keywords ────────────────────────────────────
SUPPLY_CHAIN_KEYWORDS = {
    "port", "porto", "harbor", "harbour",
    "shipping", "freight", "cargo",
    "customs", "dogana", "tariff", "tariffa",
    "logistics", "logistica", "supply chain", "supply-chain",
    "warehouse", "magazzino", "storage",
    "refinery", "raffineria",
    "factory", "fabbrica", "plant",
    "semiconductor", "microchip",
    "oil", "gas", "pipeline",
    "railway", "railroad", "ferrovia",
    "airport", "aeroporto",
    "tanker", "container",
}


# ═══════════════════════════════════════════════════════════════════════════════
# KEY RENAMING — ingestion compatibility
# ═══════════════════════════════════════════════════════════════════════════════

def rename_integer_keys(record: dict) -> dict:
    """
    The ingestion poller (poller.py) reads GDELT CSVs with pandas (no header)
    and calls df.to_dict(orient='records'), producing dicts with integer keys:
        {0: "1381729282", 1: "20260430", 2: "202604", …}

    IMPORTANT: the poller then serialises each record with json.dumps() before
    sending it to Kafka. JSON object keys are ALWAYS strings, so after the
    consumer's json.loads() the keys arrive as "0", "1", "2", … (strings),
    not integers. This function therefore accepts BOTH forms: it first tries
    the integer key (direct/local use) and falls back to the string key
    (the real Kafka path). Missing columns are padded with empty strings.
    """
    return {
        col: record.get(i, record.get(str(i), ""))
        for i, col in enumerate(GDELT_COLUMNS)
    }


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_str(value) -> str:
    """Convert a potentially NaN / None value to an empty string."""
    if value is None:
        return ""
    try:
        if math.isnan(float(value)):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


# ═══════════════════════════════════════════════════════════════════════════════
# FILTERS
# ═══════════════════════════════════════════════════════════════════════════════

def has_relevant_event_code(record: dict) -> bool:
    """F1: 4-digit EventCode in the relevant list OR EventRootCode in the macro list."""
    code = _safe_str(record.get("EventCode", ""))
    root = _safe_str(record.get("EventRootCode", ""))
    return code in RELEVANT_EVENT_CODES or root in RELEVANT_ROOT_CODES


def has_relevant_type_or_group(record: dict) -> bool:
    """F2: at least one TypeCode or KnownGroupCode field contains a relevant actor."""
    type_fields = [
        "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
        "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    ]
    group_fields = ["Actor1KnownGroupCode", "Actor2KnownGroupCode"]

    for field in type_fields:
        if _safe_str(record.get(field, "")).upper() in RELEVANT_TYPE_CODES:
            return True
    for field in group_fields:
        if _safe_str(record.get(field, "")).upper() in RELEVANT_KNOWN_GROUPS:
            return True
    return False


def has_alternative_keyword(record: dict) -> bool:
    """F3: at least one supply-chain keyword found in Actor1Name, Actor2Name, or SOURCEURL."""
    text_fields = ["Actor1Name", "Actor2Name", "SOURCEURL"]
    combined = " ".join(_safe_str(record.get(f, "")) for f in text_fields).lower()
    return any(kw in combined for kw in SUPPLY_CHAIN_KEYWORDS)


def has_source_url(record: dict) -> bool:
    """Validation: every silver event must have a link to its source article."""
    return bool(_safe_str(record.get("SOURCEURL", "")))


def passes_filter(record: dict) -> bool:
    """
    Return True if the record satisfies all filter + validation criteria:
        F1 AND (F2 OR F3) AND has_source_url
    Accepts either integer-keyed dicts (from Kafka) or named-column dicts.
    If integer keys are detected, rename_integer_keys() is called automatically.
    """
    # Auto-detect index-keyed records (from Kafka ingestion). Keys may be
    # integers (local use) or digit strings "0".."60" (after JSON round-trip).
    if record:
        first_key = next(iter(record))
        if isinstance(first_key, int) or (isinstance(first_key, str) and first_key.isdigit()):
            record = rename_integer_keys(record)

    if not has_relevant_event_code(record):
        return False
    if not (has_relevant_type_or_group(record) or has_alternative_keyword(record)):
        return False
    if not has_source_url(record):
        return False
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# RISK SCORING
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_risk_score(
    goldstein: float,
    num_articles: int,
    avg_tone: float,
) -> float:
    """
    Compute a normalised risk score in [0.0, 10.0].

    Components:
        base_score     — derived from GoldsteinScale (-10/+10 → 10/0)
        coverage_boost — logarithmic amplifier on article count (max +1.0)
        tone_boost     — amplifier on negative average tone (max +1.0)
    """
    try:
        goldstein = float(goldstein)
        if math.isnan(goldstein):
            goldstein = 0.0
    except (TypeError, ValueError):
        goldstein = 0.0

    try:
        num_articles = max(int(num_articles), 1)
    except (TypeError, ValueError):
        num_articles = 1

    try:
        avg_tone = float(avg_tone)
        if math.isnan(avg_tone):
            avg_tone = 0.0
    except (TypeError, ValueError):
        avg_tone = 0.0

    base_score     = ((-goldstein) + 10) / 2                 # [0, 10]
    coverage_boost = min(math.log10(num_articles), 2) / 2    # [0, 1]
    tone_boost     = min(abs(min(avg_tone, 0.0)) / 20.0, 1.0)  # [0, 1]

    return round(min(base_score + coverage_boost + tone_boost, 10.0), 2)


# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT: SILVER EVENT
# ═══════════════════════════════════════════════════════════════════════════════

def to_silver_event(record: dict) -> dict:
    """
    Convert a filtered, named-column GDELT record into the silver schema dict.

    Output schema:
        event_id, date, event_code, event_root,
        actor1, actor2, country_code, fips_country,
        lat, lon, goldstein, avg_tone, num_articles,
        risk_score, source_url, source
    """
    def _float(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    def _int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0

    goldstein    = _float(record.get("GoldsteinScale", 0))
    num_articles = _int(record.get("NumArticles", 1))
    avg_tone     = _float(record.get("AvgTone", 0))

    adm1         = _safe_str(record.get("ActionGeo_ADM1Code", ""))
    fips_country = adm1[:2] if len(adm1) >= 2 else ""

    return {
        "event_id":     _safe_str(record.get("GlobalEventID", "")),
        "date":         _safe_str(record.get("Day", "")),
        "event_code":   _safe_str(record.get("EventCode", "")),
        "event_root":   _safe_str(record.get("EventRootCode", "")),
        "actor1":       _safe_str(record.get("Actor1Name", "")),
        "actor2":       _safe_str(record.get("Actor2Name", "")),
        "country_code": _safe_str(record.get("ActionGeo_CountryCode", "")),
        "fips_country": fips_country,
        "lat":          _float(record.get("ActionGeo_Lat", 0)),
        "lon":          _float(record.get("ActionGeo_Long", 0)),
        "goldstein":    goldstein,
        "avg_tone":     avg_tone,
        "num_articles": num_articles,
        "risk_score":   calculate_risk_score(goldstein, num_articles, avg_tone),
        "source_url":   _safe_str(record.get("SOURCEURL", "")),
        "source":       "gdelt_events",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# GDELT MENTIONS TABLE
# ═══════════════════════════════════════════════════════════════════════════════
# The mentions table references events (one event can have many mentions).
# It is published by GDELT as a separate CSV every 15 minutes and arrives on
# the Kafka topic 'gdelt_mentions_raw'. The key field is MentionIdentifier,
# which holds the source-article URL used for Newspaper3k enrichment.

# GDELT 2.0 Mentions columns (16 columns, 0-based index)
MENTIONS_COLUMNS = [
    "GlobalEventID",            # 0  links the mention to an event
    "EventTimeDate",            # 1
    "MentionTimeDate",          # 2
    "MentionType",              # 3  1=web, 2=citation, 3=core, ...
    "MentionSourceName",        # 4  e.g. "bbc.co.uk"
    "MentionIdentifier",        # 5  the article URL  ← scraped by Newspaper3k
    "SentenceID",               # 6
    "Actor1CharOffset",         # 7
    "Actor2CharOffset",         # 8
    "ActionCharOffset",         # 9
    "InRawText",                # 10
    "Confidence",               # 11
    "MentionDocLen",            # 12
    "MentionDocTone",           # 13
    "MentionDocTranslationInfo",# 14
    "Extras",                   # 15
]


def rename_mention_integer_keys(record: dict) -> dict:
    """
    Same idea as rename_integer_keys() but for the mentions table.
    The poller sends mention rows with integer keys (0, 1, 2 …) which become
    string keys ("0", "1", …) after the json.dumps()/json.loads() round-trip
    through Kafka. Both forms are accepted: integer key first, string key as
    fallback.
    """
    return {
        col: record.get(i, record.get(str(i), ""))
        for i, col in enumerate(MENTIONS_COLUMNS)
    }


def to_silver_mention(record: dict) -> dict:
    """
    Convert a named-column GDELT mention record into the silver-mention schema.

    The enrichment fields (article_title, article_keywords, enriched) are
    left empty here; they are filled later by enrichment.enrich_mentions_parallel().

    Output schema:
        event_id, event_time, mention_time, mention_type, source_name,
        mention_url, confidence, doc_tone,
        article_title, article_keywords, enriched
    """
    def _float(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    return {
        "event_id":         _safe_str(record.get("GlobalEventID", "")),
        "event_time":       _safe_str(record.get("EventTimeDate", "")),
        "mention_time":     _safe_str(record.get("MentionTimeDate", "")),
        "mention_type":     _safe_str(record.get("MentionType", "")),
        "source_name":      _safe_str(record.get("MentionSourceName", "")),
        "mention_url":      _safe_str(record.get("MentionIdentifier", "")),
        "confidence":       _float(record.get("Confidence", 0)),
        "doc_tone":         _float(record.get("MentionDocTone", 0)),
        # Enrichment fields — populated by Newspaper3k downstream
        "article_title":    "",
        "article_keywords": "",
        "enriched":         False,
    }
