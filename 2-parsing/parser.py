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

EVENT_COLUMNS_TO_DROP = [
    column for column in GDELT_COLUMNS
    if column not in {
        "GlobalEventID", "Day", "Actor1Name", "Actor1CountryCode",
        "Actor2Name", "Actor2CountryCode", "EventCode", "EventRootCode",
        "GoldsteinScale", "NumArticles", "AvgTone",
        "Actor1Geo_CountryCode", "Actor2Geo_CountryCode",
        "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_Lat",
        "ActionGeo_Long", "DATEADDED", "SOURCEURL",
    }
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

# ── F3: alternative supply-chain keywords (chosen from thematic analysis) ────────────────────────────────────
SUPPLY_CHAIN_KEYWORDS = {
    "port",
    "harbor", "harbour",
    "shipping",
    "freight",
    "cargo", "cargoes"
    "customs",
    "tariff",
    "logistics",
    "supply chain", "supply-chain",
    "supply-side",
    "warehouse",
    "storage",
    "refinery", "refineries"
    "factory", "factories",
    "plant",
    "semiconductor",
    "microchip",
    "oil",
    "gas",
    "pipeline",
    "railway",
    "railroad",
    "airport",
    "tanker",
    "container",

    "avalanche",
    "blizzard",
    "wildfire",
    "bushfire",
    "cold wave",
    "derecho",
    "drought",
    "earthquake",
    "flash flood",
    "haboob",
    "heat wave",
    "hurricane",
    "lahar",
    "landslide",
    "limnic eruption",
    "polar vortex event",
    "riverine flood",
    "sinkhole",
    "storm surge",
    "tornado",
    "tornadoes",
    "tropical cyclone",
    "tsunami",
    "typhoon",
    "volcanic eruption",
    "waterspout",

    "inflation",
    "deflation",

    "terrorist attack",
}
"""
"RISK_CATEGORY_OPTIONS", for reference.
Many are being considered not for this parser logic but for the user-specific processing preferences downstream.

Demand-side economic conditions
Supply-side financial instability
Supply-side raw materials quality issues
Recent supply-side transit-related accidents
Delay in obtaining governmental approvals
New regulatory, legal, or bureaucratic plans and policies
Climatic adverse situations
Terrorist attacks on infrastructure
Civil movements
Labour disputes involving worker associations in key firms
Dissatisfied employee unions from key firms
Key firms choosing mergers
Key firms choosing divestments
Disasters in supply chain locations
Disasters affecting involved companies
Recent supply-chain-related theft history
Recent supply-chain-related counterfeiting history
Recent supply-side contract non-compliance
Variations in climatic conditions
Scandals related to key firms
Sanctions on key firms
Inflation in supply-side economy
Major supply-side accidents or breakdowns
Incentivising announcements by ruling political figures
"""


# ═══════════════════════════════════════════════════════════════════════════════
# KEY RENAMING — VESTIGIAL. Nothing in the pipeline reaches this any more.
#
# These two functions convert a positionally-keyed record ({0: …, 1: …}) into a
# named one ({"GLOBALEVENTID": …}). They date from the removed Kafka design, in
# which the poller sent `df.to_dict()` of a header-less frame through a broker.
#
# Every caller today passes NAMED keys already, because both read their CSVs with
# an explicit `names=` argument:
#     2-parsing/main.py:262      pd.read_csv(..., header=None, names=GDELT_COLUMNS)
#     bootstrap/bulk_load.py:100 pd.read_csv(..., header=None, names=columns)
# `header=None` alone would give {0: …, 1: …}; `header=None` WITH `names=` gives
# {"GLOBALEVENTID": …}. Verified directly with pandas.
#
# They are kept rather than deleted because they are inert and correct: dead code
# that cannot fire, guarding an input shape that would otherwise crash the filter
# if a caller ever did hand over a header-less frame. See passes_filter() for the
# cost of the guard that decides whether to call them (one iterator and two type
# checks per row).
# ═══════════════════════════════════════════════════════════════════════════════

def rename_integer_keys(record: dict) -> dict:
    """
    Map a positionally-keyed event record onto GDELT_COLUMNS. NOT REACHED — see
    the section header above.

    Accepts both {0: …} and {"0": …}. The integer form is what pandas produces
    from a header-less frame read without `names=`; the STRING form only ever
    arose because the old Kafka path serialised each record with json.dumps(),
    and JSON object keys are always strings, so json.loads() returned "0", "1",
    … on the far side. There is no serialisation boundary left in the pipeline,
    so the string branch can no longer be reached by anything.

    Both are kept because the dual lookup is what makes the function total: any
    positional record maps correctly regardless of how its keys were spelled, and
    a column absent from a short row is padded with "" rather than raising.
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

    Takes NAMED-column dicts, which is what both callers pass. It also tolerates
    positionally-keyed ones, though nothing produces those any more — see the
    KEY RENAMING header above.
    """
    # Positional-record guard, vestigial. It cannot fire on current input: both
    # callers read their CSV with `names=`, so the first key is a column name
    # like "GLOBALEVENTID", which is neither an int nor a digit string.
    #
    # Left in place because it is genuinely cheap and cannot misfire. It costs
    # one iterator and two isinstance checks per row, and no GDELT column name is
    # a digit string, so a named record can never be mistaken for a positional
    # one. Deleting it (with the two rename_* functions) would be safe today and
    # would only cost the tolerance for a caller that hands over a header-less
    # frame — which is exactly the mistake this catches instead of crashing on
    # every lookup returning "".
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
# GDELT publishes it as a separate CSV every 15 minutes, alongside the events
# file; the ingestion layer downloads both and drops them on the shared volume
# for this layer to pick up. (It used to arrive on a Kafka topic named
# 'gdelt_mentions_raw' — there is no broker in the pipeline now.) The key field
# is MentionIdentifier, which holds the source-article URL that the validation
# layer scrapes with Newspaper3k.

# GDELT 2.0 Mentions columns (16 columns, 0-based index)
# ── Field-width guard ────────────────────────────────────────────────────────
# GDELT's column count is a fixed part of its format, and every read in this
# project supplies the names positionally (`header=None, names=[...]`). That
# combination is silently destructive if the real width ever changes, which is
# why this check exists rather than trusting the feed.
#
# Given N names and N+1 fields, pandas does NOT raise. It promotes the first
# field to the DataFrame index and shifts every name one position left, so each
# value lands under its neighbour's name. Measured on a real 513-row slice with
# one extra column appended: no exception, shape still (513, 61), but
# GlobalEventID held `20250816` (a Day), SOURCEURL held the new value, and
# passes_filter selected 2 rows instead of 11.
#
# Downstream that is worse than an error, because the corruption is plausible:
#   * parsing REWRITES the file at the declared width, so the extra field is
#     gone by the time validation sees it — a guard there alone cannot fire;
#   * events are caught eventually, but only by luck of typing: GLOBALEVENTID and
#     DATEADDED are UInt64 in ClickHouse, so shifted text fails the insert with
#     `TypeMismatchError`, and the slice is retried 3x then dead-lettered;
#   * MENTIONS ARE NOT CAUGHT AT ALL. Shifting there puts EventTimeDate
#     (`20260816133000`) into GLOBALEVENTID, which is a valid UInt64, so the rows
#     insert cleanly and attach to the wrong events.
#
# Too FEW fields is rejected as well. The expected width here is the RAW input
# width, not the length of the schema the rows are eventually stored under —
# validation reads a 16-field mentions file against a 19-name list on purpose,
# padding the three enrichment columns — so the two must not be confused.
EVENTS_FIELD_COUNT   = 61
MENTIONS_FIELD_COUNT = 16


def check_field_width(path, expected: int, kind: str) -> None:
    """
    Raise unless the first non-blank line of `path` has exactly `expected`
    tab-separated fields.

    An EMPTY file passes. That is not laxity: a slice in which the relevance
    filter matched nothing is written as a 0-byte file, which is legitimate and
    common, and whose first line would otherwise count as one field and be
    rejected. Emptiness is a row-count condition, not a schema one.

    The first NON-BLANK line is measured, so a leading newline cannot be read as
    a one-field row.
    """
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if not line.strip():
                continue
            actual = len(line.rstrip("\n").split("\t"))
            if actual > expected:
                raise ValueError(
                    f"Looks like GDELT added more columns than expected to this "
                    f"file! {kind} file {getattr(path, 'name', path)} has {actual} "
                    f"fields, expected {expected}. Refusing to parse it: reading "
                    f"it positionally would shift every column and corrupt the "
                    f"slice silently."
                )
            if actual < expected:
                raise ValueError(
                    f"Looks like GDELT removed columns, or this file is "
                    f"truncated! {kind} file {getattr(path, 'name', path)} has "
                    f"{actual} fields, expected {expected}. Refusing to parse it."
                )
            return          # first non-blank line is the whole check
    return                  # empty file: nothing to check


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

MENTION_COLUMNS_TO_DROP = [
    "EventTimeDate", "MentionType", "Actor1CharOffset", "Actor2CharOffset",
    "ActionCharOffset", "MentionDocTranslationInfo", "Extras", "enriched",
]

PARSED_EVENT_COLUMNS = [
    column for column in GDELT_COLUMNS if column not in EVENT_COLUMNS_TO_DROP
]
PARSED_MENTION_COLUMNS = [
    column for column in MENTIONS_COLUMNS if column not in MENTION_COLUMNS_TO_DROP
]


def rename_mention_integer_keys(record: dict) -> dict:
    """
    Same as rename_integer_keys(), for the mentions table — and equally NOT
    REACHED, for the same reason: bulk_load.py and 2-parsing/main.py both read
    the mentions CSV with `names=MENTIONS_COLUMNS`, so the records are named
    before they arrive here.
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
