#!/usr/bin/env python
"""
dev_tools_for_filter_diagnostics/explain_gold.py
------------------------------------------------
Explain, for every article that reached the gold layer, WHY it got there.

The pipeline discards roughly 97% of each GDELT slice on the way to silver, and
then a further large fraction on the way to gold. When a user's pool looks too
small (or too large), the question is always the same: which rule let these
particular articles through, and which rule is rejecting the rest? This tool
answers the first half by re-deriving the decision for every row that survived.

It is DIAGNOSTIC ONLY:
    * it opens PostgreSQL, ClickHouse and MongoDB read-only,
    * it writes nothing to any store,
    * its only output is gold_provenance.csv, next to this file.

It re-uses the pipeline's own filter code rather than reimplementing it:
    * 2-parsing/parser.py           — the bronze->silver criteria (F1, F2, F3)
    * 4-processing/countries.py     — territory name -> CAMEO / FIPS codes
    * 4-processing/processor.py     — keyword tokenisation and normalisation
so the explanation cannot drift from the filters actually in force. If a rule
changes, this tool reports the new rule automatically.

Usage (from the repository root, with the stores running):

    python3 dev_tools_for_filter_diagnostics/explain_gold.py

Environment variables mirror the pipeline's own (CLICKHOUSE_HOST, POSTGRES_DSN,
MONGO_URI, …); the defaults target the stores on this machine, reached through
the published ports rather than the Docker network.
"""

import csv
import logging
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "2-parsing"))
sys.path.insert(0, str(REPO / "4-processing"))

import parser as gdelt_parser          # 2-parsing
import countries                       # 4-processing
import processor                       # 4-processing

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("explain_gold")

OUT_FILE = Path(__file__).resolve().parent / "gold_provenance.csv"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "radar")
POSTGRES_USER = os.getenv("POSTGRES_USER", "radar")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "radar")
POSTGRES_DSN = os.getenv("POSTGRES_DSN") or (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
MONGO_DB = os.getenv("MONGO_DB", "radar")

FIELDNAMES = [
    "user_id", "doc_id", "url", "article_title",
    "global_event_id", "event_dateadded",
    "parse_f1_event_code", "parse_f2_type_or_group", "parse_f3_alt_keyword",
    "parse_branch",
    "geo_match_system", "geo_match_code", "geo_match_field",
    "keyword_matched", "keyword_tokens", "keyword_match_field",
]


# ══════════════════════════════════════════════════════════════════════════════
# PARSING PROVENANCE (bronze -> silver): F1 AND (F2 OR F3) AND has_source_url
# ══════════════════════════════════════════════════════════════════════════════

def explain_parse(event: dict) -> dict:
    """
    Re-evaluate the bronze->silver criteria on one event row and report which
    values satisfied them. Mirrors parser.passes_filter, which requires
    F1 AND (F2 OR F3) AND has_source_url.
    """
    s = gdelt_parser._safe_str
    out = {"parse_f1_event_code": "", "parse_f2_type_or_group": "",
           "parse_f3_alt_keyword": "", "parse_branch": ""}

    # F1 — the 4-digit EventCode, or the 2-digit EventRootCode as a fallback.
    code, root = s(event.get("EventCode")), s(event.get("EventRootCode"))
    if code in gdelt_parser.RELEVANT_EVENT_CODES:
        out["parse_f1_event_code"] = f"EventCode={code}"
    elif root in gdelt_parser.RELEVANT_ROOT_CODES:
        out["parse_f1_event_code"] = f"EventRootCode={root}"

    # F2 — a relevant actor type or known group.
    for field in ("Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
                  "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code"):
        if s(event.get(field)).upper() in gdelt_parser.RELEVANT_TYPE_CODES:
            out["parse_f2_type_or_group"] = f"{field}={s(event.get(field))}"
            break
    if not out["parse_f2_type_or_group"]:
        for field in ("Actor1KnownGroupCode", "Actor2KnownGroupCode"):
            if s(event.get(field)).upper() in gdelt_parser.RELEVANT_KNOWN_GROUPS:
                out["parse_f2_type_or_group"] = f"{field}={s(event.get(field))}"
                break

    # F3 — a supply-chain word in either actor name or in the source URL.
    combined = " ".join(s(event.get(f)) for f in
                        ("Actor1Name", "Actor2Name", "SOURCEURL")).lower()
    hits = sorted(kw for kw in gdelt_parser.SUPPLY_CHAIN_KEYWORDS if kw in combined)
    if hits:
        out["parse_f3_alt_keyword"] = "|".join(hits)

    # Which arm of the (F2 OR F3) disjunction carried the row.
    branch = []
    if out["parse_f2_type_or_group"]:
        branch.append("F2")
    if out["parse_f3_alt_keyword"]:
        branch.append("F3")
    out["parse_branch"] = "+".join(branch)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# PROCESSING PROVENANCE (silver -> gold): territory AND keyword
# ══════════════════════════════════════════════════════════════════════════════

def explain_geo(event: dict, cameo: set, fips: set) -> dict:
    """
    Report which territory code matched. An event qualifies on EITHER standard:
    CAMEO identifies the actors, FIPS the location, and matching either is enough.
    """
    systems, codes, fields = [], [], []

    for field in ("Actor1CountryCode", "Actor2CountryCode"):
        value = (event.get(field) or "").strip().upper()
        if value and value in cameo:
            systems.append("CAMEO")
            codes.append(value)
            fields.append(field)

    for field in ("ActionGeo_CountryCode", "Actor1Geo_CountryCode", "Actor2Geo_CountryCode"):
        value = (event.get(field) or "").strip().upper()
        if value and value in fips:
            systems.append("FIPS")
            codes.append(value)
            fields.append(field)

    return {
        "geo_match_system": "|".join(dict.fromkeys(systems)),
        "geo_match_code": "|".join(dict.fromkeys(codes)),
        "geo_match_field": "|".join(dict.fromkeys(fields)),
    }


def _row_tokens(mention: dict) -> set:
    """
    The mention's title and extracted keywords, tokenised and stemmed exactly as
    processor._ROW_TOKENS_SQL does it in ClickHouse.
    """
    text = f"{mention.get('article_title') or ''} {mention.get('article_keywords') or ''}"
    raw = [t for t in processor._TOKEN_SPLIT.split(text.lower()) if t]
    return {processor.stem_token(t) for t in raw}


def explain_keyword(mention: dict, keywords: list) -> dict:
    """
    Report the first user keyword that matched and the field it matched in.
    Mirrors processor.build_keyword_clause: the URL is searched with the
    hyphenated variants, the title and article keywords with stemmed tokens, and
    every field is searched for every row.
    """
    if not keywords:
        return {"keyword_matched": "(no keywords set)", "keyword_tokens": "",
                "keyword_match_field": "(unfiltered)"}

    url = (mention.get("MentionIdentifier") or "").lower()
    tokens = _row_tokens(mention)
    title_tokens = {processor.stem_token(t) for t in
                    processor._TOKEN_SPLIT.split((mention.get("article_title") or "").lower()) if t}

    for kw in keywords:
        for variant in sorted(processor.normalize_keyword(kw)):
            if variant and variant in url:
                return {"keyword_matched": kw, "keyword_tokens": variant,
                        "keyword_match_field": "url"}

    for kw in keywords:
        toks = processor.tokenize_keyword_enriched(kw)
        if not toks:
            continue
        stems = [processor.stem_token(t) for t in toks]
        if all(s in tokens for s in stems):
            field = "article_title" if all(s in title_tokens for s in stems) else "article_keywords"
            return {"keyword_matched": kw, "keyword_tokens": " + ".join(stems),
                    "keyword_match_field": field}

    # Reached only if gold predates the current rules — worth seeing, not hiding.
    return {"keyword_matched": "(no current keyword matches)", "keyword_tokens": "",
            "keyword_match_field": "(stale gold row)"}


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    import clickhouse_driver
    import psycopg
    from pymongo import MongoClient

    # ── Read the gold pairs (user_id, doc_id) and the article rows ────────────
    log.info("PostgreSQL  %s", POSTGRES_DSN)
    pg = psycopg.connect(POSTGRES_DSN)
    cur = pg.cursor()
    # NOTE the column names, which do not mean what they appear to mean:
    # `document_identifier` holds the article URL — the same value as silver's
    # MentionIdentifier, and the key to join on — while `mention_identifier`
    # holds the headline, because that is the field the serving tier displays
    # (see 4-processing/gold.py). Joining on the wrong one silently matches only
    # the rows whose headline extraction failed and fell back to the URL.
    cur.execute("""
        SELECT ua.user_id, upper(encode(a.doc_id, 'hex')), a.document_identifier,
               a.global_event_id
        FROM user_articles ua JOIN articles a ON a.doc_id = ua.doc_id
        ORDER BY ua.user_id
    """)
    pairs = cur.fetchall()
    log.info("gold holds %d (user, article) pairs", len(pairs))
    if not pairs:
        log.warning("nothing in gold — run the pipeline first; no CSV written")
        return

    # ── Read the profiles the filters were applied with ──────────────────────
    log.info("MongoDB %s", MONGO_URI)
    mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    profiles = {str(d.get("_id")): d for d in mongo[MONGO_DB]["users"].find({})}
    log.info("loaded %d user profiles", len(profiles))

    # ── Read the silver rows the gold was derived from ───────────────────────
    log.info("ClickHouse %s:%d", CLICKHOUSE_HOST, CLICKHOUSE_PORT)
    ch = clickhouse_driver.Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)

    event_ids = sorted({int(p[3]) for p in pairs if p[3] and str(p[3]).isdigit()})
    urls = sorted({p[2] for p in pairs if p[2]})

    ev_rows, ev_cols = ch.execute(
        "SELECT * FROM gdelt_events FINAL WHERE GLOBALEVENTID IN %(ids)s",
        {"ids": event_ids}, with_column_types=True)
    events = {str(r[0]): dict(zip([c[0] for c in ev_cols], r)) for r in ev_rows}

    mn_rows, mn_cols = ch.execute(
        "SELECT * FROM gdelt_mentions FINAL WHERE MentionIdentifier IN %(urls)s",
        {"urls": urls}, with_column_types=True)
    mentions = {}
    for r in mn_rows:
        d = dict(zip([c[0] for c in mn_cols], r))
        # An enriched row supersedes an unenriched one, as ReplacingMergeTree does.
        key = d["MentionIdentifier"]
        if key not in mentions or d.get("enriched", 0) >= mentions[key].get("enriched", 0):
            mentions[key] = d
    log.info("matched %d events and %d mentions in silver", len(events), len(mentions))

    # ── Re-derive every decision ─────────────────────────────────────────────
    written = orphaned = 0
    with OUT_FILE.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()

        for user_id, doc_id, url, geid in pairs:
            event = events.get(str(geid))
            mention = mentions.get(url)
            if event is None or mention is None:
                # Gold outliving its silver is itself a finding, so it is recorded.
                orphaned += 1
                writer.writerow({
                    "user_id": user_id, "doc_id": doc_id, "url": url,
                    "article_title": "", "global_event_id": geid,
                    "event_dateadded": "", "parse_branch": "(silver row gone)",
                    "keyword_match_field": "(silver row gone)",
                    "parse_f1_event_code": "", "parse_f2_type_or_group": "",
                    "parse_f3_alt_keyword": "", "geo_match_system": "",
                    "geo_match_code": "", "geo_match_field": "",
                    "keyword_matched": "", "keyword_tokens": "",
                })
                written += 1
                continue

            profile = profiles.get(str(user_id), {})
            cameo, fips = countries.codes_for_names(profile.get("territories") or [])
            keywords = []
            for values in (profile.get("keywords") or {}).values():
                keywords.extend(str(v).strip() for v in (values or []) if str(v).strip())

            row = {
                "user_id": user_id,
                "doc_id": doc_id,
                "url": url,
                "article_title": mention.get("article_title", ""),
                "global_event_id": geid,
                "event_dateadded": event.get("DATEADDED", ""),
            }
            row.update(explain_parse(event))
            row.update(explain_geo(event, cameo, fips))
            row.update(explain_keyword(mention, keywords))
            writer.writerow(row)
            written += 1

    log.info("wrote %d rows to %s", written, OUT_FILE)
    if orphaned:
        log.warning("%d gold rows have no matching silver row (recorded as "
                    "'(silver row gone)')", orphaned)

    cur.close()
    pg.close()
    mongo.close()
    ch.disconnect()


if __name__ == "__main__":
    main()
