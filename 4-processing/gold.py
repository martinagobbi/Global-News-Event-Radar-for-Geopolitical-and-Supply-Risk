"""
4-processing/gold.py
--------------------

Map ClickHouse silver (gdelt_events + gdelt_mentions) into the serving's Oracle
`articles` schema, and select each user's article set for `user_articles`.

One "article" row = one mention (article URL) denormalised with its event's
fields. Several derivations are still WORK IN PROGRESS and are left as
placeholders here (see the loose ends):
    * country        — derived best-effort from ActionGeo_FullName
    * risk_category  — left "" (needs CAMEO-code -> 24-category classification)
    * cameo_label    — left "" (needs a CAMEO code -> label table)
    * risk_score     — 0 (the team dropped risk scores)
    * per-user filter (select_document_ids_for_user) — returns ALL for now
"""

from datetime import date


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _s(v) -> str:
    return "" if v is None else str(v)


def _event_date(day):
    """YYYYMMDD string -> datetime.date, or None if unparseable."""
    d = _s(day).strip()
    if len(d) == 8 and d.isdigit():
        try:
            return date(int(d[:4]), int(d[4:6]), int(d[6:8]))
        except ValueError:
            return None
    return None


def _country_name(ev) -> str:
    """Best-effort country NAME from ActionGeo_FullName ("Beijing, …, China" -> China)."""
    full = _s(ev.get("ActionGeo_FullName")).strip()
    if full:
        return full.split(",")[-1].strip()
    return _s(ev.get("ActionGeo_CountryCode")).strip()


def _article_row(m: dict, ev: dict) -> dict:
    ed = _event_date(ev.get("Day"))
    age = (date.today() - ed).days if ed else None
    # mention_identifier is used by serving as the article headline -> use the
    # enriched article_title, falling back to the URL when enrichment is empty.
    title = _s(m.get("article_title")).strip() or _s(m.get("MentionIdentifier"))
    return {
        "document_identifier": _s(m.get("MentionIdentifier")),   # the URL (PK)
        "mention_identifier":  title,                            # headline
        "global_event_id":     _s(m.get("GLOBALEVENTID")),
        "in_raw_text":         _i(m.get("InRawText")) or 0,
        "confidence":          _i(m.get("Confidence")) or 0,
        "mention_doc_tone":    _f(m.get("MentionDocTone")),
        "country":             _country_name(ev),
        "risk_category":       "",     # TODO: classify event -> one of the 24 categories
        "goldstein":           _f(ev.get("GoldsteinScale")),
        "risk_score":          0,      # unused (team dropped risk scores)
        "cameo_code":          _s(ev.get("EventCode")),
        "cameo_label":         "",     # TODO: CAMEO code -> label
        "actor":               _s(ev.get("Actor1Name")),
        "latitude":            _f(ev.get("ActionGeo_Lat")),
        "longitude":           _f(ev.get("ActionGeo_Long")),
        "event_date":          ed,
        "age_days":            age,
    }


def build_article_rows(events_df, mentions_df) -> list[dict]:
    """
    Join each mention to its event (by GLOBALEVENTID) and produce one Oracle
    `articles` row per mention. Mentions whose event isn't present are skipped.
    """
    if events_df is None or events_df.empty or mentions_df is None or mentions_df.empty:
        return []

    events_by_id = {str(r.get("GLOBALEVENTID")): r for r in events_df.to_dict("records")}

    rows: list[dict] = []
    for m in mentions_df.to_dict("records"):
        ev = events_by_id.get(str(m.get("GLOBALEVENTID")))
        if ev is None:
            continue
        if not _s(m.get("MentionIdentifier")).strip():
            continue  # no URL -> no primary key
        rows.append(_article_row(m, ev))
    return rows


def select_document_ids_for_user(profile: dict, article_rows: list[dict]) -> list[str]:
    """
    Which articles a given user receives (-> user_articles).

    PLACEHOLDER: returns every article. The real per-user filter is still WIP and
    needs (a) country names -> CAMEO/FIPS codes matched against the event codes,
    (b) risk_category classification matched against profile['risk_categories'],
    and (c) any future keyword matching. Wire those into this one function.
    """
    return [r["document_identifier"] for r in article_rows]
