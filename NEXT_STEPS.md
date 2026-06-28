# NEXT_STEPS — pipeline handoff

Resume point for the processing/serving work. Read this + the code to continue
without prior chat context.

## Architecture
- **1-ingestion** *(team-owned; don't edit)*: poll GDELT every 15 min → transport → parsing.
  Known gaps: container runs the `print()` stub `main.py` (not `src/ingestion/poller.py`);
  `requirements.txt` has a bogus `os~=0.1`; `poller.py` has the `dtype=str` fix applied.
- **2-parsing** *(leave risk logic alone)*: consumes the transport, filters **events**
  (`passes_filter` supply-chain relevance), passes **mentions raw**, writes per-15-min raw
  file pairs to `/data/latest_files`. (Option-A row-reassembly is implemented.)
- **3-validation_and_storage** (sole owner of the store): watches `latest_files`;
  GLOBALEVENTID referential filter on mentions; **enriches** mentions (Newspaper3k, 10-min
  budget → `article_title/article_keywords/enriched`); appends to the ClickHouse cluster;
  writes the global status file `/data/status/pipeline_status.json`.
- **4-processing**: reads `gdelt_events`/`gdelt_mentions` (ClickHouse) + user profiles
  (Mongo `radar.users`) → writes the Oracle gold (`articles`, `user_articles`,
  `pipeline_status`). Endpoints: `POST /process-all`, `POST /process/{user_id}`.
- **5-serving** (separate compose `5-serving/docker-compose.serving.yml`, joins external
  `pipeline_network`): backend (FastAPI) reads Oracle + Mongo; frontend (Streamlit).

## Stores
- **ClickHouse**: 2 shards × 3 replicas + 3-node Keeper ensemble. `gdelt_events`
  (`ReplicatedReplacingMergeTree`, dedup by `DATEADDED`), `gdelt_mentions`
  (`ReplicatedMergeTree`, 19 cols incl. enrichment). Sharded by `cityHash64(GLOBALEVENTID)`.
- **MongoDB**: replica set `rs0` (mongo1/2/3). db `radar`, collection `users` (profiles keyed
  by `_id` = user_id), collection `tags`. **Must run `rs.initiate(...)` once.** Profile today:
  `display_name, countries (NAMES), risk_categories, briefing_days, older_news_days, status`.
- **Oracle** (`pipeline_oracle:1521/GDELT`, user `radar`) — the gold sink, **external**
  (not in main compose). Tables: `articles(document_identifier PK, mention_identifier,
  global_event_id, in_raw_text, confidence, mention_doc_tone, country, risk_category,
  cameo_code, cameo_label, actor, latitude, longitude, event_date, age_days)`,
  `user_articles(user_id, document_identifier)`, `pipeline_status(status, timestamp_of_last_update)`.

## Remaining plan (in order)

> **STATUS — updated 2026-06-28 (all four implemented; HELD FOR REVIEW, nothing committed):**
> - **(a) risk scores removed** across `4-processing` + `5-serving/backend` (frontend was already done).
> - **(b) country tables — done.** Vendored `4-processing/countries.py` (`COUNTRY_CODES` name→{cameo,fips} for 237 curated countries + `codes_for_names`); `5-serving/frontend/configuration/countries.py` regenerated to the same set. Reconciled from the GDELT FIPS+CAMEO lookups (divergent names merged; GDELT data errors corrected: Guinea/Equatorial-Guinea, Slovakia=LO, Congo). **Curated** set chosen (sovereign + key territories; non-countries + Netherlands Antilles dropped). Palestinian territories consolidated into one multi-code entry "Palestine (Gaza Strip & West Bank)" — a `cameo`/`fips` value may therefore be a string or a list, and `codes_for_names` normalises both.
> - **(c) keyword form — done.** `configuration/keywords.py` + `components/keyword_form.py`; profile stores a `keywords` dict (keys sourcing/manufacturing/storage/delivery/companies; add one at a time, cap 100, strip/drop-empty). Risk categories removed from onboarding+dashboard (`sectors.py` left unused); demo-user seeding retired (`cleanup_demo_users` deletes the old demo ids at backend startup).
> - **(d) triggers — done.** In-process daemon threads in `4-processing/triggers.py` (started on FastAPI startup): Mongo `radar.users` change stream → `recompute_user`; ClickHouse `max(DATEADDED)` watermark poll → `recompute_all`.
>
> **Key decisions / deviations to review:**
> - **Per-user filter = Option B (SQL push-down)** in ClickHouse (`clickhouse_writer.query_user_documents` + `_build_geo_clause` + `processor.build_keyword_clause`), chosen over Python-side filtering for scale. `gold.select_document_ids_for_user` removed; `articles` now holds only rows some user references.
> - **geo × keyword = AND** (event must match a country AND the mention a keyword; an empty side imposes no constraint, so a no-prefs user gets everything).
> - **Validation→processing trigger polls ClickHouse `max(DATEADDED)`** instead of a validation-written marker file — keeps processing a pure reader (no `3-validation` edit). `ENABLE_TRIGGERS=0` disables the threads.
> - No `docker-compose.yml` change needed (the `processing` service already has Mongo/ClickHouse/Oracle/STATUS env).
>
> **Still open:** untested against live Oracle/Mongo/ClickHouse; `cameo_label` still `""`; `country` still heuristic; `rs.initiate` one-time; ingestion→parsing transport absent from compose.

### (a) Remove risk scores — validation-onward only (NOT ingestion/parsing)
- **DONE**: `5-serving/frontend` — `heatmap.py` (weight → `event_count`), `event_card.py`, `briefing.py`.
- **TODO 4-processing**: `gold.py` (drop `"risk_score":0` + docstring bullet); `oracle_writer.py`
  (drop `risk_score` from the MERGE — UPDATE SET, INSERT col list, VALUES); `clickhouse_writer.py`
  (dead `silver_events` DDL / `query_silver` / `_event_to_row` — drop risk); `processor.py`
  (drop `apply_country_weight`, `min_risk_score`, the risk lines in `silver_to_gold`).
- **TODO 5-serving/backend**: `mock_gold_layer.py` (4 `risk_score` lines); `oracle_store.py`
  (`a.risk_score,` ×2 in SQL, the `"risk_score"` in `_build_event_card`, the schema-doc line);
  `main.py` (`"risk_score": e["risk_score"]` in `events_summary`).
- The Oracle `articles.risk_score` column becomes vestigial → user can `ALTER TABLE ... DROP COLUMN`.

### (b) Country code tables + countries.py
- Fetch FIPS (`https://www.gdeltproject.org/data/lookups/FIPS.country.txt`) and CAMEO
  (`https://www.gdeltproject.org/data/lookups/CAMEO.country.txt`). Build `name → (CAMEO|None, FIPS|None)`
  for the **union** of countries in either list.
- `5-serving/frontend/configuration/countries.py` = full country names (the union).
- Processing: map a user's selected country **names → CAMEO + FIPS codes**, then filter events where
  `Actor1/2CountryCode ∈ CAMEO` **OR** `ActionGeo_CountryCode / Actor1/2Geo_CountryCode ∈ FIPS`
  (match if **either** hits).

### (c) 5-question keyword form (replaces risk_categories)
- Frontend: **comment out** `RISK_CATEGORY_OPTIONS` usage (`onboarding.py`, `dashboard.py`) and the
  category-based demo-user seeding (`mongo_store.ensure_demo_users` / `DEMO_USERS`). If demo users were
  already created in Mongo, delete them. Everything must still work (users get **all** events regardless
  of category).
- Add 5 questions: "What are you sourcing?", "What are you shipping for manufacturing?", "What are you
  shipping for storage?", "What are you shipping for delivery?", "Please list the names of all companies
  involved." Each = user adds **one field at a time**, cap **100** fields/question. On submit: strip
  leading/trailing spaces; drop empty fields.
- Store the keywords on the Mongo profile (per question). Processing feeds them into
  `processor.build_keyword_clause` (already built: `normalize_keyword` + the URL/title/keywords
  conditional) for the per-user article filter.

### (d) Wire the triggers
- **Mongo change stream** on `radar.users` (operationType insert/update/replace) → call processing
  `/process/{user_id}` for the changed user (replica set enables change streams).
- **Validation watermark**: when validation appends (silver changes), bump a marker (e.g. in
  `/data/status`) → processing `/process-all`. (ClickHouse has no change stream.)

## Loose ends
- Per-user filter is a **placeholder** (`gold.select_document_ids_for_user` returns ALL) until (b)+(c).
- `cameo_label` = human-readable **CAMEO event-code** label (needs `CAMEO.eventcodes.txt`); currently `""`.
- `country` derived heuristically from `ActionGeo_FullName`; (b) gives proper codes.
- Oracle writes **untested** against a live DB.
- Mongo `rs.initiate` one-time step; ingestion→parsing **transport is absent from compose** (Kafka was removed).
- Full run = `docker compose up` (main, creates `pipeline_network`) **then**
  `docker compose -f 5-serving/docker-compose.serving.yml up`.
  
# Delete this document

Once ALL of the above is confirmed done, remind the user to delete this NEXT_STEPS document.
