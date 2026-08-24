-- ═══════════════════════════════════════════════════════════════════════════
-- Gold schema for the serving layer.
--
-- Mounted into the PostgreSQL container at /docker-entrypoint-initdb.d/, so the
-- image runs this ONCE, at first database creation (i.e. when the postgres_data
-- volume is empty). Every later restart skips it — which is what we want: this is
-- a one-time setup of a long-lived store, never something the (disposable)
-- pipeline re-runs. In intended mode the same file is applied by Patroni's
-- bootstrap.post_init hook, because Patroni initialises the cluster itself and
-- never invokes the image's entrypoint hook.
--
-- The `radar` role and the `radar` database are created by the image from
-- POSTGRES_USER / POSTGRES_DB, and this script runs as that role against that
-- database, so the tables land in its default `public` schema and every query in
-- the project can address them unqualified.
--
-- KEY DESIGN — why doc_id BYTEA (a hash) instead of the URL:
--   document_identifier is the article URL. Using a VARCHAR(2000) URL as the
--   PRIMARY KEY can exceed PostgreSQL's btree limit — an index entry must fit in
--   roughly 2704 bytes (a third of an 8K page), and 2000 characters of UTF-8 can
--   reach 8000 — so a long URL would be rejected at INSERT time with
--   "index row size ... exceeds btree version 4 maximum". The key is therefore
--   doc_id = SHA-256(document_identifier): a fixed 32 bytes regardless of URL
--   length. The full URL is kept as ordinary data alongside it.
--   4-processing/postgres_writer.py computes the same hash when writing, and
--   5-serving/backend/postgres_store.py joins on doc_id.
--
-- KEY DESIGN — why TIMESTAMP and not DATE:
--   Oracle's DATE carries a time-of-day; PostgreSQL's DATE does not, it stores a
--   calendar day only. mention_time is the article's own MentionTimeDate from
--   silver and drives the card ordering, so storing it as DATE would silently
--   truncate every article to midnight and collapse the ordering within a day.
--   TIMESTAMP is the faithful equivalent of what Oracle held here.
--
-- NOTE: deliberately no FK from user_articles.doc_id -> articles.doc_id, because
-- processing writes a user's rows before it upserts the de-duplicated article
-- catalogue; a FK would reject those inserts.
-- ═══════════════════════════════════════════════════════════════════════════

-- KEY DESIGN — why the key is (doc_id, global_event_id) and not doc_id alone:
--   One article URL is routinely a mention of MANY GDELT events. Measured on the
--   shipped 30-day seed: 27,132 of 52,359 distinct URLs (51.8%) appear under more
--   than one GLOBALEVENTID, and one URL appears under 64 of them. Titles behave
--   the same way — 53.8% span several events.
--
--   Keying on doc_id alone would therefore keep ONE of those pairs and silently
--   discard the rest, so an article that legitimately belongs to six different
--   event cards would appear on only one, chosen arbitrarily. The grain of this
--   table is a MENTION — an (article, event) pair — which is exactly the grain of
--   silver's gdelt_mentions, whose ReplacingMergeTree sorts on
--   (GLOBALEVENTID, MentionIdentifier) for the same reason.
--
--   De-duplication anywhere in the pipeline must therefore compare the PAIR:
--   two rows may collapse only if they share a GLOBALEVENTID as well as a URL.
-- ── Types deliberately mirror the silver schema, and are never STRICTER ──────
-- Gold is derived from silver, so a value silver accepted must not be rejected
-- here: that would turn a stored row into a failed publish, and the whole
-- recompute with it. Two earlier mismatches did exactly that in principle:
--
--   SMALLINT vs Int32   SMALLINT is 16-bit (-32768..32767); silver's counters are
--                       Nullable(Int32). `age_days` is the live risk — it is
--                       datediff(today, event_date), so an event dated far in the
--                       past or a corrupt `Day` overflows it. Verified: inserting
--                       99999 raises `ERROR: smallint out of range`. Now INTEGER.
--   VARCHAR(n) vs String  every silver text column is an unbounded String, so any
--                       length cap here is a stricter rule than the source. In
--                       particular VARCHAR(50) on global_event_id constrained an
--                       identifier this pipeline promises never to interpret.
--                       Now TEXT, which in PostgreSQL costs nothing extra:
--                       VARCHAR(n) and TEXT share one storage representation.
--
-- NOT NULL is confined to the three IDENTITY columns, matching silver exactly —
-- there, GLOBALEVENTID and MentionIdentifier are the only non-Nullable columns,
-- and validation DROPS any row missing either. Everything else is nullable
-- because "not provided" is now a value the pipeline carries end to end.
CREATE TABLE articles (
  doc_id              BYTEA            NOT NULL,     -- SHA-256 of document_identifier
  document_identifier TEXT             NOT NULL,     -- the article URL (data, not a key)
  mention_identifier  TEXT,                          -- headline (enriched title, or URL)
  global_event_id     TEXT             NOT NULL,
  in_raw_text         INTEGER,
  confidence          INTEGER,
  mention_doc_tone    DOUBLE PRECISION,
  country             TEXT,
  risk_category       TEXT,
  goldstein           DOUBLE PRECISION,
  cameo_code          TEXT,
  cameo_label         TEXT,
  -- The publisher the article came from (GDELT's MentionSourceName, e.g.
  -- "bbc.co.uk"). Shown on the card next to each link. Replaced the former
  -- `actor` column (Actor1Name), which no serving query ever selected.
  mention_source_name TEXT,
  latitude            DOUBLE PRECISION,
  longitude           DOUBLE PRECISION,
  -- The article's own timestamp (silver's MentionTimeDate). event_date below is
  -- per-EVENT, so it is identical for every article on a card; this is what lets
  -- the serving layer order cards by the oldest article each one carries.
  mention_time        TIMESTAMP,
  date_added          TIMESTAMP,
  event_date          TIMESTAMP,
  age_days            INTEGER,
  CONSTRAINT pk_articles PRIMARY KEY (doc_id, global_event_id)
);

-- The composite key leads with doc_id, so it cannot serve lookups by event
-- alone — and the retention job, the orphan sweep and the triage pages all
-- filter on global_event_id on its own.
CREATE INDEX ix_articles_event ON articles (global_event_id);

-- Carries global_event_id for the same reason `articles` does: a user receives an
-- (article, event) PAIR, and one article can reach them through several events.
-- Keying on (user_id, doc_id) would silently keep only one of them.
CREATE TABLE user_articles (
  user_id         TEXT  NOT NULL,
  doc_id          BYTEA NOT NULL,
  global_event_id TEXT  NOT NULL,
  CONSTRAINT pk_user_articles PRIMARY KEY (user_id, doc_id, global_event_id)
);

-- silver_watermark is max(DATEADDED) in ClickHouse gdelt_events as of the last
-- publish: the slice id the gold on display was built from. It is carried through
-- PostgreSQL rather than read live because the serving tier has no ClickHouse
-- client — gold is the only store it talks to for events. Kept as the raw 14-char
-- GDELT slice id (YYYYMMDDHHMMSS) so it compares lexicographically, exactly as the
-- processing layer's trigger compares it.
CREATE TABLE pipeline_status (
  status                   VARCHAR(10),
  timestamp_of_last_update TIMESTAMP,
  silver_watermark         VARCHAR(14)
);
