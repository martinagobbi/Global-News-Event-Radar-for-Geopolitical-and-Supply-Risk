-- ═══════════════════════════════════════════════════════════════════════════
-- Gold schema for the serving layer.
--
-- Mounted into the Oracle container at /container-entrypoint-initdb.d/, so the
-- gvenzl image runs this ONCE, as SYSDBA, at first database creation (i.e. when
-- the oracle_data volume is empty). Every later restart skips it — which is what
-- we want: this is a one-time setup of a long-lived store, never something the
-- (disposable) pipeline re-runs.
--
-- The `radar` user itself is created by the image from APP_USER/APP_USER_PASSWORD.
-- We only add its tables here. `ALTER SESSION SET CONTAINER` makes the target PDB
-- explicit so this works regardless of which container the hook starts in.
--
-- KEY DESIGN — why doc_id RAW(32) instead of the URL:
--   document_identifier is the article URL. Using a VARCHAR2(2000) URL as the
--   PRIMARY KEY can blow Oracle's maximum index key length (~6398 bytes on an
--   8K block; 2000 chars of AL32UTF8 can reach 8000 bytes) -> ORA-01450.
--   So the key is doc_id = SHA-256(document_identifier): a fixed 32 raw bytes,
--   regardless of URL length. The full URL is kept as ordinary data alongside it.
--   4-processing/oracle_writer.py computes the same hash when writing, and
--   5-serving/backend/oracle_store.py joins on doc_id.
--
-- NOTE: deliberately no FK from user_articles.doc_id -> articles.doc_id, because
-- processing writes a user's rows before it upserts the de-duplicated article
-- catalogue; a FK would reject those inserts.
-- ═══════════════════════════════════════════════════════════════════════════

ALTER SESSION SET CONTAINER = FREEPDB1;

CREATE TABLE radar.articles (
  doc_id              RAW(32)        PRIMARY KEY,   -- SHA-256 of document_identifier
  document_identifier VARCHAR2(2000) NOT NULL,      -- the article URL (data, not a key)
  mention_identifier  VARCHAR2(2000),               -- headline (enriched title, or URL)
  global_event_id     VARCHAR2(50),
  in_raw_text         NUMBER(1),
  confidence          NUMBER(3),
  mention_doc_tone    FLOAT,
  country             VARCHAR2(200),
  risk_category       VARCHAR2(500),
  goldstein           FLOAT,
  cameo_code          VARCHAR2(10),
  cameo_label         VARCHAR2(200),
  actor               VARCHAR2(500),
  latitude            FLOAT,
  longitude           FLOAT,
  -- The article's own timestamp (silver's MentionTimeDate). event_date below is
  -- per-EVENT, so it is identical for every article on a card; this is what lets
  -- the serving layer order cards by the oldest article each one carries.
  mention_time        DATE,
  event_date          DATE,
  age_days            NUMBER(4)
);

CREATE TABLE radar.user_articles (
  user_id VARCHAR2(200) NOT NULL,
  doc_id  RAW(32)       NOT NULL,
  CONSTRAINT pk_user_articles PRIMARY KEY (user_id, doc_id)
);

CREATE TABLE radar.pipeline_status (
  status                   VARCHAR2(10),
  timestamp_of_last_update TIMESTAMP
);
