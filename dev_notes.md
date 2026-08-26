---
output:
  pdf_document: default
  html_document: default
editor_options: 
  markdown: 
    wrap: 72
---


# Global News Event Radar — Geopolitical & Supply Risk

To start everything at once:
```
docker compose --env-file .env.testing -f docker-compose.stores.yml up -d && docker compose --env-file .env.testing up -d --build && ./bootstrap/silver_snapshot.sh restore && python3 5-serving/seed_test_users.py && docker compose -f 5-serving/docker-compose.serving.yml up --build
```

A five-layer pipeline that polls GDELT every 15 minutes, retains
supply-chain-relevant events, enriches and stores them, and serves each
user a personalised briefing filtered by the **territories** and
**supply-chain keywords** they registered. Python version: 3.11.

```         
1-ingestion → 2-parsing → 3-validation_and_storage → 4-processing → 5-serving (backend + frontend)
```

- **1-ingestion** — polls GDELT's `lastupdate.txt` feed and writes the
  raw events and mentions CSVs to the shared volume.
- **2-parsing** — applies the supply-chain relevance filter to events,
  passes mentions through unchanged, and publishes each slice to
  `latest_files`.
- **3-validation_and_storage** — enforces referential integrity,
  performs Newspaper3k enrichment, and owns the silver store
  (ClickHouse), including its schema and deduplication.
- **4-processing** — filters silver per user by territory codes and
  keywords, writes the PostgreSQL gold (`articles`, `user_articles`,
  `pipeline_status`), and publishes the territory table to MongoDB.
- **5-serving** — a **backend** (FastAPI, reads PostgreSQL and MongoDB)
  and a **frontend** (Streamlit dashboard).

All specific numbers in terms of memory, rows stored, etc. in this
README refer to numbers computed with roughly 30 days of data, and three
test users.

### Stores

- **ClickHouse** — silver: `gdelt_events` / `gdelt_mentions`, 2 shards ×
  3 replicas plus a 3-node Keeper ensemble.
- **MongoDB** — replica set `rs0`: user profiles (`radar.users`),
  per-user tags (`radar.tags`) and the territory table
  (`radar.reference`).
- **PostgreSQL** — the gold sink the serving backend reads. One node in
  testing mode; three under Patroni in intended mode.

------------------------------------------------------------------------

## System requirements

The Docker memory allocation must be raised in **both** modes — the
default is too small for either. Set it in **Docker Desktop → Settings →
Resources → Memory limit**.

| Mode | Store containers | Measured usage | Set the limit to |
|----|----|----|----|
| **Testing** | 4 | **≈ 3.4 GB** with the stores, processing and backend running | **at least 6 GB** |
| **Intended**, one machine per store node | 3–4 per store machine | ≈ 1.5 GB per store machine; ≈ 2.5 GB on the pipeline machine | **4 GB per store machine, 6 GB for the pipeline machine** |

**Spark is why testing mode needs more than it used to.** The processing
container is built on the Spark image and runs the silver → gold job
in-process (`SPARK_MASTER=local[*]`), so it holds a JVM: **1.77 GB on
its own**, against roughly 200 MB when that job was plain pandas. The
image grew from about 0.6 GB to **2.69 GB** for the same reason. The
rest of the stack is unchanged and small — ClickHouse ≈ 1.2 GB, MongoDB
≈ 0.1 GB, PostgreSQL ≈ 0.02 GB.

That is the price of having ONE implementation of silver → gold instead
of two. The alternative was keeping a separate pandas path for testing
mode, which is exactly the duplication that let the two versions drift
apart in the first place.

### Memory allowance, layer by layer {#memory-allowance-layer-by-layer}

Every container in testing mode, ingestion through serving frontend.
"Allowance" is the ceiling the component is *permitted*; "measured" is
steady-state usage on the 30-day seed plus live slices, taken 2026-08-16
on a 6.77 GiB Docker VM.

| Layer | Container | Allowance | Measured | Where the allowance is set |
|----|----|----|----|----|
| 1 — ingestion | `pipeline_ingestion` | uncapped | 51 MiB | — |
| 2 — parsing | `pipeline_parsing` | uncapped | 43 MiB | — |
| 3 — validation | `pipeline_validation` | uncapped | 86 MiB | — |
| 4 — processing | `pipeline_processing` | uncapped (Spark driver defaults to a 1 GB heap) | **1.66 GiB** | `spark.driver.memory`, unset |
| silver store | `pipeline_clickhouse_s1r1` | **2.5 GB** | 1.52 GiB | `clickhouse/memory.local.xml` |
| coordination | `pipeline_ch_keeper_1` | uncapped | 107 MiB | — |
| profiles/tags | `pipeline_mongo1` | **2.88 GB** WiredTiger cache | 145 MiB | MongoDB default: ½(RAM) − 1 GB |
| gold store | `pipeline_postgres` | uncapped (`shared_buffers` 128 MB default) | 34 MiB | — |
| 5 — backend | `radar-backend` | uncapped | 52 MiB | — |
| 5 — frontend | `radar-frontend` | uncapped | 131 MiB | — |

Two things this table makes visible that are worth stating outright.

**The allowances oversubscribe the VM; the measurements do not.**
ClickHouse (2.5 GB) plus MongoDB's cache (2.88 GB) plus the Spark driver
(\~1.7 GB) is over 7 GB on a 6.77 GiB VM. Nothing fails today only
because MongoDB never approaches its cache ceiling — it holds three user
profiles and some tags, 145 MiB against 2.88 GB permitted. It is a
latent overcommit, not a safe margin, and it is the first thing to look
at if the JVM is killed again.

**Only ClickHouse is explicitly capped.** Everything else takes a
default. That is defensible for the pipeline layers, which are small and
flat, but it means the two largest consumers — the Spark JVM and
MongoDB's cache — are bounded by their own defaults rather than by
anything this project decided.

**In intended mode the shape is different.** Each store machine runs one
ClickHouse node (`clickhouse/memory.distributed.xml`, **4 GB**, sized
for an 8 GB machine) plus at most one Keeper and one MongoDB member or
PostgreSQL node. Spark becomes a real cluster — `spark-master` plus
`SPARK_WORKERS` (default 2) workers — so executors no longer share a
container with the processing layer, and each worker is bounded by
`SPARK_WORKER_MEMORY` (default **2G**). Only the *driver* JVM stays
resident in the processing container.

> **The Spark services are NOT pinned, and this is worth knowing before
> a real deployment.** `ingestion`, `parsing`, `validation` and
> `processing` all carry
> `constraints: ["node.labels.role == pipeline"]`. `spark-master`,
> `spark-worker` and the three `backend` replicas carry none,
> deliberately — they hold no durable state, so Swarm may schedule them
> anywhere. "Anywhere" includes **store1–6**.
>
> A 2 GB Spark worker landing on a store machine sits beside a
> ClickHouse node already permitted 4 GB on an 8 GB box. That is the
> same squeeze that killed the driver JVM in testing mode, just
> relocated — and there it would be ClickHouse competing rather than the
> pipeline. Either give the Spark services a placement constraint of
> their own, label a machine for them, or size the store machines with
> the workers in mind. The machine table below counts the *store* nodes;
> it does not reserve capacity for roaming Spark workers.

**On ClickHouse's 2.5 GB.** This was briefly 3.5 GB, raised when
Oracle's \~1.1 GB was freed by the move to PostgreSQL and because the
widest per-user keyword query peaked right at 2.5 GB and failed with
`Code: 241`. Both reasons have since expired: unifying silver → gold on
Spark moved that predicate out of SQL entirely, so ClickHouse now only
serves partitioned column reads — 735 queries over three live hours
peaked at **74.54 MiB** — and the freed gigabyte is no longer free,
because the Spark driver now runs resident in the same VM. See
`clickhouse/memory.local.xml`.

Below these thresholds the ClickHouse nodes are terminated under memory
pressure and restart in a loop, which appears as queries timing out or
returning nothing rather than as an obvious error.

Testing mode exists precisely because the thirteen-container topology
does not fit comfortably on a machine with 8 GB of physical RAM: the six
ClickHouse servers alone occupy roughly 4.6 GB.

------------------------------------------------------------------------

## The whole system, one box per node

Two drawings of the same pipeline. Nothing in the application code
differs between them — only how many nodes each store has, and how many
machines they sit on.

### Testing mode — 10 containers, 1 machine

```         
┌─ YOUR MACHINE — docker compose --env-file .env.testing ──────────────────────────────┐
│                                                                                      │
│  ┌──────────────┐  1-INGESTION — polls GDELT every 15 min                            │
│  │ 1-ingestion  │  reads lastupdate.txt, downloads two zips:                         │
│  │              │    20260727171500.export.CSV.zip    ~979 events                    │
│  │              │    20260727171500.mentions.CSV.zip  ~3,222 mentions                │
│  └──────┬───────┘  -> /data/raw on the `shared_data` volume                          │
│         │                                                                            │
│  ┌──────▼───────┐  2-PARSING — the bronze filter                                     │
│  │ 2-parsing    │  keeps only supply-chain-relevant events:                          │
│  │              │    EventCode 190 kept · actor type MNC kept                        │
│  │              │    ~979 events -> ~30 survive (roughly 97% dropped)                │
│  └──────┬───────┘                                                                    │
│         │                                                                            │
│  ┌──────▼───────┐  3-VALIDATION — enrichment, and it OWNS the silver schema          │
│  │ 3-validation │  Newspaper3k fetches each article URL:                             │
│  │  _and_storage│    title    "Novo Nordisk sues Eli Lilly"                          │
│  │              │    keywords [obesity, drug, lawsuit]                               │
│  └──────┬───────┘  creates the tables ON CLUSTER, then writes SILVER                 │
│         │  clickhouse-driver, port 9000                                              │
│  ┌──────▼──────────────────────────┐   ┌───────────────────────────────┐             │
│  │ clickhouse-s1r1                 │   │ clickhouse-keeper-1           │             │
│  │ SILVER · columnar OLAP          │◄─►│ Raft coordination for         │             │
│  │                                 │   │ replication + ON CLUSTER DDL. │             │
│  │ gdelt_events                    │   │ Speaks the ZooKeeper protocol.│             │
│  │   GLOBALEVENTID 1315499039      │   │ Alone here, so it forms a     │             │
│  │   EventCode 190 · Day 20260727  │   │ quorum of one and elects      │             │
│  │   ActionGeo_CountryCode IE      │   │ itself leader.                │             │
│  │ gdelt_mentions                  │   └───────────────────────────────┘             │
│  │   MentionIdentifier https://... │                                                 │
│  │   MentionTimeDate 20260727113000│   1 shard x 1 replica, so the                   │
│  │                                 │   Distributed table routes every                │
│  │ 103,972 events / 111,430 mentions│  row to this single node.                      │
│  └──────┬──────────────────────────┘                                                 │
│         │  polled every 60s: has max(DATEADDED) advanced?                            │
│  ┌──────▼───────┐  4-PROCESSING — silver -> gold, per user                           │
│  │ 4-processing │  PySpark (SPARK_MASTER=local[*]) — the ONLY silver->gold           │
│  │              │  path. Reads 13 of 61 event columns; joins, de-dupes per           │
│  │              │  EVENT, filters per user, stages, publishes. See below.            │
│  └──┬────────┬──┘  Plain Python here: 2 triggers, retention, territory seed          │
│     │        │                                                                       │
│  ┌──▼─────────────────┐   ┌──▼──────────────────────────────────────────┐            │
│  │ mongo1  (rs0)      │   │ pipeline_postgres                           │            │
│  │ PROFILES & TAGS    │   │ GOLD · row-store OLTP                       │            │
│  │                    │   │                                             │            │
│  │ radar.users        │   │ articles                                    │            │
│  │   territories      │   │   doc_id  = SHA-256(url) -> fixed 32 bytes   │           │
│  │   keywords         │   │   mention_identifier "Novo Nordisk sues..."  │           │
│  │ radar.tags         │   │   mention_time 2026-07-27 11:30:00           │           │
│  │   archived/needs   │   │ user_articles                               │            │
│  │   action/monitoring│   │   (radar_pharma, doc_id)                    │            │
│  │ radar.reference    │   │ pipeline_status                             │            │
│  │                    │   │   (OK, 2026-08-15 14:02:33)                 │            │
│  │ A replica set of   │   │                                             │            │
│  │ ONE, not a stand-  │   │ 164 articles / 164 links from the seed      │            │
│  │ alone mongod: the  │   │ agrifood 127 · electronics 21 · pharma 16   │            │
│  │ change stream is   │   └──────────────────┬──────────────────────────┘            │
│  │ what triggers a    │                      │                                       │
│  │ rebuild when a     │   ┌──────────────────▼──────────────────────────┐            │
│  │ profile is edited. │──►│ radar-backend · FastAPI :8000               │            │
│  └────────────────────┘   │ GET /users/radar_pharma/events -> 16 cards, │            │
│                           │ newest story first. Reads only; never writes.│           │
│                           └──────────────────┬──────────────────────────┘            │
│                                              │ HTTP                                  │
│                           ┌──────────────────▼──────────────────────────┐            │
│                           │ radar-frontend · Streamlit :8501            │            │
│                           │ BACKEND_URL=http://host.docker.internal:8000│            │
│                           └─────────────────────────────────────────────┘            │
│                                                                                      │
│  NO REDUNDANCY: one ClickHouse, one Keeper, one Mongo, one PostgreSQL.               │
│  Losing this machine loses everything GDELT cannot be re-polled for.                 │
│                                                                                      │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

### Intended mode — 19 store tasks across 6 machines, plus the pipeline machine

```         
┌─ DOCKER SWARM · overlay network `pipeline_network` ──────────────────────────────────────┐
│ Service names resolve on EVERY machine, which is why clickhouse/cluster.xml,             │
│ the ON CLUSTER DDL and every store address are byte-identical to testing mode.           │
│ No store port is published anywhere. Managers = store1+2+3 (Raft, tolerates 1).          │
└──────────────────────────────────────────────────────────────────────────────────────────┘

  ── MACHINES store1–3 ─────────────────────────────────────────────────────────
  Hosting: ClickHouse shard 1 · Keeper quorum · etcd quorum · PostgreSQL trio ·
           the three Swarm managers

┌─ store1 ───────────────────┐ ┌─ store2 ───────────────────┐ ┌─ store3 ───────────────────┐
│ clickhouse-s1r1            │ │ clickhouse-s1r2            │ │ clickhouse-s1r3            │
│   SILVER shard 1           │ │   SILVER shard 1           │ │   SILVER shard 1           │
│   51,914 rows              │ │   51,914 rows              │ │   51,914 rows              │
│   ReplicatedReplacing-     │ │   identical copy           │ │   identical copy           │
│   MergeTree                │ │                            │ │                            │
│   4 GB cap: it has the     │ │                            │ │                            │
│   machine to itself        │ │                            │ │                            │
│                            │ │                            │ │                            │
│ clickhouse-keeper-1        │ │ clickhouse-keeper-2        │ │ clickhouse-keeper-3        │
│   Raft: replication +      │ │   quorum 2 of 3            │ │                            │
│   ON CLUSTER DDL           │ │                            │ │                            │
│                            │ │                            │ │                            │
│ etcd-1                     │ │ etcd-2                     │ │ etcd-3                     │
│   holds PATRONI's          │ │   quorum 2 of 3            │ │                            │
│   LEADER LOCK (a TTL)      │ │                            │ │                            │
│                            │ │                            │ │                            │
│ postgres-1  (Spilo =       │ │ postgres-2                 │ │ postgres-3                 │
│   PostgreSQL+Patroni)      │ │                            │ │                            │
│   GOLD · LEADER            │ │   GOLD · replica           │ │   GOLD · replica           │
│   accepts ALL writes       │ │ ◄──────── WAL streaming    │ │ ◄──────── WAL streaming    │
│   streams WAL ────────►    │ │   0 MB lag                 │ │   0 MB lag                 │
│                            │ │   read-only until it is    │ │   read-only until it is    │
│ PATRONI on each node:      │ │   promoted                 │ │   promoted                 │
│   renews the etcd lock;    │ │                            │ │                            │
│   if it expires, a         │ │                            │ │                            │
│   replica promotes         │ │                            │ │                            │
│   itself, re-points the    │ │                            │ │                            │
│   other replica, and       │ │                            │ │                            │
│   pg_rewind rebuilds       │ │                            │ │                            │
│   the old leader as a      │ │                            │ │                            │
│   replica — never two      │ │                            │ │                            │
│   writers at once.         │ │                            │ │                            │
│                            │ │                            │ │                            │
│ SWARM MANAGER              │ │ SWARM MANAGER              │ │ SWARM MANAGER              │
└────────────────────────────┘ └────────────────────────────┘ └────────────────────────────┘

  ── MACHINES store4–6 ─────────────────────────────────────────────────────────
  Hosting: ClickHouse shard 2 · the MongoDB replica set

  NOTE: "shard 2" describes ClickHouse ONLY. MongoDB is not sharded and has no
  shards; all three of its members hold the SAME data. It sits on these machines
  because they were the ones left over after store1–3 took the four quorums —
  co-location, not a relationship. See "What a shard is, and what it is not".

┌─ store4 ───────────────────┐ ┌─ store5 ───────────────────┐ ┌─ store6 ───────────────────┐
│ clickhouse-s2r1            │ │ clickhouse-s2r2            │ │ clickhouse-s2r3            │
│   SILVER shard 2           │ │   SILVER shard 2           │ │   SILVER shard 2           │
│   52,058 rows              │ │   52,058 rows              │ │   52,058 rows              │
│                            │ │                            │ │                            │
│ mongo1   rs0 PRIMARY       │ │ mongo2   secondary         │ │ mongo3   secondary         │
│   radar.users              │ │   majority 2 of 3          │ │                            │
│     territories+keywords   │ │   automatic election       │ │                            │
│   radar.tags               │ │   w="majority" writes      │ │                            │
│     archived / needs       │ │                            │ │                            │
│     action / monitoring    │ │                            │ │                            │
│   radar.reference          │ │                            │ │                            │
│                            │ │                            │ │                            │
│   CHANGE STREAM ───────►   │ │                            │ │                            │
│   a profile edit fires     │ │                            │ │                            │
│   a rebuild immediately,   │ │                            │ │                            │
│   with no polling          │ │                            │ │                            │
└────────────────────────────┘ └────────────────────────────┘ └────────────────────────────┘

     51,914 + 52,058 = 103,972.  Which shard a row lands on is decided by
     cityHash64(GLOBALEVENTID), so an event AND ALL ITS MENTIONS land together —
     joins stay on one machine, and re-ingested copies collapse where they sit.

┌─ pipeline1 — may ROAM; its volume is disposable scratch ─────────────────────────────────┐
│ 1-ingestion -> 2-parsing -> 3-validation -> 4-processing -> radar-backend :8000          │
│                                                                                          │
│ SPARK CLUSTER (this tier, but UNPINNED — Swarm places it on any machine):                │
│   spark-master  :8080 UI, :7077 submit                                                   │
│   spark-worker  x SPARK_WORKERS, each SPARK_WORKER_CORES / _MEMORY                       │
│   4-processing is the DRIVER: it submits to spark-master rather than                     │
│   running the job itself, so the read/join/write spread across workers.                  │
│   Scale live:  docker service scale radar_spark-worker=N                                 │
│                                                                                          │
│ Every store is addressed as a LIST, so no client is a single point of failure:           │
│   CLICKHOUSE_ALT_HOSTS = clickhouse-s1r2:9000,clickhouse-s1r3:9000                       │
│   MONGO_URI            = mongo1,mongo2,mongo3/?replicaSet=rs0                            │
│   POSTGRES_DSN         = postgres-1,postgres-2,postgres-3                                │
│                          ?target_session_attrs=read-write  <- finds the leader           │
│                                                                                          │
│ If THIS machine dies, Swarm re-creates the whole tier on another machine                 │
│ labelled role=pipeline; it re-polls the current GDELT slice and continues.               │
└──────────────────────────────────────────────────────────────────────────────────────────┘
                    │ :8000 through Swarm's routing mesh — ANY node's port works
   ┌────────────────┼────────────────┐
┌──▼───────────┐ ┌──▼───────────┐ ┌──▼───────────┐
│ user machine │ │ user machine │ │ user machine │
│ radar-       │ │ radar-       │ │ radar-       │
│ frontend:8501│ │ frontend:8501│ │ frontend:8501│
└──────────────┘ └──────────────┘ └──────────────┘
```

### The silver → gold job, step by step

The two drawings above show WHERE things run. This one shows WHAT the
PySpark job actually does, because that single job is now the whole of
silver → gold in both modes — there is no second implementation.

```         
┌─ SILVER -> GOLD, the PySpark job (4-processing/spark_gold.py) ─────────────────────────┐
│                                                                                        │
│ Driven by the resident processing service, NOT by spark-submit. Runs whenever the      │
│ watermark advances, or one user edits their preferences (only_user=...).               │
│ SPARK_MASTER decides WHERE: local[*] in testing, the worker cluster in intended.       │
│                                                                                        │
├─ 1  READ — partitioned JDBC, column-pruned ────────────────────────────────────────────┤
│   Asks ClickHouse for min/max GLOBALEVENTID, splits that range into                    │
│   SPARK_READ_PARTITIONS (8) slices, and issues one concurrent query per slice          │
│   per table. Spark pushes the projection down: ClickHouse's system.query_log           │
│   shows it receives 13 named columns of gdelt_events' 61, not SELECT *.                │
│   FINAL collapses re-ingested duplicates at read time.                                 │
│     -> events   13 of 61 columns   (19% of the table's bytes)                          │
│     -> mentions  9 of 19 columns   (84% — the text columns ARE the payload)            │
├─ 2  JOIN — events x mentions, a distributed shuffle ───────────────────────────────────┤
│   Inner join on GLOBALEVENTID. Rows sharing an id are moved so they meet in one        │
│   partition; that movement is the shuffle, and SPARK_SHUFFLE_PARTITIONS sets how       │
│   many partitions it produces (16 testing / 64 intended; Spark's default of 200        │
│   put ~500 rows in each and cost ~3x the runtime in scheduling alone).                 │
│   Derives the gold columns here: doc_id = SHA-256(url) via a UDF, the headline         │
│   (enriched title, else the URL), country from ActionGeo_FullName, event_date,         │
│   age_days (all of gold has this value updated at every 15-minute advance in the       │
│   watermark, provided that the pipeline is operational), and mention_time from         │
│  MentionTimeDate                                                                       │
├─ 3  DE-DUPLICATE — twice, and BOTH scoped to one event ────────────────────────────────┤
│   (doc_id, global_event_id)      the real grain: one row per (article, event)          │
│   (global_event_id, _title_key)  syndication — one headline per CARD                   │
│                                                                                        │
│   Neither collapses a URL ACROSS events. Measured on the seed, 51.8% of URLs           │
│   and 53.8% of titles appear under more than one GLOBALEVENTID (one URL under          │
│   64 of them), so keying on the URL alone silently discarded most pairs.               │
├─ 4  FILTER — each user's predicate, against the cached catalogue ──────────────────────┤
│   The catalogue is cached once, then each profile's predicate runs across the          │
│   cluster:   geo(CAMEO actor codes OR FIPS geo codes)  AND  keyword(URL variant        │
│   OR all tokens present in the stemmed title+keywords).  A user receives the           │
│   (article, event) PAIR, so one article reaches them once per matching event.          │
│   only_user=<uid> restricts this to a single profile — the change-stream path.         │
├─ 5  STAGE — distributed JDBC write into two scratch tables ────────────────────────────┤
│   articles_stage and user_articles_stage are DROPped and recreated at the start        │
│   of every run, so their column types are always the declared ones. write_gold()       │
│   opens one connection per partition (SPARK_WRITE_PARTITIONS), so the executors        │
│   write in parallel; mode=overwrite with truncate=true empties rather than drops.      │
│   Each stage table carries a SQL COMMENT saying it is transient and safe to drop.      │
├─ 6  PUBLISH — stage -> live, in ONE transaction ───────────────────────────────────────┤
│   articles        INSERT ... SELECT ... ON CONFLICT (doc_id, global_event_id)          │
│                   DO UPDATE  — upsert, never deletes                                   │
│   user_articles   DELETE this run's users, then INSERT from the stage                  │
│   orphan sweep    delete articles no user_articles row references, PROTECTING          │
│                   events tagged requires_action / monitor (NOT archive)                │
│   pipeline_status replaced with one row mirroring the validation status file           │
├─ 7  DROP THE STAGE TABLES — only on success ───────────────────────────────────────────┤
│   Left in place deliberately if publish() raised, so a failed run can be               │
│   inspected; the next run drops and recreates them anyway.                             │
├─ WHAT DOES NOT GO THROUGH SPARK ───────────────────────────────────────────────────────┤
│   The watermark poll, the MongoDB change stream, the daily retention sweep and         │
│   the status mirror stay plain Python: small transactional statements against          │
│   ClickHouse, PostgreSQL and MongoDB with nothing to distribute.                       │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

**What each system does, in one line each.** *ClickHouse* holds silver
and is scanned analytically, so it is columnar and sharded. *ClickHouse
Keeper* is the Raft service that lets the six ClickHouse nodes agree on
replication and `ON CLUSTER` DDL — it speaks the ZooKeeper protocol but
is a ClickHouse component, not ZooKeeper itself. *MongoDB* holds user
profiles, triage tags and the territory reference, and its **change
stream** is what lets a preference edit trigger a rebuild without
polling. *PostgreSQL* holds gold and is read by point lookup, so it is a
row store with B-tree keys. *Patroni* is the supervisor that elects one
PostgreSQL leader and fails over automatically; *etcd* is the quorum
store holding the lock Patroni elects with; *Spilo* is simply the image
that ships PostgreSQL and Patroni together. *Docker Swarm* pins each
node to its machine, distributes the config files, and provides the
overlay network that makes every service name resolve from everywhere.

------------------------------------------------------------------------

## Deployment

The system is divided into three independently deployable tiers, each
with its own lifecycle:

| Tier | Contents | Lifecycle and location |
|----|----|----|
| **Stores** | ClickHouse and Keeper, MongoDB `rs0`, PostgreSQL (`docker-compose.stores.yml`) | Started once and left running. Owns every durable volume and the `pipeline_network`. |
| **Pipeline** | Layers 1–4 and the serving **backend** (`docker-compose.yml`) | Disposable and replaceable. Owns no database. |
| **User frontend** | The serving **frontend** only (`5-serving/docker-compose.serving.yml`) | One instance per user machine. |

### Two modes, selected by command-line arguments only

The system runs in one of two modes. **No file is ever edited to switch
between them**: the mode is chosen entirely by the arguments passed to
`docker compose`.

|   | **Testing** (one machine) | **Intended** (distributed) |
|----|----|----|
| ClickHouse | 1 server, 1 Keeper | 6 servers (2 shards × 3 replicas), 3 Keepers |
| MongoDB | 1-node replica set | 3-node replica set |
| Gold store | 1 PostgreSQL node | 3 PostgreSQL nodes under Patroni (+3 etcd) |
| Store containers | **4** | **18**, spread over 6 machines |
| Memory, all running | **≈ 2.1 GB** on one machine | **≈ 1.5 GB** per store machine |
| Insert quorum | 1 (no redundancy) | 2 of 3 replicas |
| Pipeline location | same machine as the stores | its own machine, reschedulable by Swarm |
| Frontend | same machine | one instance per user machine |

The modes are selected by which **files** are deployed, not by editing
anything: testing mode uses `docker compose` with
`docker-compose.stores.yml` and `.env.testing`; intended mode uses
`docker stack deploy` with `docker-stack.stores.yml`,
`docker-stack.pipeline.yml` and `.env.intended`. The env file selects
the ClickHouse cluster and Keeper topology, the per-node memory cap, the
insert quorum, the MongoDB member count and the store address lists.

The application code, the cluster name, the table names and the service
names are identical in both modes. See [Every difference between testing
mode and intended
mode](#every-difference-between-testing-mode-and-intended-mode) for the
complete accounting.

### A. Testing — everything on one machine

``` bash
git clone https://github.com/martinagobbi/Global-News-Event-Radar-for-Geopolitical-and-Supply-Risk
cd Global-News-Event-Radar-for-Geopolitical-and-Supply-Risk

# 1. the stores (they create pipeline_network)
docker compose --env-file .env.testing -f docker-compose.stores.yml up -d

# 2. the pipeline — the validation layer creates the silver schema on first connect
docker compose --env-file .env.testing up -d --build

# 3. the 30-day history (needs the schema from step 2; waits for it if necessary)
./bootstrap/silver_snapshot.sh restore

# 4. the three test profiles — without these the gold layer stays empty
python3 5-serving/seed_test_users.py          # needs `requests` on the host. Also, may have to type `python` instead of `python3`.

# 5. the gold layer, pre-computed (optional, but it saves ~2 minutes of waiting)
./bootstrap/gold_snapshot.sh restore

# 6. the dashboard
docker compose -f 5-serving/docker-compose.serving.yml up --build
```

#### The gold seed, and why it is safe

Step 5 is the gold counterpart of step 3. Silver restores in \~7 seconds
from a committed snapshot; gold would otherwise take **\~2 minutes** to
compute from it, because the Spark job runs once per user profile.
`gold_snapshot.sh restore` loads a committed dump of `articles` and
`user_articles` in **under a second**.

It does not weaken the rule that gold must never drift from what the
pipeline would produce — but the reason is narrower than it looks, and
worth stating precisely.

**On a first run** the snapshot is only a head start: restoring *silver*
advances the watermark, which fires a recompute, which replaces all of
it a couple of minutes later. The pipeline wins, automatically.

**That argument holds only while gold is empty.** Restoring over a gold
layer the pipeline has already built is destructive, and does not
self-correct: `restore` truncates both tables, and the watermark fires
on new **silver**, which restoring gold does not touch. So nothing
recomputes until the next GDELT slice arrives — or never, if the
pipeline is stopped. Anything derived from live data would be gone in
the meantime, and any account other than the three seeded ones would
show an empty dashboard, because the dump contains no rows for them.

`restore` therefore **refuses when gold is non-empty**, and points at
the command that rebuilds gold from the silver you actually have:

```         
REFUSING: gold already holds 5,412 article rows.
  To rebuild gold from the silver you actually have (usually what you want):
    docker exec pipeline_processing python3 -c 'import main; main.recompute_all()'
  To overwrite anyway:  FORCE=1 ./bootstrap/gold_snapshot.sh restore
```

What the seed buys is a dashboard with real cards immediately on a fresh
clone, rather than an empty one that fills in over two minutes.

**One column has to be repaired on restore, and it is worth knowing
why.** Every other value in gold is absolute — timestamps, ids, text —
and keeps its meaning indefinitely. `age_days` does not: it is
`today − event_date` as computed when the row was written, and the
dashboard filters on it (`age_days <= briefing_days`). A dump restored a
month later would make every article a month too young. So the restore
recomputes it from `event_date`, which is absolute. Verified after a
restore: all 607 rows satisfy `age_days = CURRENT_DATE − event_date`.

**Silver's `restore` has no such guard, and needs none.** The two are
asymmetric for a reason worth stating. Silver holds append-only facts
keyed on `GLOBALEVENTID`, so re-inserting the seed over live data is
well defined: `restore` only `INSERT`s, never truncates, and
`ReplacingMergeTree` collapses the repeats by key. Verified against a
store holding 25 live events beyond the seed — before and after the
restore it read 103,997 rows, 25 of them live, newest timestamp
unchanged, and exactly 103,972 in the seed window rather than double.
Silver's destructive operations are separately named and say so: `wipe`,
`trim`, `recreate`.

Gold cannot work that way, because it is derived per-user state rather
than facts. A user's article set is *rebuilt*, not merged — unioning an
old set with a new one gives an answer that matches neither — so the
restore must replace wholesale, which is exactly why it needs a guard
that silver does not.

**Re-export it when the filter logic changes** —
`./bootstrap/gold_snapshot.sh export`. Gold is derived, so the committed
dump is only valid for the code that produced it. Change the parsing
filters, the per-user predicate, the deduplication rules or the gold
schema, and the seed is stale until re-exported. Stale here means
"briefly wrong, then corrected by the recompute", not "wrong forever".

#### How long this takes

Measured end to end from a **fresh clone with empty volumes**, following
the steps above in order:

| Step | What it waits for | First run | Every later run |
|----|----|----|----|
| 1 | ClickHouse and PostgreSQL accepting connections | 4 s | 4 s |
| 2 | `--build` of **five** images, pipeline started, schema created | **7–10 min** | 15 s |
| 3 | 30-day silver seed restored | 7 s | 7 s |
| 4 | three profiles created through the backend | 1 s | 1 s |
| 5 | gold seed restored | **\< 1 s** | \< 1 s |
| 6 | frontend image **built**, dashboard answering on :8501 | **≈ 160 s** | 8 s |
|  | **total to a working dashboard** | **10–13 min** | **≈ 35 s** |

**Step 2 is the whole story on a first run, and it has two separate
costs.** The pipeline builds **five** images, and they are not alike:

| Image | Built by | Base | First-build cost |
|----|----|----|----|
| processing | step 2 | `bitnamilegacy/spark:3.5` | **≈ 429 s** — almost entirely pulling the \~1.5 GB base |
| validation | step 2 | `python:3.11-slim` | 126 s `pip install` + 15 s NLTK data |
| ingestion | step 2 | `python:3.11-slim` | 102 s `pip install` |
| parsing | step 2 | `python:3.11-slim` | 91 s `pip install` |
| backend | step 2 | `python:3.11-slim` | 85 s `pip install` |
| **frontend** | **step 6** | `python:3.11-slim` | **128 s `pip install` + 28 s export** |

The frontend is easy to overlook because it is built by a **different
compose file** (`5-serving/docker-compose.serving.yml`) — it is not part
of step 2's `--build`, and it runs on the user's machine rather than the
operator's.

BuildKit builds them concurrently, so the four Python images together
take about **152 s of wall clock**, not the sum of their parts. Add the
Spark pull, which overlaps only partly, and a true first build lands
somewhere between **7 and 10 minutes** depending on connection speed.
Every later build is seconds, because every one of those layers is
cached and only `COPY . .` re-runs.

Two of those figures were measured separately — 429 s with the four
Python images already cached, and 152 s with processing already cached —
so the combined range is a bound rather than a single stopwatch reading.

**Reproducing a genuine first run means purging all five images plus the
base**, not just the one. Removing `radar-processing` alone leaves the
other four fully cached and makes step 2 look like seconds:

``` bash
docker compose -f 5-serving/docker-compose.serving.yml down --rmi local   # frontend
docker compose --env-file .env.testing down --rmi local                   # the four python images
docker compose --env-file .env.testing -f docker-compose.stores.yml down
docker rmi -f radar-processing:latest bitnamilegacy/spark:3.5             # custom tag + base
docker builder prune -af                                                   # the layer cache
```

`--rmi local` covers the images Compose names after the project;
`radar-processing` carries an explicit tag, so it needs removing by
name.

**Without step 5** — if you skip the gold seed — the dashboard starts
empty and fills in over roughly **two minutes**. Creating three profiles
fires the MongoDB change-stream trigger once per profile, so the Spark
job runs three times rather than once; the first user's cards appear
after about 40 seconds and the rest follow. Nothing is wrong while that
happens.

**The order matters.** The silver tables are owned and created by the
validation layer, so the seed cannot be restored before step 2 has run —
there would be nothing to insert into. `restore` waits up to five
minutes for the schema to appear and tells you what to start if it does
not. Step 4 is equally load-bearing: the gold layer is built **per
user**, so until at least one profile exists there is nothing for the
processing layer to build and the dashboard stays empty.

Step 4 is the only step that runs on the host rather than in a
container, and its only dependency is `requests`. It waits up to two
minutes for the backend to answer before giving up, so it can safely be
run the moment step 2 returns.

**There is no virtual environment to activate, and none is needed.**
Every layer runs in its own container with its own `requirements.txt`,
so the project has no host-side Python environment at all — this one
script is the sole exception. If the host has no suitable Python, run it
in a throwaway container instead, which needs nothing installed and
reaches the backend by service name:

``` bash
docker run --rm --network pipeline_network -v "$PWD/5-serving:/seed:ro" \
  -e BACKEND_URL=http://radar-backend:8000 python:3.11-slim \
  sh -c "pip install -q requests && python /seed/seed_test_users.py"
```

Either way it prints one line per account and is safe to repeat — the
profiles are upserted, so re-running restores them to their seeded
state.

The `restore` step loads `data/silver_seed/*.parquet` — 15 MiB committed
to this repository, holding a fully filtered and enriched 30-day history
— straight into the ClickHouse volume. It takes about **three seconds**.
Gold follows on its own: the processing layer's watermark trigger
notices silver has grown and builds `articles` and `user_articles`
within a minute, so the dashboard has real content almost immediately
rather than after the days the 15-minute pipeline would need.

Restoring is safe to repeat: both silver tables are
`ReplacingMergeTree`, so re-inserting the same rows collapses back to
the same counts.

> **If gold stays empty after a few minutes, force one rebuild.** The
> trigger watches `max(DATEADDED)` in silver and fires only when that
> value *increases*. On a fresh clone it always does, because the
> trigger starts with no previous value and the first poll reads
> `None -> 20260727171500`. But on a cluster that has already processed
> **newer** data — for instance one that ran the live pipeline before
> restoring the seed — inserting the seed's older rows leaves
> `max(DATEADDED)` untouched, so the watermark does not advance and the
> trigger correctly declines to fire. Gold then stays as it was. One
> command rebuilds it:
>
> ``` bash
> docker exec pipeline_processing python3 -c "import main; main.recompute_all()"
> ```
>
> This is the same work the trigger performs, run on demand. It is safe
> to repeat: `user_articles` is rebuilt per user from scratch, and
> `articles` is upserted and then swept of rows no user still references
> — see [Orphaned gold
> rows](#orphaned-gold-rows-and-why-they-can-be-removed-safely).

This starts four store containers rather than thirteen, which is what
makes the stack usable on a machine with 8 GB of RAM.

> **`--env-file .env.testing` is required on every command that talks to
> the stores.** Without it they fall back to
> `CLICKHOUSE_INSERT_QUORUM=2`, which a single-replica cluster can never
> satisfy, and every insert fails with ClickHouse error 285. The
> intended-mode equivalent is sourcing `.env.intended` into the shell,
> because `docker stack deploy` takes no `--env-file` at all.

The dashboard is then available at
[**http://localhost:8501**](http://localhost:8501){.uri}.

#### Signing in

The dashboard shows a sign-in screen first and nothing else until you
are authenticated — there is no anonymous view, and account creation is
deliberately not implemented. The three fixed accounts come from step 4
above (`seed_test_users.py`); **without that step the credentials below
still sign in, but the account has no profile, so it lands on the setup
page and the gold layer stays empty** — the gold is built per user, and
there is no user to build it for.

| Username | Password | Supply chain | Territories |
|----|----|----|----|
| `radar_electronics` | `chips2026` | Semiconductors and electronics | Asia-Pacific |
| `radar_pharma` | `vials2026` | Pharmaceuticals and biologics | Europe |
| `radar_agrifood` | `grain2026` | Agri-food commodities | Americas and Africa |

The three profiles are mutually exclusive: no territory and no keyword
appears in more than one account. Inactive sessions are signed out after
15 minutes.

These are test credentials, published deliberately. The sign-in gates
the dashboard UI only — the serving backend has no authentication of its
own, so anyone who can reach it directly can query any user's data. It
is not production authentication.

#### What each account will show

Measured on the committed seed, immediately after the steps above:

| Account | Articles | Event cards (unique `GLOBALEVENTID`) |
|----|----|----|
| `radar_agrifood` | **127** | **124** |
| `radar_electronics` | **21** | **21** |
| `radar_pharma` | **16** | **16** |
| *total distinct articles in gold* | *164* |  |

An event card groups the articles reporting the same GDELT event, so
cards are never more numerous than articles. Only `radar_agrifood` shows
the difference here — 127 articles across 124 cards, meaning three of
its events were reported by two outlets each. For the other two accounts
every article is a distinct event.

The three counts differ by roughly eight-fold because the profiles
genuinely differ in reach, not because anything is wrong: agri-food
keywords (*grain*, *wheat*, *port*, *tariff*) are ordinary news
vocabulary, whereas the electronics and pharmaceutical lists are
procurement-register terms (*photoresist*, *epoxy molding compound*,
*bromobutyl stoppers*) that rarely reach a general-news headline. A
single 15-minute slice frequently matches nothing for a given user; that
is the filter working as intended.

**All of it is historical.** A fresh clone starts with the committed
30-day history and nothing else: GDELT slices from **2026-06-27 17:15
UTC to 2026-07-27 17:15 UTC**, 2,881 slices in total. Live polling
begins the moment the pipeline starts, so newer articles accumulate from
then on at roughly one slice every 15 minutes — but everything present
at first sign-in comes from that 30-day window.

> > **The first `--build` takes a while.** The processing image is built
> > on the Spark base and pulls the ClickHouse and PostgreSQL JDBC
> > drivers, so the first build is \~2.7 GB and can take several minutes
> > on a cold cache. Later builds reuse the layers and only re-copy the
> > source.

**The `--build` flag is required.** Compose reuses an existing image and
does not rebuild because a source file changed. Any modification to
Python code, or to `.streamlit/config.toml`, reaches a container only
when `--build` is passed. Files that are *mounted* rather than copied
into the image — the compose files, `clickhouse/*.xml`,
`postgres-init/*.sql` — are read at container start and require no
rebuild.

The stores are ready in **seconds**. This used to take several minutes,
because Oracle built its database files from scratch before running the
gold schema; PostgreSQL initialises its data directory almost instantly.
Measured on this machine: `pg_isready` answered and all three gold
tables existed within 20 seconds of `up -d`.

### B. Intended — one node of each store per machine

Testing mode puts every store container on one machine, so its
redundancy is real only against *container* loss: that machine dying
takes every ClickHouse replica, every MongoDB member and the gold layer
with it. Intended mode places **one node of each storage system on a
different machine**, so no two nodes of the same store share a failure
domain. Different stores may share a machine — what must never share one
is two nodes of the same store.

The stores run as a **Docker Swarm stack** on an overlay network. That
is the single decision the rest follows from: overlay DNS resolves
service names across machines, so `clickhouse/cluster.xml`, the
`ON CLUSTER gnews_cluster` DDL and every store address in
`docker-compose.yml` keep working **unchanged**, and no store port has
to be published to the host at all. There is no `STORES_HOST` to
substitute.

#### Choosing and preparing the machines

# Cluster Architecture & Service Placement

The production topology consists of seven dedicated infrastructure machines for storage, consensus, and ingestion processing, plus optional dedicated machines for dashboard user clients.

## Machine Topology

| Machine | ClickHouse | Keeper | MongoDB | PostgreSQL+Patroni | etcd | Docker Swarm Role | Assigned Workloads |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **store1** | s1r1 | keeper-1 | — | leader | etcd-1 | **manager** | Stores Tier (Shard 1, Replica 1) |
| **store2** | s1r2 | keeper-2 | — | replica | etcd-2 | **manager** | Stores Tier (Shard 1, Replica 2) |
| **store3** | s1r3 | keeper-3 | — | replica | etcd-3 | **manager** | Stores Tier (Shard 1, Replica 3) |
| **store4** | s2r1 | — | mongo1 | — | — | worker | Stores Tier (Shard 2, Replica 1) |
| **store5** | s2r2 | — | mongo2 | — | — | worker | Stores Tier (Shard 2, Replica 2) |
| **store6** | s2r3 | — | mongo3 | — | — | worker | Stores Tier (Shard 2, Replica 3) |
| **pipeline1** | — | — | — | — | — | worker (`role=pipeline`) | Ingestion through Serving Backend, `spark-master`, `spark-worker` |

> **Note on Swarm Roles:** The **Swarm role** column (`manager` vs. `worker`) refers strictly to Docker Swarm cluster orchestration privileges. It is completely independent of Apache Spark worker services.

---

## Spark Co-location & Resource Safety

`spark-master` and `spark-worker` hold no durable file system state, but they are **not** permitted to roam freely across the Swarm cluster. 

### 1. Hard Placement Isolation
Because `store1`–`store6` host RAM-sensitive consensus engines (etcd, Keeper) and primary databases (ClickHouse, PostgreSQL, MongoDB), allowing Spark workers to execute on store nodes creates an severe risk of Out-Of-Memory (OOM) terminations on database processes.

Both Spark services carry explicit placement constraints in `docker-stack.pipeline.yml`:

```yaml
placement:
  constraints: ["node.labels.role == pipeline"]
```

#### What a shard is, and what it is not

**"Shard" is a ClickHouse word only.** It means the events table is
split in two by `cityHash64(GLOBALEVENTID)`: shard 1 holds roughly half
the rows, shard 2 the other half, and neither holds the whole table. A
query against the Distributed table asks both and merges the answers.

Nothing else in this system is sharded:

| System | How it is spread | Does every node hold all the data? |
|----|----|----|
| **ClickHouse** | **sharded** (2 shards) *and* replicated (×3) | No — each shard holds \~half |
| **MongoDB** | replica set `rs0`, 3 members | **Yes** — all three identical |
| **PostgreSQL** | Patroni: 1 leader + 2 streaming replicas | **Yes** — all three identical |
| **Keeper / etcd** | Raft quorums of 3 | **Yes** — consensus, not partitioning |

So a machine group is *not* a shard. store4–6 host ClickHouse shard 2
**and** the MongoDB replica set, but those two facts are unrelated:
MongoDB is not "in" shard 2, is not sharded, and would not care if
ClickHouse's sharding changed tomorrow. They share machines because
there are six store machines and the placement rule is simply *never put
two nodes of the same system on one machine*.

Read the layout as "which machine runs which containers", never as
"which shard owns which systems".

Shard 1 lives on store1–3 and shard 2 on store4–6, so a shard survives
losing two **machines**. store1–3 carry four separate quorums between
them — Keeper, etcd, the Swarm managers and Patroni's PostgreSQL trio —
each of which tolerates losing one of its three. **Three Swarm managers,
not one**: Swarm coordinates through Raft exactly as Keeper does, so a
single manager would be a single point of failure for orchestration, and
nothing could be rescheduled while it was down.

Each machine needs:

- **Docker Engine** (or Docker Desktop) with Compose v2, and **≥ 4 GB**
  available to Docker. Measured per store machine: ≈ 1.5 GB.

- **The repository checked out on the three managers.** Workers need
  nothing: the ClickHouse XMLs and the gold schema travel as Swarm
  **configs**, distributed by the manager. This is why they are configs
  and not bind mounts — a bind mount would have to exist on whichever
  machine the task happened to land on.

- **`nofile` raised to 262144** on every ClickHouse machine. Swarm
  **ignores** the `ulimits:` key, so it must be set on the daemon
  instead, in `/etc/docker/daemon.json`, followed by a daemon restart:

  ``` json
  { "default-ulimits": { "nofile": { "Name": "nofile", "Soft": 262144, "Hard": 262144 } } }
  ```

- **Clock sync (NTP).** Keeper's Raft, etcd's Raft, Patroni's leader
  lease and MongoDB's elections all depend on it.

- **Ports open between the machines**: `2377/tcp` (manager),
  `7946/tcp+udp` (node discovery) and `4789/udp` (VXLAN). Only the
  pipeline machine publishes anything to the outside world — `8000` for
  the dashboard.

- **Outbound internet on the pipeline machine**, which polls GDELT every
  15 minutes.

The machines must reach each other by IP; `localhost` will not do. Find
a machine's address with `ipconfig getifaddr en0` (macOS) or
`hostname -I` (Linux).

#### Everything that names a machine, in one place

The overlay network is what makes this list short. Because service names
resolve swarm-wide, **no store address is configured anywhere** — not
ClickHouse, not the MongoDB members, not PostgreSQL. The old
`STORES_HOST` placeholder is gone. What remains is only the things
Docker itself needs to know:

| What | Where you set it | Value |
|----|----|----|
| Swarm manager address | `docker swarm init --advertise-addr <ip>` on store1 | that machine's real IP |
| Joining the swarm | `docker swarm join --token <token> <manager-ip>:2377` on every other machine | printed by `swarm init` |
| Extra managers | `docker node promote <store2> <store3>` | hostnames |
| Which machine is which store node | `docker node update --label-add store=storeN <hostname>` | `store1`…`store6` |
| Which machine runs the pipeline | `docker node update --label-add role=pipeline <hostname>` | one hostname |
| Image registry | `REGISTRY` in the shell before `docker stack deploy` | your registry |
| Dashboard address | `BACKEND_URL` on each user machine | `http://<any-swarm-node>:8000` |
| ClickHouse file handles | `/etc/docker/daemon.json` on each ClickHouse machine | `nofile` 262144 |
| Spark cluster size | `SPARK_WORKERS` / `SPARK_WORKER_CORES` / `SPARK_WORKER_MEMORY` in `.env.intended` | to taste |

Find a machine's address with `ipconfig getifaddr en0` (macOS) or
`hostname -I` (Linux). `localhost` and `127.0.0.1` will not work for
`--advertise-addr`, because the other machines have to be able to reach
it.

**Nothing in `.env.intended` is a machine address.** It selects topology
(`CH_CLUSTER_CONFIG`, `MONGO_MEMBERS`), tuning (`SPARK_*`,
`CLICKHOUSE_INSERT_QUORUM`) and the store lists that are already service
names. It needs no editing before use.

#### Steps

**Step 1 — form the swarm.** On store1:

``` bash
docker swarm init --advertise-addr <store1-ip>
docker network create -d overlay --attachable pipeline_network
```

`docker swarm init` prints a join command. Run it on every other
machine, then promote store2 and store3 so there are three managers:

``` bash
docker node promote <store2-hostname> <store3-hostname>
```

**Step 2 — label the machines**, which is what the placement constraints
match:

``` bash
docker node update --label-add store=store1 <store1-hostname>   # …through store6
docker node update --label-add role=pipeline <pipeline1-hostname>
```

Every store service is **pinned** to its machine. This is mandatory, not
tidiness: a ClickHouse node owns a local volume *and* a replica identity
registered in Keeper, so rescheduled elsewhere it would start with an
empty volume under an identity Keeper already knows — a broken replica,
not a fresh one. The pipeline tier may roam precisely because its volume
is disposable scratch.

**Step 3 — build and push the images** (Swarm cannot build from a
Dockerfile), on a manager:

``` bash
export REGISTRY=<your-dockerhub-user>          # or a private registry
for l in ingestion parsing validation processing; do
  docker build -t $REGISTRY/radar-$l:latest ./$(ls -d [1-4]-* | grep $l)
  docker push  $REGISTRY/radar-$l:latest
done
docker build -t $REGISTRY/radar-backend:latest ./5-serving/backend
docker push  $REGISTRY/radar-backend:latest
```

**Step 4 — deploy both stacks** from a manager. `docker stack deploy`
accepts **no `--env-file`**, so the environment has to be sourced into
the shell first — this is the single most common way an intended-mode
deployment goes wrong, because the defaults it silently falls back to
are the testing-mode ones:

``` bash
set -a; . ./.env.intended; set +a
docker stack deploy -c docker-stack.stores.yml   radar-stores
docker stack deploy -c docker-stack.pipeline.yml radar
docker stack ps radar-stores            # watch until every task says Running
```

**Step 5 — load the seed**, on the machine hosting `clickhouse-s1r1`
(store1):

``` bash
./bootstrap/silver_snapshot.sh restore
```

`docker exec` is local to one daemon, so this must run where s1r1
actually is. The script finds the Swarm task by service label, since
Swarm ignores `container_name`. It must follow Step 4: the silver tables
are created by the validation layer, which runs on the pipeline machine.

> On a six-node cluster the row count this prints can read **low** — it
> is taken the instant the insert returns, while the second shard and
> the replicas are still being filled. Measured here: it printed 51,914
> immediately and 103,972 a few seconds later. Nothing is lost; re-run
> `SELECT count() FROM gdelt_events FINAL` to confirm.

**Step 6 — create the three test profiles**, once, from any machine that
can reach the backend. The gold layer is built per user, so until a
profile exists there is nothing to build:

``` bash
BACKEND_URL=http://<any-swarm-node>:8000 python3 5-serving/seed_test_users.py
```

**Step 7 — each user machine** (any number):

``` bash
BACKEND_URL=http://<any-swarm-node>:8000 \
  docker compose -f 5-serving/docker-compose.serving.yml up --build
```

Any node's `:8000` works, because Swarm's routing mesh forwards it to a
live backend replica wherever that happens to be running.

#### Confirming it is actually distributed

These are the checks that distinguish a real seven-machine deployment
from six containers on one host:

``` bash
docker node ls                              # 7 nodes, 3 of them Leader/Reachable
docker stack ps radar-stores | grep -c Running

# ClickHouse: 6 nodes across 2 shards, and rows genuinely split between them
docker exec <s1r1-task> clickhouse-client --query \
  "SELECT count(), uniqExact(shard_num) FROM system.clusters WHERE cluster='gnews_cluster'"
docker exec <s1r1-task> clickhouse-client --query \
  "SELECT hostName(), count() FROM clusterAllReplicas('gnews_cluster', default.gdelt_events_local) GROUP BY hostName() ORDER BY 1"

# MongoDB: three members, on three different hosts
docker exec <mongo1-task> mongosh --quiet --eval 'rs.status().members.map(m => m.name)'

# PostgreSQL: one leader, two streaming replicas
docker exec <postgres-1-task> patronictl -c /home/postgres/postgres.yml list
```

The ClickHouse query should report `6  2`, and the per-node counts
should show two distinct totals appearing three times each — one per
shard, replicated three ways. Anything else means the cluster is not
laid out as intended.

#### Trying intended mode on one machine, and why it refuses

It is worth running this once, because it demonstrates the point of the
mode better than any description: intended mode does not merely *prefer*
seven machines, it declines to run on one.

Point every "machine" at your own by forming a single-node swarm and
deploying the real stack unchanged:

``` bash
docker swarm init --advertise-addr 127.0.0.1
docker network create -d overlay --attachable pipeline_network

NODE=$(docker node ls -q)
docker node update --label-add store=store1  $NODE
docker node update --label-add role=pipeline $NODE

set -a; . ./.env.intended; set +a
docker stack deploy -c docker-stack.stores.yml radar-stores
sleep 45
docker stack ps radar-stores --format '{{.Name}}\t{{.CurrentState}}\t{{.Error}}'
```

**The first wall is scheduling, and it is immediate.** A Docker node
holds one value per label key, so a single node cannot be `store1` *and*
`store2`. Only the four services pinned to `store1` start; the other
fifteen never do:

```         
clickhouse-keeper-1.1   Running
clickhouse-s1r1.1       Running
etcd-1.1                Running
postgres-1.1            Running
clickhouse-s1r2.1       Pending   "no suitable node (scheduling constraints not satisfied on 1 node)"
clickhouse-s2r1.1       Pending   "no suitable node (scheduling constraints not satisfied on 1 node)"
mongo1.1                Pending   "no suitable node (scheduling constraints not satisfied on 1 node)"
…
Running: 4 / 19        Pending: 15 / 19
```

That is the placement constraints doing exactly their job. They are not
a formality: a ClickHouse node owns a local volume *and* a replica
identity registered in Keeper, so Swarm refusing to put two replicas of
one shard on one machine is the mechanism that makes "a shard survives
losing two machines" true.

**The second wall is memory, and it is the reason the first one is not
simply relaxed.** Suppose you delete the `placement:` blocks so
everything schedules. Intended mode sets
`CH_MEMORY_CONFIG=memory.distributed.xml`, which gives each ClickHouse
server **4 GB**, because in intended mode each has a machine to itself.
Confirm it on the running node:

``` bash
docker exec $(docker ps -qf name=radar-stores_clickhouse-s1r1) \
  clickhouse-client --query \
  "SELECT value FROM system.server_settings WHERE name='max_server_memory_usage'"
# 4000000000
```

Six of those is 24 GB before anything else starts:

| Component              | Each      | Total        |
|------------------------|-----------|--------------|
| 6 × ClickHouse         | 4.00 GB   | **24.00 GB** |
| 3 × PostgreSQL (Spilo) | \~0.25 GB | 0.75 GB      |
| 3 × MongoDB            | \~0.20 GB | 0.60 GB      |
| 3 × ClickHouse Keeper  | \~0.15 GB | 0.45 GB      |
| 3 × etcd               | \~0.05 GB | 0.15 GB      |
|                        |           | **≈ 26 GB**  |

Docker on the machine this was written on has **6.77 GiB**. The stores
alone ask for close to four times that, before the pipeline tier and the
dashboard. Below the limit ClickHouse nodes are terminated by the kernel
(exit code 137) and restart in a loop, which surfaces as queries timing
out or returning nothing rather than as an obvious error.

**What you can genuinely rehearse on one machine.** Lowering
`CH_MEMORY_CONFIG` to `memory.xml` (1.1 GB per node, the value written
for six servers sharing a host) does fit six ClickHouse servers plus
three Keepers here, which is enough to exercise the Swarm mechanics —
configs delivery, overlay DNS, `ON CLUSTER` DDL, the shard split,
`silver_snapshot.sh`'s task lookup — with the placement constraints
removed. The three PostgreSQL nodes and three etcd nodes likewise fit
comfortably **on their own**, which is how the Patroni failover
behaviour described above was verified. What does not fit is all of it
at once, which is the honest summary: the mechanisms are all testable on
one machine, one subsystem at a time; the topology is not.

Clean up afterwards:

``` bash
docker stack rm radar-stores
docker swarm leave --force
```

#### Which machine runs which command

| Command | Where |
|----|----|
| `docker stack deploy` | any **manager** (store1–3) |
| `./bootstrap/silver_snapshot.sh` (`restore`, `trim`, `export`) | the machine hosting **s1r1** (store1) |
| `docker exec … main.recompute_all()` | the **pipeline** machine |
| `docker compose -f docker-compose.diagnostics.yml run --rm diagnostics` | any machine on `pipeline_network`, with `.env.intended` sourced |
| `docker compose -f docker-compose.spark.yml …` | the **pipeline** machine (it mounts that machine's `shared_data`) |
| `seed_test_users.py` | anywhere that can reach `:8000` |
| frontend | each user machine |

### Returning to testing mode

Remove the two Swarm stacks, then start the plain-Compose stack. Nothing
is deleted, in either direction:

``` bash
docker stack rm radar radar-stores                     # on a manager
docker compose --env-file .env.testing -f docker-compose.stores.yml up -d
docker compose --env-file .env.testing up -d --build
```

Leaving the swarm itself (`docker swarm leave --force`) is only
necessary if the machines are being repurposed; an idle swarm costs
nothing.

Testing mode reattaches to its **own** ClickHouse and Keeper volumes
(`radar-testing_*`), which still hold whatever silver they held when
this mode was last used; the Swarm stack's `radar-stores_*` volumes are
left exactly as they were. If this is the first time testing mode has
run on this machine, its silver starts empty and needs one
`./bootstrap/silver_snapshot.sh restore`.

**MongoDB carries across; the gold layer does not, and does not need
to.** `mongo-init` detects that the replica set is still configured for
three members while only one is running — a state in which no primary
can be elected and every write fails — and reconfigures it to a single
member automatically, so **user profiles and tags survive the switch**.
Gold is a different case: a Patroni data directory records the cluster
identity and cannot be reused by a standalone server, so each mode keeps
its own. That costs nothing, because gold is *derived* — one recompute
rebuilds it from silver:

``` bash
docker exec pipeline_processing python3 -c "import main; main.recompute_all()"
```

> **Run that after any switch between the modes, and after moving gold
> to a new store.** The watermark trigger fires only when
> `max(DATEADDED)` in silver *increases*, and swapping the gold store
> leaves silver untouched — so the trigger correctly declines to fire
> and gold would otherwise stay empty. Restarting the processing
> container also does it, because the watermark it compares against is
> held in memory and starts at `None`, but the command above is
> explicit.

**What `BACKEND_URL` is.** The frontend holds no database credentials
and never contacts ClickHouse, MongoDB or PostgreSQL directly; every
value it displays is retrieved from the serving backend's HTTP API.
`BACKEND_URL` is the address of that API. Because the frontend runs on
each user's machine while the backend runs on the operator's, it must be
set to the operator machine's address and the backend's published port
8000.

| Situation | Value |
|----|----|
| Frontend and backend on the same machine | `http://host.docker.internal:8000` (the default; no configuration required) |
| Frontend on a user machine, backend on the operator machine | `http://<operator-host>:8000` |

If the address is unreachable, the dashboard reports that the backend is
unavailable rather than displaying data.

**One frontend runs per user machine.** The frontend is stateless and
holds no data; any number of instances may run concurrently against a
single backend. The only port a user machine needs to reach is **8000**,
on any swarm node — the routing mesh forwards it to a live backend
replica. The stores publish nothing: they are reachable only from inside
the overlay network, which is why they need no firewall rules of their
own beyond the three Swarm ports between machines.

------------------------------------------------------------------------

## What data is present, and when

**Present in the volumes before the pipeline starts.** The bootstrap
step loads a body of historical GDELT slices directly into the silver
layer (ClickHouse). This exists because the live pipeline processes one
15-minute slice at a time, at approximately one minute per slice; a
30-day history would otherwise require two to four days to appear. The
bootstrap applies the same filters as the live pipeline and writes
through the validation layer's own storage class, so the result is
indistinguishable from data that arrived live. It does not write gold:
the processing layer's watermark trigger detects the growth in silver
and builds the gold itself.

**Downloaded every 15 minutes once the pipeline is running.** The
ingestion layer reads
`http://data.gdeltproject.org/gdeltv2/lastupdate.txt`, which lists the
current 15-minute release, and downloads two files from it:
`<timestamp>.export.CSV.zip` (events) and `<timestamp>.mentions.CSV.zip`
(mentions). A representative slice contains approximately 979 events and
3,222 mentions before filtering.

There is a gap between the end of the bootstrap history and the moment
the pipeline is started. Data published by GDELT during that interval is
not retrieved.

------------------------------------------------------------------------

## Shutting down while preserving all data

Stop the tiers in the reverse of their start order. The commands differ
per mode, because each is torn down with the tool that started it:
`docker compose down` in testing mode, `docker stack rm` in intended
mode.

**Optional first (for testing mode) — keep only the shipped 30-day
history.** The pipeline polls continuously, so a store that has been
running holds the seed *plus* everything since. Stop the ingest path
before trimming, or the next poll lands mid-clean-up and puts the excess
straight back:

``` bash
docker compose --env-file .env.testing stop ingestion parsing validation        # stop new data
./bootstrap/silver_snapshot.sh trim seed                                        # silver
docker exec pipeline_processing python3 -c "import main; main.recompute_all()"  # gold
```

`seed` resolves to the last slice in `data/silver_seed`;
`trim <YYYYMMDDHHMMSS>` takes an explicit cutoff. Skip all three to keep
the live history.

**Testing mode:**

``` bash
docker compose -f 5-serving/docker-compose.serving.yml down
docker compose --env-file .env.testing down
docker compose --env-file .env.testing -f docker-compose.stores.yml down
```

**In intended mode these three commands run on three different
machines**, which is easy to miss because they read as one sequence:

``` bash
# pipeline machine — stop new data arriving
docker service scale radar_ingestion=0 radar_parsing=0 radar_validation=0

# the machine hosting clickhouse-s1r1 (store1) — docker exec is daemon-local
./bootstrap/silver_snapshot.sh trim seed

# pipeline machine again — propagate the trim into gold
docker exec $(docker ps -qf name=radar_processing) \
  python3 -c "import main; main.recompute_all()"
```

**What the third command does to the gold layer, precisely.** `trim`
only touches silver, so the rebuild is what propagates the change into
gold:

- **`user_articles` is rebuilt from scratch per user**
  (`DELETE … WHERE user_id`, then re-insert), so every article that no
  longer survives the trim disappears from that user's pool.
- **`articles` is upserted** (`MERGE INTO`) and then **swept**: any row
  no `user_articles` row still references is deleted.

The sweep is what stops `articles` growing forever. It runs at the end
of every recompute — full or single-user — so it also fires when someone
narrows their own preferences on the dashboard, not only after a trim.
See [Orphaned gold
rows](#orphaned-gold-rows-and-why-they-can-be-removed-safely).

**Intended mode** — the stacks are removed from a manager, so there is
no per-machine sequence to get right; Swarm stops the tasks wherever
they run:

``` bash
# each user machine
docker compose -f 5-serving/docker-compose.serving.yml down

# any manager — removes both tiers across all seven machines
docker stack rm radar radar-stores
```

`docker stack rm` removes services, tasks and the stack's networks. It
does **not** remove volumes or configs, so all durable data survives.
Leave the swarm itself only if the machines are being repurposed:

``` bash
docker swarm leave --force     # on each machine
```

The overlay network is created by hand and is not owned by either stack,
so it survives `stack rm` and is reused on the next deploy. Recreate it
only if it was explicitly removed:

``` bash
docker network create -d overlay --attachable pipeline_network
```

`down` removes containers and networks. It does not affect named
volumes, so all durable data survives and is available at the next
start:

| Data | Volume | Preserved |
|----|----|----|
| User profiles — territories, keywords | `mongo1/2/3_data` (`radar.users`) | Yes |
| Per-user tags — archived, needs action, monitoring | `mongo1/2/3_data` (`radar.tags`) | Yes |
| Gold — `articles`, `user_articles` | `postgres_data` (testing) / `radar-stores_pg_*_data` (intended) | Yes |
| Silver — `gdelt_events`, `gdelt_mentions` | `radar-testing_ch_*_data` (testing) / `radar-stores_ch_*_data` (intended) | Yes |
| In-flight raw and parsed slices | `shared_data` | Yes, and disposable in any case |

> **The `-v` flag must never be used.** `docker compose … down -v`
> deletes the volumes, permanently destroying every user profile, every
> tag, and the entire silver and gold history. Only the most recent
> 15-minute GDELT slice could be re-retrieved.

Passing `--build` at the next start is safe: it rebuilds images from
source and does not affect volumes.

------------------------------------------------------------------------

## One-time setup (automated)

Both steps belong to the **stores** tier, so the pipeline never repeats
them, and both are idempotent:

- **MongoDB replica set** — the `mongo-init` service executes
  `rs.initiate(rs0)`, guarded by `rs.status()`, and is a no-operation
  once the set exists.
- **PostgreSQL gold schema** — `postgres-init/01_schema.sql` is executed
  once, at first database creation. In testing mode the image's own
  `/docker-entrypoint-initdb.d` hook runs it; in intended mode Patroni
  runs `postgres-init/init-gold.sh` from its `bootstrap.post_init` hook
  instead, because Patroni performs `initdb` itself and never invokes
  that entrypoint hook.
- The processing layer publishes the territory table to MongoDB at
  startup, retrying until MongoDB is available; the frontend retrieves
  it from the backend via `GET /territories`.

Three test accounts are defined in `5-serving/frontend/auth.py`. Their
profiles are created by running, once, after the backend is available:

``` bash
python3 5-serving/seed_test_users.py
```

The gold layer is built per user. Until at least one profile exists,
`articles` and `user_articles` remain empty by design.

------------------------------------------------------------------------

# Design notes

The following section records the reasoning behind the storage and
pipeline decisions.

## Every difference between testing mode and intended mode {#every-difference-between-testing-mode-and-intended-mode}

The two modes run **identical application code**. No Python file, table
name, cluster name or service name differs between them. Everything that
changes is configuration and topology.

The two modes are also driven by **different files**, which is the
clearest way to hold the distinction:

|   | Testing | Intended |
|----|----|----|
| Orchestrator | plain `docker compose` | **Docker Swarm** (`docker stack deploy`) |
| Stores file | `docker-compose.stores.yml` | `docker-stack.stores.yml` |
| Pipeline file | `docker-compose.yml` | `docker-stack.pipeline.yml` |
| Env file | `.env.testing` | `.env.intended` |
| Network | local bridge `pipeline_network` | **attachable overlay** `pipeline_network` |
| Machines | 1 | 7, plus one per user |

### Why the overlay network is the decision everything else follows from

An overlay network gives every machine the same DNS namespace:
`clickhouse-s1r1` resolves to the same node from anywhere in the swarm.
That single property is why almost nothing else had to change to go
distributed:

- `clickhouse/cluster.xml` still names hosts as
  `clickhouse-s1r1`…`s2r3`.
- `storage.py` still issues `CREATE TABLE … ON CLUSTER gnews_cluster`.
- `docker-compose.yml`'s store addresses still default to service names.
- **No store port is published at all.** ClickHouse, MongoDB and
  PostgreSQL are reachable only from inside the overlay; only the
  backend's `8000` faces users.

The earlier single-machine arrangement needed a `STORES_HOST`
placeholder edited into an env file, and published ports for every
store. Both are gone.

### `endpoint_mode: dnsrr` — mandatory, and silent when wrong

Swarm's default endpoint mode is `vip`: a service name resolves to a
virtual IP that load-balances across tasks. That is **wrong** for
ClickHouse and Keeper, and it fails in a way that looks like nothing
happening at all.

ClickHouse works out *which node it is* by matching its own interface
addresses against the `<replica><host>` entries in `cluster.xml`; Keeper
does the same against `<raft_configuration>`. Under `vip` the service
name resolves to a virtual address that belongs to no container, so **no
node ever recognises itself**. An `ON CLUSTER` statement is then
accepted, written to the Keeper queue, and never claimed:

```         
Code: 159. Distributed DDL task /clickhouse/task_queue/ddl/query-0000000002
is not finished on 6 of 6 hosts (0 of them are currently executing the task,
0 are inactive)
```

The tables are silently never created. `endpoint_mode: dnsrr` resolves
the name straight to the task's own container IP, which is what both
self-identification and replica-to-replica part fetches need;
`hostname:` makes the container agree about its own name. This was
reproduced and then fixed on a real swarm.

### What Swarm ignores from a Compose file

| Key | What happens | Replacement |
|----|----|----|
| `container_name` | ignored — tasks are `<stack>_<service>.<slot>.<id>` | `silver_snapshot.sh` resolves s1r1 by service label |
| `profiles` | ignored | not needed: the Swarm stack *is* intended mode |
| `depends_on` | ignored | already safe — every layer retries its store indefinitely |
| `restart` | ignored | `deploy.restart_policy` |
| `ulimits` | **ignored** | `/etc/docker/daemon.json` per machine, daemon restarted |
| bind mounts | need the file on the target machine | Swarm **configs**, distributed by the manager |

`docker stack deploy` additionally accepts **no `--env-file`**:
variables must be sourced into the shell
(`set -a; . ./.env.intended; set +a`) or the defaults quietly apply —
which are the testing-mode ones.

### Why store nodes are pinned and the pipeline is not

Every store service carries
`placement.constraints: [node.labels.store == storeN]`. A ClickHouse
node owns a local volume *and* a replica identity registered in Keeper,
so rescheduling it onto another machine would produce a node with an
empty volume claiming an identity Keeper already knows — a broken
replica, not a fresh one. The same is true of a Patroni data directory
and a MongoDB member's local oplog.

The pipeline tier is the opposite and roams freely, because
`shared_data` holds only the in-flight slice: a rescheduled pipeline
re-polls GDELT and continues.

### One idea, three times: every store is addressed as a list

This is the single most useful thing to know about how the tiers
connect. No client holds one address for a store; each holds **all** of
them, and the driver finds a live node:

| Store | Variable | Mechanism |
|----|----|----|
| ClickHouse | `CLICKHOUSE_ALT_HOSTS` | clickhouse-driver tries the alternates when the entry node is unreachable |
| MongoDB | `MONGO_URI` (three members) | PyMongo discovers the primary and follows elections |
| PostgreSQL | `POSTGRES_DSN` (three members, `target_session_attrs=read-write`) | libpq skips read-only standbys and finds the leader |

Without the first of these, losing s1r1's *machine* would stop the
pipeline even though the shard's other two replicas hold every row — the
data would survive a failure the connection could not. In testing mode
all three degenerate to a single address and behave exactly as before.

### Services that exist only in intended mode

| Service                           | Purpose                           |
|-----------------------------------|-----------------------------------|
| `clickhouse-s1r2`, `s1r3`         | replicas 2 and 3 of shard 1       |
| `clickhouse-s2r1`, `s2r2`, `s2r3` | the whole of shard 2              |
| `clickhouse-keeper-2`, `-3`       | the other two Keeper nodes        |
| `mongo2`, `mongo3`                | the other two replica-set members |
| `postgres-2`, `postgres-3`        | the two PostgreSQL replicas       |
| `etcd-1`, `etcd-2`, `etcd-3`      | Patroni's leader lock             |

In `docker-compose.stores.yml` the first four rows are gated behind
`profiles: ["full"]`, so testing mode simply does not start them. The
Swarm stack has no profiles and always deploys everything.

### Configuration files that exist in more than one version

| Setting | Testing | Intended | Consequence |
|----|----|----|----|
| `cluster*.xml` `<remote_servers>` | 1 shard, 1 replica | 2 shards × 3 replicas | testing has neither sharding nor redundancy |
| `cluster*.xml` `<zookeeper>` | 1 Keeper | 3 Keepers | the list must match the running ensemble |
| `keeper-1*.xml` `<raft_configuration>` | 1 server | 3 servers | a lone server forms a quorum of one and elects itself |
| `memory*.xml` `max_server_memory_usage` | 2.5 GB (`memory.local.xml`) | **4 GB** (`memory.distributed.xml`) | one node per machine, so it may take most of it. `memory.xml`'s 1.1 GB exists only for the six-on-one-host case |
| gold schema | image entrypoint hook | Patroni `bootstrap.post_init` | Patroni runs `initdb` itself and never invokes the image's hook |

### Settings that differ in value only

| Variable | Testing | Intended | Why |
|----|----|----|----|
| `CLICKHOUSE_INSERT_QUORUM` | `1` | `2` | one replica can never satisfy a quorum of two — every insert would fail with error 285 |
| `CLICKHOUSE_ALT_HOSTS` | unset | `clickhouse-s1r2:9000,clickhouse-s1r3:9000` | see above |
| `MONGO_MEMBERS` | `1` | `3` | drives whether `mongo-init` configures a one- or three-member set |
| `POSTGRES_DSN` | one host | three hosts + `target_session_attrs=read-write` | finds the Patroni leader without reconfiguration |
| `CH_MEMORY_CONFIG` | `memory.local.xml` | `memory.distributed.xml` | see above |
| `STORE_VOLUMES` | `radar-testing` | n/a — the Swarm stack names its own | keeps the two topologies' coordination state apart |

### Sharding, and the one query shape it forbids

Only intended mode actually shards. Both silver tables are `Distributed`
routers over `ReplicatedReplacingMergeTree` locals, sharded on
`cityHash64(GLOBALEVENTID)`, which is chosen so that **an event and all
of its mentions land on the same shard** — joins stay local, and
repeated copies of an event collapse on the node that holds them.
`internal_replication=true` means the Distributed table writes to one
replica per shard and lets ReplicatedMergeTree copy it onward; with
`false` it would write to all three itself and triple-insert.

Measured on a real two-shard cluster, restoring the seed: shard 1 took
51,914 rows and shard 2 took 52,058 — 103,972 together, each replicated
identically to its three nodes.

The constraint this creates: ClickHouse refuses a **distributed subquery
nested inside a distributed query**
(`distributed_product_mode = 'deny'`), so
`… WHERE GLOBALEVENTID IN (SELECT … FROM gdelt_mentions …)` fails with
`Code: 288`. Every query in the project therefore uses plain aggregates
and literal id lists — see [Retention](#retention-the-365-day-rule),
where this is worked through in full. In testing mode the planner has
one shard and nothing to distribute, so the restriction never bites and
a bad query shape would pass unnoticed.

### Operations that behave differently, without any code change

- **`mutations_sync = 2`** waits for *every* replica to apply a
  mutation, which is what makes the counts reported by retention and
  `trim` final. With one replica it returns instantly; with three it
  waits for all of them, and if one is down it blocks until
  `replication_wait_for_inactive_replica_timeout` (120 s) and then
  errors. Nothing is lost — the retention job writes its marker only
  after a successful run, so the next daily pass retries.
- **Row counts read immediately after an insert can be low** on six
  nodes, while the second shard and the replicas are still being filled.
  `silver_snapshot.sh restore` printed 51,914 and then 103,972 seconds
  later. Testing mode is exact.
- **`ON CLUSTER` DDL** has to reach six nodes through Keeper rather than
  one, so the first schema creation takes seconds rather than being
  instantaneous.

### Switching modes: state that persists across the switch

Three stores record their own cluster membership on disk. That record
survives a mode change, and if it disagrees with the topology now
running, the store fails in a way that looks like a network fault rather
than a configuration error.

**MongoDB — handled automatically.** A set still configured for three
members while one is running has no majority, so no primary is elected
and every write fails with `NotWritablePrimary`. `mongo-init` compares
the configured count against `MONGO_MEMBERS` on every start and issues
`rs.reconfig(…, {force: true})` when they differ. `force` is required
precisely because there is no primary to accept an ordinary
reconfiguration.

**ClickHouse Keeper — resolved by giving each mode its own volumes.**
Keeper persists its Raft membership. A Keeper whose volume says it
belongs to a three-node ensemble cannot elect a leader when started
alone, and rejects every connection with
`Coordination::Exception: … doesn't see leader or stale`. ClickHouse
then retries indefinitely, so the symptom is a pipeline that hangs at 0%
CPU rather than an error. This cannot be fixed by *clearing* a volume
from a compose argument — Compose has no conditional "empty this volume"
directive — but it can be fixed by never sharing them. Testing mode's
volumes are named `${STORE_VOLUMES:-radar-testing}_*`; the Swarm stack
names its own.

**PostgreSQL — same reasoning.** A Patroni data directory records the
cluster identity and cannot be reused by a standalone server, so each
mode keeps its own gold volume. This costs nothing, because gold is
derived: one `recompute_all()` rebuilds it from silver.

MongoDB is deliberately **not** mode-scoped, so user profiles and tags
survive the switch intact.

### Capabilities that testing mode does not have

- **No redundancy of any kind.** Losing the single ClickHouse node loses
  silver; losing the single MongoDB node loses profiles and tags; losing
  the single PostgreSQL node loses gold until it is recomputed.
- **No sharding**, so no parallel scan across shards — and no exposure
  to the `distributed_product_mode` restriction, which is why a query
  shape that would fail in intended mode can pass here.
- **No automatic failover** anywhere: no second MongoDB member to
  promote, and no Patroni or etcd at all.
- **No quorum-acknowledged writes**, since there is no second replica to
  acknowledge.
- **A single Keeper**, a single point of failure for coordination.
- **No machine-level failover of the pipeline**, which Swarm provides in
  intended mode by rescheduling it onto another node.

Everything else — the filters, the deduplication rules, the retry
behaviour, the triggers, the schemas and the enrichment — is
byte-for-byte identical.

## Why three different databases

Each store was selected for the access pattern of the data it holds.

**ClickHouse holds the silver layer** — every event and every article
mention GDELT publishes, appended continuously and never updated in
place. The case for a columnar engine here rests on the SHAPE of the
data and the queries, not on how many rows there happen to be, so it
holds identically at a few hundred rows and at tens of millions. Three
properties, each measured on the shipped seed:

**1. The table is far wider than any query needs — 61 columns, 13
used.** GDELT's event schema has 61 columns. The silver-to-gold job
needs thirteen of them (`GLOBALEVENTID`, `Day`, `EventCode`,
`GoldsteinScale`, `Actor1Name`, the geo columns and the actor country
codes). Spark pushes that projection down, and ClickHouse's own
`system.query_log` confirms what it receives is not `SELECT *` but
exactly those thirteen names. Those columns are **2.05 MiB of the
table's 10.90 MiB — 19%**. A row store must read all 61 columns to reach
13, because a row is one contiguous record on disk; a column store reads
thirteen files and ignores forty-eight. That 13-of-61 ratio is a
property of the schema: it is the same on the first slice ingested as on
the ten-millionth.

**2. The most frequent query reads exactly one column.** The processing
layer polls `SELECT max(DATEADDED) FROM gdelt_events` every 60 seconds
to decide whether silver has advanced. That is one column out of 61, and
in a column store it touches one file and skips the rest of the table
entirely. In a row store the same aggregate walks every row in full to
read one field from each. The daily retention sweep is the same shape —
it groups on `GLOBALEVENTID` and aggregates `MentionTimeDate`, two
columns out of nineteen. These are cheap in absolute terms today, but
they are the queries that run constantly, and their cost scales with the
table while the work they do does not.

**3. Storing one column together compresses it.** Values in a column are
the same type and often repeat, so they compress far better stored
together than interleaved with unrelated fields. Measured: **3.2× on
events (35.33 MiB → 10.90 MiB)** and **3.3× on mentions (75.27 MiB →
22.99 MiB)**. That is a per-column property and holds at any row count.

**Two honest qualifications**, because the picture is not uniform:

- **Pruning barely helps on `gdelt_mentions`.** The job reads **84% of
  that table's bytes**, because the columns it wants *are* the expensive
  ones: `MentionIdentifier` 6.25 MiB, `article_keywords` 6.07 MiB,
  `article_title` 4.40 MiB — 16.7 of 23 MiB. You cannot prune away the
  payload. Columnar earns its place on the wide events table and on the
  narrow repeated aggregates, not here.
- **The compression is ordinary, not spectacular.** 3.2× is LZ4 on
  mostly high-cardinality free text. The low-cardinality codes (country,
  CAMEO) do compress beautifully, but they are a small share of the
  bytes.

**The justification has also changed shape.** It used to be that
ClickHouse itself did the per-user filtering — each user's geo and
keyword predicate was pushed into SQL. That work now happens in Spark,
and ClickHouse's role has narrowed to serving pruned column ranges in
parallel to the executors. That is a legitimate and common role — it is
what a columnar source does for a distributed compute engine — but the
accurate claim today is **wide-table column pruning plus parallel range
reads**, not "an analytical filtering engine".

**Measured on both tables.** Reading only the columns the job needs,
versus reading the whole row:

| Table | Columns read | Bytes | Time |
|----|----|----|----|
| `gdelt_events` | 13 of 61 | **22.15 MiB** vs 89.85 MiB | **46 ms** vs 185 ms |
| `gdelt_mentions` | 9 of 19 | 240.54 MiB vs 311.90 MiB | 1,149 ms vs **930 ms** |

Events is a **4× win on both counts** — a quarter of the bytes in a
quarter of the time. Mentions is the honest counterexample: pruning
saves only 23% of the bytes, and the pruned read came out **slower than
reading everything**. Fewer columns did not help, because the `FINAL`
merge dominates the cost and the columns the job wants are most of the
table. Columnar storage earns its keep on `gdelt_events` and on the
narrow repeated aggregates; on `gdelt_mentions` it is at best a wash.

**Why both tables are sorted on `GLOBALEVENTID` first.**

```         
gdelt_events_local     ORDER BY (GLOBALEVENTID, ActionGeo_CountryCode)
gdelt_mentions_local   ORDER BY (GLOBALEVENTID, MentionIdentifier)
```

In a MergeTree the sorting key *is* the primary index, and a query can
only skip granules when it filters on a **prefix** of it. The query this
is chosen for runs on every 15-minute slice: the validation layer checks
that each arriving mention refers to an event that already exists —

``` sql
SELECT DISTINCT GLOBALEVENTID FROM gdelt_events WHERE GLOBALEVENTID IN (…)
```

(`storage.existing_event_ids`, the referential-integrity check). Leading
with `GLOBALEVENTID` makes that an index seek instead of a scan.

The events table used to lead with `ActionGeo_CountryCode`, and the cost
was measurable. Looking up 3 ids, before and after the reorder:

| Sort key                                 | Rows read  | Bytes       | Time     |
|------------------------------------------|------------|-------------|----------|
| `(ActionGeo_CountryCode, GLOBALEVENTID)` | 253,131    | 1.93 MiB    | —        |
| `(GLOBALEVENTID, ActionGeo_CountryCode)` | **30,156** | **235 KiB** | **6 ms** |

An 8.4× reduction on a query that runs every fifteen minutes. It does
not fall to 3 rows because the index addresses granules of 8,192 rows,
and `FINAL` reads across parts — but the difference between scanning the
table and touching a few granules is the whole point.

Applying a changed sort key to an existing volume takes a deliberate
step, because the schema is created with `CREATE TABLE IF NOT EXISTS`
and ClickHouse cannot `ALTER` a sorting key into a different order:
`./bootstrap/silver_snapshot.sh recreate`, restart the validation layer
(it owns the schema and calls `ensure_tables()` once, at startup), then
`restore`. A fresh clone needs none of this — it creates the tables
correctly the first time. That ordering existed to help per-user
geographic filtering, which no longer reaches ClickHouse at all: the geo
predicate is now a Spark expression evaluated in memory
(`spark_gold.user_predicate`). Nothing was given up by reordering,
because geographic filtering never relied on the sort key anyway — it
has a dedicated skip index,
`INDEX idx_action_cc ActionGeo_CountryCode TYPE set(0)`, which is
untouched. Deduplication is also unaffected: `ReplacingMergeTree`
collapses rows whose sorting-key *tuple* is equal, and reordering the
columns does not change the set being compared.

One cost remains. Every read uses `FINAL`, which forces merge-on-read
across parts and partly offsets the scan speed — the price of using
`ReplacingMergeTree` to absorb re-ingested slices.

**PostgreSQL holds the gold layer** — the finished, per-user article
sets the dashboard reads. Those queries are selective point lookups:
retrieve one user's articles, by index, returning complete rows. They
are transactional, and they upsert. A row-oriented store with B-tree
indexes is the correct instrument for retrieving whole rows by key. This
is the canonical OLTP workload.

The division can be stated in one sentence: **columnar storage where the
system scans, row storage where it looks up.** The expensive columnar
scan is paid once, during processing, so that every dashboard read is an
inexpensive indexed lookup.

The gold layer was originally Oracle, and moved to PostgreSQL for one
reason: Oracle Database Free supports neither RAC nor Data Guard, so it
could not be replicated across machines at all. That was tolerable while
every store sat on one host, and untenable once each store had to span
machines — it would have left gold as the single component with no
redundancy. Nothing in the design depended on Oracle specifically, and
the row-store argument above applies unchanged.

**MongoDB holds user profiles, per-user tags and the territory reference
table.** These are small, self-contained documents whose shape varies: a
profile contains a list of territories and a nested dictionary of five
keyword groups, none of which is naturally relational. A document store
fits without an object-relational mapping. MongoDB was also chosen for a
second, operational reason: its **change streams** allow the processing
layer to react the instant a user modifies their preferences, without
polling.

## Node counts, fault tolerance and distribution

The system is distributed at the storage layer.

- **ClickHouse — 6 data nodes plus 3 Keeper nodes.** The data is divided
  into **2 shards** by `cityHash64(GLOBALEVENTID)`, and each shard is
  held by **3 replicas** running `ReplicatedReplacingMergeTree`. A shard
  therefore survives the loss of two of its three nodes without data
  loss. The 3-node Keeper ensemble coordinates replication and
  `ON CLUSTER` DDL, and retains a quorum when one node is lost. The
  sharding key is deliberate: an event and all of its mentions hash to
  the same shard, so joins are local rather than cross-node, and
  repeated copies of an event from successive batches land on the same
  node, where the `ReplacingMergeTree` can collapse them.
- **MongoDB — 3 nodes** in replica set `rs0`. A majority is retained
  when one node is lost, and PyMongo performs primary failover
  automatically. Writes are issued with `w="majority"`.
- **PostgreSQL — 3 nodes under Patroni**, on three machines, with a
  3-node etcd ensemble holding the leader lock. One leader accepts
  writes and streams to two replicas. If the leader is lost, Patroni
  promotes a replica automatically, re-points the survivor at it, and
  rebuilds the old leader with `pg_rewind` when it returns rather than
  admitting a second writer. Clients need no reconfiguration:
  `POSTGRES_DSN` lists all three members with
  `target_session_attrs=read-write`, so libpq finds whichever node
  currently leads. Measured on a live cluster: killing the leader
  promoted a replica on a new timeline, and the same unchanged DSN
  reconnected to it with the pre-failover write intact.
- **Hand-offs.** The pipeline writes to ClickHouse with `insert_quorum`,
  so an append is acknowledged only once it has reached a quorum of
  replicas. MongoDB writes use majority acknowledgement. PostgreSQL
  writes are committed transactionally, and the row counts the server
  reports are compared against the counts submitted.

**PySpark is the silver-to-gold path** — the only one. There used to be
a second, in-process pandas implementation of the same rules; keeping
two hand-written versions of one intent meant every rule had to be
changed in both, and they had in fact diverged. Its parallelism is
genuine at all three stages: the read is a **partitioned JDBC read**, in
which Spark divides the `GLOBALEVENTID` range into
`SPARK_READ_PARTITIONS` disjoint ranges and issues one concurrent query
per range, so each executor retrieves only its own slice (and
ClickHouse's own `system.query_log` confirms the projection is pushed
down — it receives 13 named columns of `gdelt_events`' 61, not
`SELECT *`); the events-to-mentions join is a distributed shuffle join
across `SPARK_SHUFFLE_PARTITIONS`; and `df.write.jdbc()` opens one
connection per partition, so the write is executed by the executors in
parallel. Because no result is materialised centrally, **no row cap is
required** — the old `GOLD_EVENTS_LIMIT` of 20,000 has been removed, and
it had been silently truncating: one seeded account matched 45,381
events against that ceiling. Spark's JDBC writer offers only `append`
and `overwrite` and cannot upsert, so the job writes to staging tables
that it creates and drops itself, and a single subsequent SQL statement
publishes them into the live tables:
`INSERT … ON CONFLICT (doc_id, global_event_id)` for `articles`,
delete-and-reinsert per user for `user_articles`, and then the anti-join
sweep of orphaned rows — all inside one publish transaction, after
`user_articles` has been rebuilt.

**Where it runs is the only thing that differs between the modes.**
`SPARK_MASTER` is `local[*]` in testing mode, so the job runs inside the
processing container with no cluster to deploy, and
`spark://spark-master:7077` in intended mode, where Swarm spreads the
master and `SPARK_WORKERS` workers across machines with no placement
constraint. Each compose file already defaults to the right one, so the
switch needs no action beyond deploying the right file.

## Why the pre-loaded silver is small, and why it took so long to produce

The silver seed committed to this repository is around 15 MiB, which is
easy to mistake for a small amount of work. It is not. Even though the
volume's ready-made data looks small in size, querying it from GDELT and
letting the pipeline turn it into silver took about half a week. This is
because the bronze layer being put together — which is removed
progressively once turned to silver — was by far larger.

The figures for the 30-day window shipped here:

| Stage                                                     | Size       |
|-----------------------------------------------------------|------------|
| Bronze — the raw GDELT archives that had to be downloaded | ≈ 410 MB   |
| Silver — after filtering, validation and enrichment       | **15 MiB** |

Two reductions compound. The supply-chain relevance filter discards
roughly 97% of events, keeping about 31 of every 979 in a slice; and
Parquet's columnar compression is very effective on the low-cardinality
codes that dominate what remains. The result is a 26-fold reduction.

The time went almost entirely into work that leaves no trace in the
final size: downloading 5,762 archives, and then fetching roughly 85,000
individual article pages to extract titles and keywords. Enrichment
alone accounts for the bulk of it, and it is bounded by how quickly
remote news servers respond rather than by any local resource.

This is precisely why the seed is committed. The expensive work is done
once, by the maintainers, and every clone restores the result in seconds
instead of repeating it.

## How the silver seed is implemented

Docker volumes are not part of a git repository. They live in Docker's
own storage on each machine, outside the project directory, so a clone
always creates **empty** volumes. The seed is the mechanism that bridges
that gap: ordinary files that git can carry, plus a command that loads
them into the volume on whatever machine runs it.

**The files.** `data/silver_seed/` holds two Parquet files,
`gdelt_events.parquet` (7.0 MiB, 103,972 rows) and
`gdelt_mentions.parquet` (8.1 MiB, 111,430 rows) — 15 MiB in total,
against the 410 MB of raw archives they were distilled from. They are
committed; `data/release/` and `data/raw/` are not.

**The tool.** `bootstrap/silver_snapshot.sh` has three verbs:

| Verb | Action |
|----|----|
| `export` | `SELECT * FROM <table> FINAL FORMAT Parquet` for each silver table, written to `data/silver_seed/`. `FINAL` collapses the `ReplacingMergeTree` duplicates, so the snapshot holds one row per key. |
| `restore` | `INSERT INTO <table> FORMAT Parquet` for each file, streamed through the **Distributed** table, so rows are routed to their shard exactly as a live write would be. Waits for the validation layer to have created the schema. |
| `wipe` | `TRUNCATE TABLE <table>_local ON CLUSTER` — used when rebuilding from scratch. |
| `trim <slice>` | Deletes every row published after a given 15-minute slice. Needed when rebuilding the seed, because the live pipeline keeps polling while a backfill runs; without it the exported seed would carry an arbitrary tail of "today" and ship it to everyone who clones the repository. |

**Rebuilding the seed from the raw archives** is a four-step sequence,
and the order matters for the same reason `trim` exists:

``` bash
# REQUIRES ingestion, parsing, and validation to be on first. (So, first perform steps 1 and 2 of Startup)
docker compose --env-file .env.testing stop ingestion parsing validation  # stop live writes
./bootstrap/silver_snapshot.sh wipe
ENRICH=1 docker compose --env-file .env.testing -f docker-compose.bootstrap.yml run --rm bootstrap # WARNING: might want to run with `caffeinate`!!!
./bootstrap/silver_snapshot.sh trim 20260727171500   # the last slice in the release
./bootstrap/silver_snapshot.sh export
```

> **On macOS, copy the archive into a named volume first.** Docker
> Desktop's file sharing is very slow for per-file metadata, and slowest
> under `~/Desktop` and `~/Documents`, which macOS additionally guards.
> Measured here, simply *listing* the 5,762 archives took **0.04 s**
> from a named volume or on the host, against **more than 25 minutes**
> through a bind mount of `./data/release` — during which the loader
> sits at 0% CPU and looks frozen. `docker-compose.bootstrap.yml`
> documents the one-line `tar` that moves the archive inside Docker,
> after which `RELEASE_SOURCE=gdelt_release` runs the load at full
> speed.

**Why restore is safe to repeat.** Both tables are `ReplacingMergeTree`:
`gdelt_events` keyed on `GLOBALEVENTID`, `gdelt_mentions` on
`(GLOBALEVENTID, MentionIdentifier)`. Re-inserting identical rows
collapses back to the same counts rather than duplicating them —
measured: a second restore left the totals unchanged at 103,972 and
111,430.

**And why the restore disables insert de-duplication.**
`ReplicatedMergeTree` keeps the checksums of recently inserted blocks
and silently skips any block it has seen before. That is a useful
protection against a retried insert, but it makes restore *after a
deletion* fail in the worst possible way: the seed file produces
byte-identical blocks, ClickHouse recognises them, drops them, and the
command reports success having restored **nothing**. It was measured
doing exactly that — after the retention job removed 7,471 events,
`restore` reported "now 96501 rows", i.e. the survivors and not one row
more. `restore` therefore issues `SETTINGS insert_deduplicate = 0`.
Correctness does not depend on the skipped check, because the
`ReplacingMergeTree` key collapses genuine duplicates anyway.

**Why gold is not snapshotted.** Only silver is captured. Gold is
derived, and derived per user: it depends on the territories and
keywords in each profile, which differ per installation. Shipping a gold
snapshot would bake one set of users' preferences into every clone.
Instead the processing layer rebuilds gold from the restored silver
through its normal trigger, so the result always matches the profiles
that actually exist on that machine and can never drift from what the
pipeline would have produced.

**Cost.** `restore` takes about three seconds; gold follows within the
trigger's 60-second poll. Producing the seed took 213 minutes of
enrichment plus the archive download.

## What causes the gold layer to update

Four things, two automatic and two manual. All of them call the same two
functions, `recompute_all()` and `recompute_user()`, so the result is
identical whichever path is taken.

| Trigger | Scope | Latency |
|----|----|----|
| **MongoDB change stream** on `radar.users` — fires on `insert`, `update` or `replace` of a profile | that one user (`recompute_user`) | **immediate**, typically under a second |
| **ClickHouse watermark poll** — compares `max(DATEADDED)` in silver against the last value seen | every user (`recompute_all`) | up to `WATERMARK_POLL_SECONDS`, default **60 s** |
| `POST /process/{user_id}` on the processing service | that one user | on demand |
| `POST /process-all` on the processing service | every user | on demand |

The change stream is what makes a preference change take effect
immediately: the moment a user saves new territories or keywords, their
profile document is written to MongoDB, the stream delivers the change,
and `recompute_user` re-runs that user's filter against the whole of
silver. Articles that were previously in silver but matched nobody are
therefore promoted into that user's gold at once, without waiting for
new data to arrive. This is also why MongoDB runs as a replica set even
in testing mode: change streams are unavailable on a standalone server.

The watermark trigger covers the other direction — new data arriving for
existing preferences. It fires whenever the watermark **changes**:
upwards when a slice arrives, and downwards when silver is trimmed,
wiped or restored over. The downward case used to be ignored, which left
the trigger stuck and the dashboard reporting a stalled pipeline that
was in fact healthy; see
[Watermarking](#watermarking-how-the-pipeline-knows-it-is-making-progress).
No manual `recompute_all()` is needed for it any more.

## Why enrichment never reaches 100%

Enrichment fetches each article's page and extracts its title and
keywords. A consistent minority of URLs cannot be enriched.

A URL yields no title when the page is a dead link, sits behind a
paywall or a consent wall, is blocked to automated clients, redirects to
a section front page rather than an article, or is not an article at
all. GDELT indexes URLs as published; it does not guarantee that they
remain reachable, and a proportion of news links are unreachable within
days of publication.

Failures are handled without loss: a mention that cannot be enriched is
stored with an empty title, empty keywords and `enriched = 0`. It
remains a fully valid silver row and is still matched by the keyword
filter, which falls back to searching the URL itself for rows where
`enriched = 0`. Nothing is discarded, and the dashboard falls back to
displaying the URL where a headline is missing.

## Fault tolerance, layer by layer

### The pipeline

Five independent mechanisms, none of which depends on shared storage:

1.  **Container restart.** Every pipeline service is declared
    `restart: unless-stopped`, so a process that crashes is restarted by
    Docker without intervention.
2.  **Recovery by re-polling rather than by shared state.**
    `shared_data` holds only the slice currently in flight. A
    replacement machine starts with an empty volume and loses nothing,
    because the durable state lives entirely in the stores, which are a
    separate tier.
3.  **Immediate poll at startup.** Ingestion fetches the current GDELT
    release the moment it starts rather than waiting for its next
    15-minute tick, so a replacement machine begins contributing at
    once.
4.  **Idempotent re-ingestion.** Both silver tables are
    `ReplacingMergeTree`, so a slice ingested twice collapses to the
    same rows. This is what makes blind re-polling safe after a failure.
5.  **Endless, bounded retries at every boundary** — see the section
    below.

The pipeline is operated **active-passive**: one live instance at a
time. Two concurrent instances would poll GDELT twice and duplicate work
that the stores would then have to deduplicate.

### What actually happens when the pipeline fails, in each mode

The recovery behaviour is **not** the same in the two modes, and the
difference matters.

| Failure | Testing mode (plain `docker compose`) | Intended mode (Docker Swarm) |
|----|----|----|
| A single container crashes | Docker restarts it **on the same machine**, because every pipeline service declares `restart: unless-stopped` | Swarm restarts the task, by the same principle |
| The Docker daemon is restarted | Containers come back automatically | Same |
| **The whole machine fails** | **Nothing happens.** There is no second machine, and no data is lost — but the pipeline stops until the machine returns | Swarm detects the node is gone and **re-creates the whole pipeline on another node** carrying the `role=pipeline` label |

In testing mode the machine is a single point of failure for
*processing*, though never for *data*: the durable state is in the
stores, and a restarted pipeline re-polls GDELT and continues. That is
an acceptable trade for a single-machine test environment.

### Docker Swarm — a blueprint, not currently active

`docker-stack.pipeline.yml` describes the pipeline as a Swarm stack,
which is what provides machine-level failover in intended mode. **It is
not deployed at present** (`docker info` reports `Swarm: inactive`); the
file is a prepared description that has to be activated deliberately.

Each service declares `replicas` and `restart_policy: any`, so when a
task or an entire node is lost, Swarm re-creates it on another node
satisfying the placement constraint. Layers 1 to 4 all carry the same
constraint, `node.labels.role == pipeline`, because they hand data to
one another through a local volume and must therefore stay co-located.
The backend, being stateless, declares `replicas: 3` and is spread
across the swarm behind the routing mesh. A rescheduled pipeline starts
with a fresh, empty volume and re-polls GDELT, which is precisely the
recovery model described above.

Activating it requires three steps that plain Compose does not:

``` bash
docker swarm init --advertise-addr <manager-ip>     # on the manager
docker swarm join --token <...> <manager-ip>:2377   # on each other machine
docker node update --label-add role=pipeline <node> # label the eligible nodes
docker stack deploy -c docker-stack.pipeline.yml radar
```

Swarm cannot build from a `Dockerfile`, so the images must be built once
and pushed to a registry before deploying; the stack refers to them
through the `REGISTRY` variable. Label more than one node
`role=pipeline` if Swarm is to have somewhere to move the pipeline to.

The stores are deliberately **not** placed in Swarm: stateful clustered
services are considerably harder to orchestrate, and they already have
their own replication and failover.

### The stores

- **ClickHouse.** Each shard holds three replicas of its data under
  `ReplicatedReplacingMergeTree`, so a shard survives losing two of its
  three nodes. Writes are issued with `insert_quorum`, meaning an append
  is acknowledged only once a quorum of replicas holds it, so an
  acknowledged write survives the loss of a node. Coordination runs on a
  three-node Keeper ensemble, which retains a majority when one member
  is lost. Each node's memory is capped explicitly, preventing the
  cluster-wide restart loop that occurs when several servers each assume
  they own the machine.
- **MongoDB.** A three-member replica set: a majority survives the loss
  of one member, and the driver performs primary failover automatically.
  Writes use `w="majority"`, so an acknowledged write is held by a
  majority. `mongo-init` additionally repairs the configuration when the
  member count changes, a state in which no primary can be elected and
  every write would otherwise fail.
- **PostgreSQL.** Three nodes under Patroni in intended mode (one in
  testing), coordinated through a 3-node etcd ensemble. A replica is
  promoted automatically when the leader is lost, and the multi-host
  `POSTGRES_DSN` means no client is reconfigured when that happens.
  Writes are committed transactionally and the row counts the server
  reports are compared against the counts submitted, so a partial write
  is detected rather than assumed successful. The honest limit:
  replication is asynchronous, so a promotion can lose writes the old
  leader had acknowledged but not yet shipped. Gold is derived from
  silver, so the repair is a recompute, not a restore.
- **All three** keep their data in named Docker volumes, which survive
  `docker compose down`.

## Why the pipeline itself runs on a single machine

Layers 1 to 4 hand data to one another as **files on a shared volume**:
ingestion writes to `/data/raw/csv`, parsing publishes to
`/data/latest_files`, validation consumes from there. A local volume
exists on exactly one host, so those four layers must be co-located.

**The heavy work no longer runs here, though.** Layers 1 to 4 must stay
co-located because they hand files to one another, and that is unchanged
— four services mount the same `shared_data` volume. But layer 4's
expensive step, silver → gold, is now submitted to Spark: in intended
mode the processing container is only the *driver*, and the read, the
join and the write execute on worker containers that Swarm places on any
machine. So "the pipeline runs on a single machine" remains true of the
file hand-off and of the triggers, retention and status writer — and is
no longer true of the computation itself.

This is a deliberate choice rather than an oversight, because the volume
of traffic does not justify anything more elaborate: four CSV files
every fifteen minutes. The relevant question is not how to distribute
that trickle, but what happens when the machine carrying it fails.

**The answer is that the pipeline recovers by re-polling, not by sharing
storage.** The shared volume holds only transient scratch data: the raw
slice currently being processed, the parsed pair awaiting validation,
and a status marker. None of it is a source of truth. If the machine
fails, a replacement starts the same stack with an empty local volume;
ingestion polls the current GDELT slice immediately at startup, and
processing resumes. The durable state is held entirely by the three
stores, which are a separate tier with a separate lifecycle and are
never re-created by the pipeline.

Re-ingestion is idempotent, which is what makes this safe.
`gdelt_events` is a `ReplacingMergeTree` keyed on `GLOBALEVENTID`,
retaining the newest `DATEADDED`; `gdelt_mentions` is a
`ReplacingMergeTree` keyed on `(GLOBALEVENTID, MentionIdentifier)`,
retaining the enriched row. A slice ingested twice collapses back to the
same rows.

The pipeline should be operated **active-passive**, with one live
instance at a time. Two concurrent instances would both poll GDELT and
perform duplicate work, which the stores would deduplicate but which
serves no purpose.

## Why Kafka would have been excessive

A message broker addresses problems this system does not have. The
hand-off is four CSV files per fifteen minutes — a trickle, not a stream
requiring buffering. Fan-out is not required, because each stage has
exactly one consumer. Replay is not required, because **GDELT itself is
the durable, replayable source**: any slice can be re-retrieved from its
published URL. Introducing Kafka would add a broker to operate, and
would add a second durable store whose contents would have to be
reconciled with the one already in place. The file volume performs the
same hand-off with no operational cost, and the recovery model above
supplies the durability a broker would otherwise provide.

## Why the bronze layer is ephemeral

Raw GDELT CSVs are deleted as soon as parsing has published the
corresponding slice, and the parsed pair is deleted as soon as
validation has stored it. Nothing raw is retained.

This is sound because the bronze layer is not a source of truth but a
staging area for data that is already durably published elsewhere and
can be retrieved again from GDELT at any time. Retaining it would
consume storage at a rate of roughly 400 MB per month per copy in
exchange for no recovery capability that re-polling does not already
provide. The layer that must not be lost is silver, which is replicated
three ways per shard.

Deletion is also what applies back-pressure: parsing publishes a new
pair only when `latest_files` is empty, so a slow validation cycle
throttles the upstream stages instead of allowing work to accumulate.

## B-trees and the choice of index key

PostgreSQL's primary keys are B-tree indexes, and a B-tree index entry
must fit within roughly 2,704 bytes — a third of an 8 KB page. (Under
Oracle the equivalent ceiling was about 6,398 bytes on an 8 KB block: a
different number, but the same problem, and the URL can exceed both.)

The natural key for an article is its URL, held as
`document_identifier VARCHAR2(2000)`. Under the `AL32UTF8` character set
a single character may occupy up to four bytes, so 2,000 characters may
occupy 8,000 bytes — in excess of the limit, raising `ORA-01450`. The
composite key `(user_id, document_identifier)` in `user_articles` would
be larger still.

The key is therefore **`doc_id BYTEA`**, the SHA-256 digest of the URL:
a fixed 32 bytes irrespective of URL length, deterministic, and
collision-resistant. The full URL is retained alongside it as ordinary,
non-indexed data, so nothing is lost for display purposes. The hash is
computed at the gold-store boundary only; every other layer continues to
handle URLs.

## Keeping validation within the 15-minute cadence

The validation cycle must complete within the interval between GDELT
releases, otherwise slices accumulate. Two bounds enforce this.

**Enrichment** is bounded by `ENRICH_TIMEOUT_SECONDS` (600 s per
mentions file), a time budget rather than a fixed cost: article scraping
proceeds across eight workers. In the offchance that the budget is
exhausted, the remaining mentions are stored unenriched rather than
delaying the cycle (which cannot be allowed to take longer than 15
minutes, since new GDELT data arrives at that cadence).

**Every ClickHouse operation** is bounded by `CLICKHOUSE_OP_TIMEOUT`
(120 s), applied as the server-side `max_execution_time` and matched by
a slightly longer socket timeout. This covers the referential-integrity
lookup, both appends, and the deduplication `OPTIMIZE`, which is
otherwise the operation most likely to run long as the table grows. The
total cycle is therefore bounded by the enrichment budget plus a bounded
number of bounded database operations.

## Country and territory code coverage

Territories are matched by two independent code systems: **CAMEO**
three-letter codes, which identify the *actors* in an event, and
**FIPS** two-letter codes, which identify the *location* of an event. An
event matches a user's perimeter if either system matches.

The two published lookups do not cover an identical set of places.
Reconciling them produced 237 entries, of which **eight have a FIPS code
but no CAMEO code** — that is, they can be matched as the location of an
event, but never as an actor. Two are sovereign states: **Kosovo** and
**South Sudan** (the CAMEO list predates South Sudan's independence).
The remaining six are territories: the British Indian Ocean Territory,
the French Southern and Antarctic Lands, Guernsey, Jersey, Saint Martin
and Saint-Barthélemy. Two further entries have the converse limitation,
holding a CAMEO code but no FIPS code: the Åland Islands and Palestine.

Reconciliation also required correcting errors in the published data —
the FIPS list mislabels Guinea's code as Equatorial Guinea, and labels
Slovakia's code as Czechoslovakia — and merging divergent spellings of
the same place, such as `Cote dIvoire` against `Ivory Coast`, and
`Columbia` against `Colombia`. The Palestinian territories are
consolidated into a single entry carrying all five related codes, so
that selecting it matches both actor and location.

## Retention: the 365-day rule {#retention-the-365-day-rule}

Everything else in the pipeline either appends or rebuilds. Silver is
append-only, gold's `articles` is upserted, and until this rule existed
**nothing in either store ever aged out** — both grew for as long as the
pipeline ran, and the only removal was the manual
`silver_snapshot.sh trim`.

A daily job now deletes events that have gone quiet for a year, together
with everything hanging off them:

| Store      | What is removed                                                |
|------------|----------------------------------------------------------------|
| ClickHouse | the event from `gdelt_events`, and all of its `gdelt_mentions` |
| PostgreSQL | its rows in `user_articles`, then in `articles`                |
| MongoDB    | any user's tag pointing at it                                  |

**365 days is a starting point, not a fixed constant.** It is set by
`RETENTION_DAYS`, so changing it is a one-line change requiring no
migration and no code edit — set the variable on the processing service
and the next run uses the new cutoff. The window is expressed in days
rather than years so it can be tuned at the granularity the store
actually turns over at; it still cannot delete anything the project
currently holds, which keeps it safe to ship.

**The clock read is `MentionTimeDate`, the article's own timestamp.**
Expiry is `max(MentionTimeDate)` over an event's mentions — the newest
article about it.

**Measured from the newest article, not the event date.** A long-running
story keeps attracting coverage: the event row is stamped once, but
articles arrive for as long as anyone is still writing. Measuring from
the event date would delete a story that is still being reported.
Measuring from its most recent article means an event survives exactly
as long as the world keeps talking about it, and ages out 365 days after
the last word. An event that never had a mention at all has no "most
recent article", so it falls back to its own `DATEADDED` — otherwise
such rows could never expire.

**Why deleting mentions by `GLOBALEVENTID` is the same thing as checking
each one's own `MentionTimeDate`.** Once an event is condemned, its
mentions are removed by event id rather than re-tested individually.
That is not a shortcut with different behaviour: if the *maximum*
`MentionTimeDate` for an event is below the cutoff, then every one of
its mentions is below it too, because the maximum is the largest. **No
mention younger than the cutoff can be deleted.**

The converse is deliberate: an old article belonging to a *still-active*
event is **kept**. A 2015 report on a story that received fresh coverage
last week survives with its event. Pruning it independently would also
corrupt the card ordering, which is keyed on the oldest article a card
holds — that key would drift forwards as a story's earliest coverage
aged out from under it.

**Order matters in both stores.** Mentions are deleted before events, so
a crash between the two statements leaves a harmless mention-less event
rather than mentions whose event has vanished — the state the
referential-integrity check assumes cannot happen. In PostgreSQL,
`user_articles` goes before `articles`, so the join never briefly points
at rows that are already gone.

**No query here nests one silver table inside another**, and on a
sharded cluster that restriction is not optional. `gdelt_events` and
`gdelt_mentions` are `Distributed` tables, and ClickHouse refuses a
*distributed subquery inside a distributed query* by default:

```         
Code: 288. Double-distributed IN/JOIN subqueries is denied
           (distributed_product_mode = 'deny')
```

Measured on a real two-shard cluster:
`… WHERE GLOBALEVENTID IN (SELECT … FROM gdelt_events …)` fails with
exactly that, while the same query with a **literal** id list succeeds.
A top-level `JOIN` between two such subqueries happens to be accepted by
the current version, but the margin is thin enough not to rely on.

The expiry set is therefore built from three single-table queries —
plain aggregates and literal `IN` lists — combined in Python. This is
exact rather than approximate because both tables shard on
`cityHash64(GLOBALEVENTID)`: every mention of an event lives on the same
shard as the event, so each shard's `GROUP BY GLOBALEVENTID` is already
complete. Verified to return an identical result on one shard and on
two.

**Deletes are issued in batches**, because the ids are substituted into
the SQL text and `max_query_size` caps a statement at 256 KB — roughly
21,800 ids, or six days of this pipeline's output. A nightly run is
nowhere near that: only the events that crossed the cutoff *that day*
expire, some 3,500 of them — one day of output, a figure that does not
change with the window length. The cases that would overflow are a
machine that was off for a week or more, whose catch-up run clears the
whole backlog at once, and the first run after `RETENTION_DAYS` is
shortened, which retires a long stretch of history in one go.

**In intended mode the deletes are slower.** `mutations_sync = 2` waits
for every replica to apply the mutation, which is what makes the
reported counts final. With one replica that returns immediately; with
three it waits for all of them, and if one is down it blocks until
`replication_wait_for_inactive_replica_timeout` (120 s) and then errors.
Nothing is lost when that happens — the day's marker is only written
after a successful run, so the next daily pass retries the same work.

**Schedule.** Daily at 00:00, plus a catch-up on startup when a midnight
was missed — a laptop that sleeps overnight would otherwise never clean
up at all. The last run is recorded in `/data/state/retention.json`,
written atomically, so a restart cannot lose or repeat it.
`ENABLE_RETENTION=0` disables the job entirely; it is gated separately
from the other triggers precisely because it is the only thing in the
pipeline that deletes data.

**On the shipped seed it deletes nothing.** The seed spans June–July
2026, so a 365-day cutoff lands in 2025 and nothing qualifies — verified
against a live store spanning 2026-06-27 to 2026-08-16, where a cutoff
of 2025-08-16 matched 0 events and 0 mentions. That is expected: the
rule is forward-looking, and a year of history has to accumulate before
it has anything to do. `RETENTION_DAYS` exists so the behaviour can be
exercised without waiting — moving the cutoff to 2026-06-30 expired
8,939 events and 9,571 mentions, left no survivor past the cutoff, and
left no mention orphaned from its event.

Every page that lists event cards says so in small print, because a card
disappearing is normal behaviour rather than a fault. The wording
differs by page, because the *second* reason a card can vanish does not
apply everywhere: the Radar View and the Archive follow the user's
preferences, so both say an event that no longer matches will be
removed, while Needs action and Monitoring are pinned against the orphan
sweep and say instead that what is filed there stays. See [Orphaned gold
rows](#orphaned-gold-rows-and-why-they-can-be-removed-safely).

## Orphaned gold rows, and why they can be removed safely {#orphaned-gold-rows-and-why-they-can-be-removed-safely}

The gold layer is normalised into `articles` (one row per **(article,
event) pair**) and `user_articles` (the
`(user_id, doc_id, global_event_id)` join). The two are written with
different semantics, and that asymmetry used to leave rubbish behind:

- `user_articles` is **rebuilt** per user on every recompute, so a
  user's set is always exactly what their current preferences select;
- `articles` is written with **MERGE**, which only ever inserts or
  updates.

So whenever a user narrowed their territories or keywords — or silver
was trimmed — the articles they dropped stayed in `articles`, referenced
by nobody. They were unreachable, because serving joins
`user_articles → articles`, but they accumulated without limit.

They are now swept at the end of every recompute:

``` sql
DELETE FROM articles a
WHERE NOT EXISTS (SELECT 1 FROM user_articles ua WHERE ua.doc_id = a.doc_id)
```

**This destroys nothing but the orphans.** A row referenced by *any*
user is kept by the anti-join, which is why the sweep is safe even after
a single-user recompute: user A dropping an article that user B still
holds does not remove it. `user_articles` is untouched, and so are
MongoDB (profiles, tags) and the silver layer. No volume has to be
recreated — an earlier version of this document claimed otherwise, and
was wrong.

**An orphan is never removed if any user is TRACKING it.** This is the
one exception to the rule above, and it is not optional. If a user has
filed an event as **red** (*Needs action from us*) or **yellow** (*Look
out for developments*), every article of that event is kept — whether or
not it still appears in anyone's `user_articles`, and whether the tag
belongs to the user whose recompute triggered the sweep or to somebody
else entirely.

The reason is how those two triage pages read. They fetch cards
**straight from `articles`, deliberately without joining
`user_articles`**, so that a tracked card survives the user later
dropping the territory that first brought it in. Such a row is
legitimately unreferenced — precisely what the anti-join targets.
Without this guard the sweep would delete it and leave the tag in
MongoDB pointing at nothing: the card would vanish from the red or
yellow page while still counted as tagged.

**Archiving is deliberately NOT protection.** Archiving an event says it
is *not* important — it is how a user pushes something off the Radar
View. There is therefore nothing worth preserving once that event also
stops matching their registered territories and keywords: the row is
swept like any other orphan and the card leaves the Archive page.
Protecting it would mean an event a user explicitly dismissed outlived
events they never dismissed, which is backwards.

The tag itself is left in MongoDB when this happens, which is deliberate
and useful rather than an oversight: if the user later re-adds the
territory or keyword, the next recompute re-creates the article row and
their archive entry simply reappears.

Only the tag **value** counts, not its presence. A cleared tag is stored
as a key with a null value, and reading the keys alone would protect
events whose tag the user has removed. `mongo_reader.PROTECTED_TAGS`
matches on the value, so `requires_action` and `monitor` protect and
nothing else does.

Measured, on the three seeded accounts: an event tagged `monitor` and an
event tagged `archive` were both removed from a user's pool, then the
sweep was run — the archived event's article row was deleted and the
monitored event's was kept.

If the tag list cannot be read, the sweep is **skipped entirely** rather
than run unguarded. Leaving a few unreachable rows costs a little space;
deleting a card someone filed loses their work.

`NOT EXISTS` rather than `NOT IN`: the planner optimises the anti-join,
and `NOT IN` would silently match nothing at all if any `doc_id` were
ever NULL.

**Measured.** Narrowing `radar_agrifood` to a single territory and one
keyword that matches nothing reduced its pool from 127 articles to none.
The change stream fired a single-user recompute, which removed **exactly
its 127 rows** and left **37** behind — precisely `radar_electronics`
(21) plus `radar_pharma` (16), whose articles were still referenced.
Restoring the profile returned the gold layer to 164 articles with zero
orphans.

**When the sweep runs.** At the end of `recompute_all()` and of
`recompute_user()`. Because the MongoDB change stream calls the latter,
editing preferences on the dashboard cleans up after itself within
seconds; and because the documented shutdown sequence ends in a
recompute, `trim seed` leaves no residue either.

The same sweep runs in the PySpark path, inside the publish transaction
and after `user_articles` has been rebuilt, so both paths leave the same
state.

## How event cards are ordered

Cards are ordered by **the timestamp of their oldest article, most
recent first** — so the story that *started* most recently leads. The
key answers "when did this begin?", which for supply-chain risk
distinguishes a situation that emerged this morning from one that has
been developing for a fortnight.

The oldest article is taken across a card's **whole** article list, not
the three shown before it is opened, so the ordering does not shift with
the preview length.

This needs a per-**article** timestamp, which the gold layer originally
lacked: `event_date` comes from the event and is therefore identical for
every article on a card. `articles.mention_time` carries silver's
`MentionTimeDate` across to make the ordering possible. Rows written
before that column existed hold `NULL` until the next recompute refills
them, and cards with no timestamp at all sort last rather than breaking
the comparison.

Ordering is applied in the backend, in both `get_events_for_user()` and
`get_events_by_ids()`, which between them feed the Radar View, Archive,
Needs action and Looking out for developments pages — the frontend sorts
nothing.

*Previously* the order was `global_event_id` ascending: GDELT's internal
allocation order, arbitrary to a reader. The confidence and tone keys
that used to sit beside it still order the articles **within** a card,
which is why the top article is the highest-confidence one.

## Why the gold layer is keyed on (article, event)

The gold layer is normalised, and its grain is a **mention** — an
(article, event) pair — not an article. `articles` holds one row per
`(doc_id, global_event_id)`, and `user_articles` is a join table of
`(user_id, doc_id, global_event_id)`. Adding a user to a mention adds
one narrow row, never a copy of the content. As for the multiple rows
with the same article, each will have a different score on some values,
like the `Confidence` score, based on the event it is paired with.

**It cannot be one row per article**, because one URL is routinely a
mention of many GDELT events. Measured on the shipped seed: **27,132 of
52,359 distinct URLs (51.8%) appear under more than one
`GLOBALEVENTID`**, one of them under 64; headlines behave the same way,
53.8% spanning several events. Keying on `doc_id` alone kept one of
those pairs and discarded the rest, so an article belonging to six cards
appeared on one, chosen arbitrarily.

This matches silver exactly: `gdelt_mentions` is sorted on
`(GLOBALEVENTID, MentionIdentifier)` for the same reason. Gold used to
be narrower than its own source, and that mismatch was where the loss
happened.

## Deduplication, in full

Duplicates are eliminated at four distinct points, because they arise
for four distinct reasons.

1.  **Re-ingested slices.** Both silver tables are `ReplacingMergeTree`.
    `gdelt_events` is keyed on `GLOBALEVENTID` with `DATEADDED` as the
    version, so the most recent copy of an event survives.
    `gdelt_mentions` is keyed on `(GLOBALEVENTID, MentionIdentifier)`
    with `enriched` as the version, so an enriched row supersedes an
    unenriched one. All readers query with `FINAL`, which collapses
    duplicates at query time rather than waiting for a background merge.
2.  **The same (URL, event) pair seen twice.** When gold rows are
    constructed the pair is deduplicated —
    `dropDuplicates(["doc_id", "global_event_id"])` — and **only** the
    pair. Collapsing on the URL alone would discard the majority of
    legitimate rows (see the section above), so two mentions may be
    merged only when they share an event id as well as a URL.
3.  **Syndicated stories, on the READ path only.** The same report is
    republished under different URLs, producing different keys but an
    identical headline, so a card should list it once. That is done by
    the serving layer when it builds each card
    (`postgres_store._dedupe_by_title`, case-insensitive, whitespace
    collapsed) — which is the only place it is meaningful, since "one
    headline per card" is a property of a card. It used to *also* run in
    the write path, and that was a bug: it ran on the shared catalogue
    **before** any user's filter, and two rows sharing an event and a
    headline can differ in URL and keywords — exactly the fields the
    keyword predicate reads. Whichever row survived therefore decided
    whether a user matched at all, which showed up as identical runs
    producing different totals. Removing it made consecutive recomputes
    reproducible.
4.  **The `user_articles` primary key.** Rows are deduplicated on the
    whole key, `(user_id, doc_id, global_event_id)`, before insertion. A
    URL reachable through several events is deliberately **several
    rows** — one per card the user sees it on.

## Watermarking: how the pipeline knows it is making progress {#watermarking-how-the-pipeline-knows-it-is-making-progress}

New data is expected every 15 minutes. The question a watermark answers
is what should happen when a particular 15-minute point **does not**
arrive, or arrives and cannot be processed. Waiting for it indefinitely
is the failure this section exists to prevent.

**Progress is measured in event time.** GDELT names every file after its
own 15-minute slice (`20260810121500`), and that identifier — not the
time the file happened to be handled — is what the pipeline records.
Slice ids can be compared and ordered, so a gap in the feed is
detectable; a URL alone cannot be.

**A late slice cannot drag the watermark back.** The watermark is
`max(DATEADDED)` over the rows *currently present*, and a maximum
ignores every value below it. A slice that arrives an hour late is
simply absorbed: it is inserted, the maximum does not change, and no
ground already covered is revisited.

**So how can it move backwards at all?** Only by rows being **removed**.
Nothing about arrival order can do it — the max is a property of what is
in the store, not of what arrived when. Four things delete from silver,
and every one of them is deliberate:

| Cause | What it does |
|----|----|
| `silver_snapshot.sh trim` | deletes everything published after a given slice |
| `silver_snapshot.sh wipe` | empties both tables |
| `silver_snapshot.sh recreate` | drops the tables to apply a changed schema |
| `docker compose down -v` | destroys the volume; the seed is then restored, and the seed ends a month earlier than live data |

The retention job is the one deletion that **cannot** cause it: it
removes the *oldest* events, and the watermark tracks the *newest*.

**A backwards move is now adopted, not refused.** This used to be logged
and otherwise ignored, reasoning that rebuilding gold from a shorter
history than it already reflected would be a regression. That was wrong
twice over:

- Gold describing events that silver no longer holds is not a richer
  gold, it is an **inconsistent** one. Gold mirrors silver; if silver
  shrank, gold must shrink.
- Refusing left the trigger's in-memory `last` latched at the old
  high-water mark **forever**. `last_advance` therefore never reset
  either, so the 45-minute staleness reporter fired on a pipeline that
  was working perfectly, and the dashboard showed "technical
  difficulties" until someone restarted the container.

The loop now treats a lower value as what it is — a real change to
silver — and runs the same `recompute_all()` a forwards move would. Only
the log line differs. Measured 2026-08-16: a trim was detected **30 s**
later and gold was consistent again **120 s** after that, with
`status=OK` throughout and no restart.

This is why nothing needs to be remembered about the ordering of
`restore` and the pipeline any more. Previously, resetting silver under
a running processing container stranded it; now it repairs itself.

**Lateness is bounded, and failure is bounded with it.** A slice that
cannot be processed is retried three times and then moved to
`/data/dead_letter/<slice>/`. This matters more than it sounds: slices
are handled oldest-first and one at a time, so before this bound existed
a single malformed file was retried every few seconds **forever**, and
every later slice queued behind it — the pipeline stopped without ever
reporting an error. The same bound applies in the validation layer,
where a failing pair would otherwise block parsing from publishing at
all, since parsing waits for `latest_files` to drain.

Nothing is deleted by any of this. An abandoned slice is **moved**, so
it remains available for inspection, and can be replayed by copying it
back or loaded deliberately with `bootstrap/bulk_load.py`.

**Silence is reported rather than assumed to be health.** Four checks
cover the four ways the pipeline can go quiet:

| Check | Where | Threshold | What it catches |
|----|----|----|----|
| No new files in `latest_files` | validation | `STALE_LIMIT_SECONDS` 35 min | parsing or ingestion has stopped |
| `latest_files` never drains | parsing | `BACKPRESSURE_MAX_WAIT` 30 min | validation has stalled; publishing is blocked |
| Silver watermark not advancing | processing | `SILVER_STALE_SECONDS` 45 min | nothing is reaching the store; gold is frozen |
| Gold older than the clock allows | serving backend | `PIPELINE_STALE_SECONDS` 60 min | the processing layer itself is down, so nobody is left to report |

The last of these is the reason the backend does not simply trust the
stored status. The processing layer writes `ERROR` when it *notices*
silver has stopped advancing — but if that layer is the thing that died,
it writes nothing at all, and the last row it wrote says `OK` for as
long as PostgreSQL keeps answering. Comparing the timestamp against the
clock is what catches a pipeline with nobody left to report on it.

### Why these thresholds are the right size

Every threshold is derived from the **15-minute release cadence**, and
each sits in the same band: long enough that ordinary jitter cannot trip
it, short enough that a real stall is noticed within the hour.

| Bound | Value | Reasoning |
|----|----|----|
| `SLICE_RETRIEVAL_DEADLINE` | 600 s | Two thirds of one release interval. A file still absent after 10 minutes is very unlikely to appear, and continuing to wait would delay the *next* slice — the deadline must be shorter than the cadence or slices would queue. |
| `RETRY_TICK` | 60 s | Gives 10 attempts inside the deadline: frequent enough to catch a file published a minute late, rare enough not to hammer GDELT. |
| `ENRICH_TIMEOUT_SECONDS` | 600 s | The single largest cost in a cycle, and deliberately capped below the cadence so validation can never take longer to process a slice than GDELT takes to publish the next one. |
| `CLICKHOUSE_OP_TIMEOUT` | 120 s | Bounds every store operation, so a cycle is enrichment (≤ 600 s) plus a bounded number of bounded operations — comfortably inside 15 minutes. Measured, a live slice costs about a minute end to end, roughly 15× headroom. |
| `BACKPRESSURE_MAX_WAIT` | 30 min | Two missed releases. One slow slice is normal; two consecutive intervals with the consumer never draining is not. |
| `STALE_LIMIT_SECONDS` | 35 min | Two missed releases plus a margin for a slice that arrives late. Set fractionally above the 30-minute back-pressure bound so the *upstream* silence is reported by whichever layer actually observes it first. |
| `SILVER_STALE_SECONDS` | 45 min | Three missed releases. Sits above the two upstream checks on purpose: if parsing or validation is going to report the problem, it should do so before processing escalates it. |
| `PIPELINE_STALE_SECONDS` | 60 min | Four missed releases, and the last line of defence. Deliberately the loosest, because it is the only check that fires when *every* other reporter is dead, and a user-visible "your briefing is stale" banner should be certain rather than twitchy. |

The ordering is the point: **30 → 35 → 45 → 60 minutes**, escalating
outward from the layer closest to the problem to the one furthest from
it. Whichever component is still alive reports first, and the backend's
clock comparison catches the case where none of them are.

**Bounded attempts, not bounded time, for processing.** Retrieval has a
wall-clock deadline because a file that has not been published will
never be published. *Processing* is bounded by **attempt count** instead
(`MAX_SLICE_ATTEMPTS` and `MAX_PAIR_ATTEMPTS`, both 3). A wall-clock
limit there would discard slices that were merely slow — a transient
ClickHouse pause would start throwing away good data — whereas three
failures of the same file is evidence about the file itself.

**Gaps are stated, not silently back-filled.** The poller fetches
whatever `lastupdate.txt` currently points at, so slices published while
it was stopped are missed. It now names them in the log instead of
passing over them. Back-filling them automatically is deliberately *not*
done: a 15-minute poller quietly reloading hours of history is exactly
the unbounded lateness the rest of this design rules out.
`bootstrap/bulk_load.py` exists to load a known period on purpose.

## Recovering a dead Spark session

### What a Spark session actually is, here

PySpark is not a Python implementation of Spark. Spark is a **Java**
program, and `pyspark` is a remote control for it. When the processing
layer builds a session, three things exist:

1.  **The Python process** — the resident FastAPI service, which holds
    the triggers.
2.  **A separate Java process (the JVM)** — a child process where the
    real work happens. It is a genuinely separate operating-system
    process with its own process id, and it can die independently of the
    Python process that started it.
3.  **A socket between them**, the *py4j gateway*. Every DataFrame call
    is a message over that socket.

`SparkSession` is therefore a **handle**, not the engine: a Python
object holding the address of a JVM that is expected to be listening.

Two consequences follow, and the whole of this section is about them.

### Why the JVM dies

In testing mode `SPARK_MASTER=local[*]`, so the JVM runs **inside the
processing container** rather than on a separate worker machine. It is
the largest single memory consumer in the stack (\~1.7 GiB). When the
host runs short of memory, the Linux kernel's out-of-memory killer picks
the largest process and terminates it with `SIGKILL` — which cannot be
caught, blocked, or cleaned up after. The JVM simply ceases to exist,
mid-call, with no exception and no shutdown.

Observed 2026-08-16 at 19:44:32.

### Why it did not recover, and why that was hard to see

`SparkSession.builder…getOrCreate()` reads its name literally: it
returns the existing session if one was ever created. It does **not**
check that the JVM behind that session is still alive. So every
recompute after 19:44 was handed the same dead handle and failed in
about 40 milliseconds:

```         
19:43:33  EOFError                                     ← the socket drops
19:44:32  Py4JNetworkError: Answer from Java side is empty
19:44:32  SparkSession$ does not exist in the JVM
19:45:32  [Errno 111] Connection refused                ← and every 60 s after
```

The trigger cannot distinguish "Spark is broken" from "this recompute
failed", so it retried the same corpse every minute for forty minutes.
**Ingestion, parsing and validation were all healthy throughout** —
silver kept advancing normally, and only gold was frozen, which is
exactly the shape of failure that is invisible without the watermark
shown on the dashboard.

### The fix, in three parts

Each part was necessary, and the first two attempts were wrong in
instructive ways.

**1. Probe before use.** `_spark()` now calls `sc().isStopped()` before
handing the session back. The point is not the answer but the round
trip: the call has to cross the py4j socket, so a dead JVM raises
instead of returning a stale value. Any exception means unusable,
whatever the cause.

**2. Clear the gateway, not just the session.** The first attempt
cleared `SparkSession._instantiatedSession`, `_activeSession` and
`SparkContext._active_spark_context` — and still failed, identically,
for another nine minutes. The reason is one line inside PySpark:

``` python
if not SparkContext._gateway:
    SparkContext._gateway = gateway or launch_gateway(conf)
```

`_gateway` is a **process-global**, and a new JVM is launched only when
it is falsy. Leaving the dead gateway in place meant the "new" session
reattached to the same dead socket. `SparkContext._gateway` and `._jvm`
must be cleared too.

**3. Do not call `stop()` on a dead JVM.** `stop()` tries to talk to the
JVM. When there is nothing listening it does not raise — it **blocks**
on the socket, so `except` does not help. Since the probe has already
established the JVM is gone, the recovery path skips it. This is the
likely cause of an unexplained 11-minute stall in the trigger loop
during testing: that thread *is* the watermark trigger, so blocking it
stops the pipeline.

**4. Rebuild the UDF too.** Parts 1–3 shipped, and the failure came back
the next night: gold frozen for ten hours while silver stayed current,
same `[Errno 111] Connection refused`, and this time a *live* JVM in the
container with two zombies beside it. The session had been rebuilt
correctly; something else was still addressing the corpse.

`doc_id_udf` was a module-level constant, `doc_id_udf = F.udf(...)`,
created once at import. A UDF lazily creates a Java counterpart on first
use and caches it, and that cached handle belongs to whichever JVM was
running then. So the recompute got a healthy session and died one line
later:

```         
build_catalogue()  .withColumn("doc_id", doc_id_udf(...))
  -> udf.py:401    jPythonUDF = judf.apply(...)
    -> py4j        _create_new_connection()  ->  [Errno 111] Connection refused
```

It is now built fresh per recompute, against the current session, so it
cannot outlive a JVM. Resetting the private `_judf_placeholder` would
also work; building it fresh uses only the public API and cannot go
stale by construction.

**The lesson, since it caught the same fix twice:** a dead JVM does not
hide in one place. Anything holding a Java handle — the session, the
gateway, a UDF, and any broadcast variable or accumulator added later —
has to be rebuilt with it. Cached Java references are the pattern to
look for, not the session specifically.

### Worked example

A recompute is due. `_spark()` probes the session it holds:

- **JVM alive** — `isStopped()` returns `False`, the session is
  returned, the recompute proceeds. This is every normal call, and it
  costs one round trip.
- **JVM killed by the OOM reaper** — the probe raises. The log says
  `the Spark JVM is gone; discarding the dead session and building a new one`.
  PySpark's session state *and* gateway are cleared, `getOrCreate()`
  finds nothing cached, `launch_gateway` starts a fresh JVM with a new
  process id, and the recompute runs on it.

Verified by killing the JVM with `SIGKILL` inside a single long-lived
process — which is the only way to reproduce it, since a fresh process
gets a fresh gateway and never exercises the bug at all. The test must
run a **full recompute**, not a toy query:

```         
STEP 1  full recompute (exercises doc_id_udf)
        ok in 139s: {'articles': 615, 'users': 3}      jvm pids: [38, 709]
STEP 2  SIGKILL                                        jvm pids: []
STEP 3  RECOVERED in 75s: {'articles': 615, 'users': 3}
        new jvm pids: [1071]   different JVM? True
```

The first version of this test used `range(5).count()` and **passed
while the pipeline was still broken**, because that query touches no
UDF. A recovery test has to run the thing that actually failed; anything
less measures the wrong path.

The live trigger loop then demonstrated the same thing unprompted: dead
session detected, new JVM built, and
`published — articles, user_articles and pipeline_status are live` —
with no container restart.

**This is recovery, not prevention.** The JVM can still be killed; see
[Memory allowance, layer by layer](#memory-allowance-layer-by-layer) for
the oversubscription that makes it possible. What changed is that the
pipeline no longer needs a human to notice.

### It matters at least as much in intended mode

The failure was found in testing mode, but nothing about the fix is
specific to it, and intended mode has *more* ways to reach the same
state, not fewer.

The driver JVM still lives in the processing container there — only the
executors move to `spark-worker`. So it can still be killed, and it can
now also lose its `SparkContext` for reasons that do not exist locally:
`spark-master` is a Swarm service with `restart_policy: condition: any`,
so it *will* restart at some point and drop the connections held by
every driver attached to it.

That case lands on the same probe. A `SparkContext` whose master has
gone away reports `isStopped() == True`, which is treated exactly like a
dead JVM — discard, clear the gateway, rebuild. Without it, a routine
`spark-master` restart would freeze the gold layer until someone
restarted the processing service by hand.

## Retries: nothing waits indefinitely

Every dependency that may be temporarily unavailable is retried rather
than allowed to block or fail permanently.

- **Validation → ClickHouse.** Table creation retries every five seconds
  until the cluster responds, tolerating Compose start-up ordering.
- **Processing → MongoDB.** The territory table is published from a
  background thread that retries every five seconds until the replica
  set has elected a primary. A single attempt would frequently lose that
  race and leave the picker empty.
- **Processing triggers.** Both the MongoDB change stream and the
  ClickHouse watermark poll run in supervised loops that log and
  continue rather than terminating.
- **Backend → PostgreSQL and MongoDB.** Both clients retry with
  exponential backoff, distinguishing transient failures (connection
  loss, timeout) from permanent ones (invalid SQL, authentication),
  which are re-raised immediately.
- **Ingestion → GDELT.** A 404 for a release that is still being
  published is retried three times at five-second intervals, since the
  file appears shortly afterwards.
- **Frontend → backend.** Every call is wrapped, and an unreachable
  backend produces an explanatory banner rather than an error page.
- **Bootstrap loader.** Waits for ClickHouse in the same five-second
  retry loop before loading.

Retrying is bounded where the thing being retried may never succeed. A
transient dependency is worth waiting for indefinitely; a malformed file
is not, because retrying it forever blocks every slice behind it.
Parsing and validation therefore give a slice three attempts and then
set it aside — see
[Watermarking](#watermarking-how-the-pipeline-knows-it-is-making-progress).

Fail-soft behaviour complements this. A failure to read tags leaves
events untagged rather than hiding them; a failure to read the pipeline
status is reported explicitly as a database outage rather than being
silently reported as healthy. One deliberate exception exists: a failure
to read a user profile is raised rather than substituted with an empty
default, because an empty profile is indistinguishable from a new user
and could otherwise be saved over a real one.

## Two naming oddities: `bitnamilegacy` and `radar-processing:latest`

### What Bitnami is doing here

Bitnami is not a component of this system — it is only the **publisher
of the Spark base image**. Four places use it, all Spark and nothing
else:

| File | Use |
|----|----|
| `4-processing/Dockerfile` | `FROM bitnamilegacy/spark:3.5` — the processing image |
| `4-processing/Dockerfile.spark` | the one-shot submitter, same base |
| `docker-compose.spark.yml` | manual batch run |
| `docker-stack.pipeline.yml` | `spark-master` and `spark-worker` in intended mode |

Apache publishes no image that ships Spark, a JDK and PySpark ready to
run, so this project uses a packaged one. What it buys: the JDK, the
Spark distribution under `/opt/bitnami/spark`, PySpark on the Python
path, and the `SPARK_MODE` environment convention that lets one image be
a master, a worker, or a driver.

**Why `bitnamilegacy/` and not `bitnami/`:** Bitnami moved its
pre-existing images to the `bitnamilegacy` namespace, and
`bitnami/spark:3.5` no longer resolves at all — it fails with "failed to
resolve source metadata … not found". Same image, re-hosted, so the
layout and the `SPARK_MODE` convention are unchanged.

**It will not get updates.** The migration path is `apache/spark:3.5.x`,
the official image, which needs three changes this one does not: jars
live in `/opt/spark/jars`, there is no `SPARK_MODE` helper so master and
worker need explicit start commands, and it runs as UID 185.

### Why the processing image is tagged by hand

Compose names images `<project>-<service>`, where the project defaults
to the directory name — hence
`global-news-event-radar-for-geopolitical-and-supply-risk-ingestion` and
friends. The processing service overrides that:

``` yaml
image: radar-processing:latest
```

Two things need a **stable, predictable** name that does not depend on
what the checkout directory happens to be called:

- `4-processing/Dockerfile.spark` builds `FROM` it, so the one-shot
  Spark submitter reuses the same base, jars and code instead of
  duplicating a 1.5 GB image build;
- `docker-stack.pipeline.yml` refers to
  `${REGISTRY:-localhost:5000}/radar-processing:latest`, because Swarm
  pulls images from a registry rather than building locally on each
  machine.

The other five services are only ever built and run in place, so the
generated name is fine for them. The inconsistency is deliberate, and
only this one image needs it.

## Testing each layer on its own

The dashboard is the *last* thing in a chain of six, so "the dashboard
is wrong" localises nothing. Each layer can be checked directly, and
doing so in order turns a vague fault into a specific one in about a
minute.

### Start here: two numbers

Almost every question is answered by comparing what silver holds against
what gold was built from.

``` bash
docker exec pipeline_clickhouse_s1r1 clickhouse-client \
  --query "SELECT count(), max(DATEADDED) FROM gdelt_events FINAL"
```

``` bash
docker exec pipeline_postgres psql -U radar -d radar \
  -c "SELECT status, timestamp_of_last_update, silver_watermark FROM pipeline_status"
```

| silver | gold's `silver_watermark` | Where the fault is |
|----|----|----|
| current | current | healthy — look at the browser, not the pipeline |
| current | **behind** | **layer 4 (processing)**. Layers 1–3 are fine; skip them |
| **behind** | behind (matches) | layers 1–3, or no network |
| current | behind **and** `status=ERROR` | processing has been stuck \>45 min |

"Current" means within a slice or two of `date -u`, since GDELT
publishes every 15 minutes and our poller is not in phase with it.

### What is reachable from where

Only some ports are published to the host. This trips people up: the
processing layer has an HTTP API but **no published port**, so it can
only be reached from inside the container.

| Container                  | From the host         | From inside               |
|----------------------------|-----------------------|---------------------------|
| `pipeline_ingestion`       | —                     | logs only                 |
| `pipeline_parsing`         | —                     | logs, `/data`             |
| `pipeline_validation`      | —                     | logs, `/data`             |
| `pipeline_processing`      | **nothing published** | `curl localhost:8001/...` |
| `pipeline_clickhouse_s1r1` | `:9000`               | `clickhouse-client`       |
| `pipeline_postgres`        | `:5432`               | `psql`                    |
| `pipeline_mongo1`          | `:27017`              | `mongosh`                 |
| `radar-backend`            | `:8000`               | —                         |
| `radar-frontend`           | `:8501`               | —                         |

### Layer 1 — ingestion

Does it reach GDELT, and is it releasing slices?

``` bash
docker logs --tail 20 pipeline_ingestion
```

`[RELEASE] slice <id> -> parsing` every \~15 minutes is healthy.
`[SKIP]` means GDELT has published nothing new, which is also healthy.
Test connectivity alone — note this uses `python3`, because the
ingestion and parsing images are slim and have **no `curl`** (only the
processing container, built on the Spark image, does):

``` bash
docker exec pipeline_ingestion python3 -c "import urllib.request as u;print(u.urlopen('http://data.gdeltproject.org/gdeltv2/lastupdate.txt',timeout=20).readline().decode().split()[-1])"
```

Compare that slice id with `date -u` — GDELT names slices on the quarter
hour and publishes slightly early, so it can read a couple of minutes
ahead of the clock.

### Layers 2 and 3 — parsing and validation

These communicate through the shared volume, so the hand-off is directly
visible:

``` bash
docker exec pipeline_parsing sh -c 'for d in /data/*/; do printf "%-22s %s\n" "$d" "$(ls -1 "$d" | wc -l)"; done'
```

- `latest_files` — parsing's output, waiting for validation. Should be
  **near zero**. A number that only grows means validation has stalled
  and parsing is about to hit `BACKPRESSURE_MAX_WAIT`.
- `dead_letter` — slices that failed three times and were set aside.
  **Not empty is worth investigating**: nothing is lost (they can be
  replayed with `bootstrap/bulk_load.py`), but each entry is a slice
  that never reached silver.
- `state` — `last_seen.json` is the poller's position, `retention.json`
  the last retention run.

The validation layer also writes a status file that the processing layer
mirrors into `pipeline_status.status`:

``` bash
docker exec pipeline_processing cat /data/status/pipeline_status.json
```

### Layer 4 — processing

This is the layer that fails invisibly, because silver keeps advancing
while gold freezes. It has an API, reachable only from inside:

``` bash
docker exec pipeline_processing curl -s localhost:8001/health
```

**To force a rebuild and see the real error**, use `/process-all`. This
matters more than it looks: the trigger logs only an exception
*message*, never a traceback, and running
`docker exec pipeline_processing python3 -c "import main; main.recompute_all()"`
starts a **fresh process with a fresh Spark JVM**, which often succeeds
and tells you nothing. `/process-all` runs inside the resident process,
so it reproduces the actual state and logs a full traceback:

``` bash
docker exec pipeline_processing sh -c 'curl -s -X POST localhost:8001/process-all -m 300'
```

``` bash
docker logs --tail 120 pipeline_processing 2>&1 | grep -A28 "process-all failed"
```

Two tells when reading those logs:

- a recompute that fails in **\~40 ms** never reached Spark — the
  session is dead;
- one that fails in **1–2 s** got into Spark and died on a stale Java
  handle.

And check the JVM itself:

``` bash
docker exec pipeline_processing pgrep -af java
```

`<defunct>` entries are JVMs that died and were replaced — recovery
working, but also evidence something is killing them (see the memory
table).

### Layer 5 — backend and frontend

The backend is the only layer with published HTTP, so it needs no
`docker exec`:

``` bash
curl -s localhost:8000/health
```

``` bash
curl -s localhost:8000/system/status
```

``` bash
curl -s "localhost:8000/users/radar_agrifood/events?max_age_days=90" | head -c 400
```

If that returns events, the entire pipeline behind it is working and any
problem is in the frontend. That is the single most useful test in this
file: it separates "the data is wrong" from "the page is wrong" in one
command.

``` bash
curl -s -o /dev/null -w '%{http_code}\n' localhost:8501
```

### Restarting one layer

Rebuild only what changed — the stores keep running and no data is
touched:

``` bash
docker compose --env-file .env.testing up -d --build processing
```

`backend` is in the **root** compose file; only `frontend` is in
`5-serving/docker-compose.serving.yml`. Code is baked in with `COPY`,
and neither uvicorn nor Streamlit runs with reload, so a `restart` alone
never picks up a code change — it must be `--build`.

## Inspecting why an article was selected

Because the filters are narrow and applied in sequence, "why is this
pool so small?" is not answerable by reading the code alone — it depends
on which rule each surviving article actually satisfied.
[`dev_tools_for_filter_diagnostics/`](dev_tools_for_filter_diagnostics/)
answers it by re-deriving the decision for every article in gold:

``` bash
docker compose -f docker-compose.diagnostics.yml run --rm diagnostics
```

**In intended mode, run this from any machine on `pipeline_network`,
with `.env.intended` sourced.** It reaches the stores by their Docker
service names, which the overlay resolves swarm-wide, so no machine is
privileged — but the container must be attached to that network, and
`docker compose` will only attach it if the network exists on the
machine you run from.

Two related tools have stricter homes: `docker-compose.bootstrap.yml`
needs the GDELT ZIPs on the machine it runs on, and
`docker-compose.spark.yml` must run on the **pipeline** machine, because
it mounts that machine's `shared_data` volume to read the status file.
Spark also reads ClickHouse over JDBC on the HTTP port 8123, which is
never published to any host — another reason it has to be inside the
network rather than merely able to reach a published port.

It reads all three stores read-only, writes no store, and produces
`gold_provenance.csv` — one row per article per user, naming the parsing
criterion that admitted it, the territory code and code system that
matched, and the keyword and field that selected it. It imports the
pipeline's own filter functions rather than restating them, so it cannot
fall out of step with the filters in force.

## What is retrieved, and what is filtered at each stage

**Every 15 minutes**, ingestion reads `lastupdate.txt` and retrieves the
current events and mentions archives. A representative slice contains
approximately 979 events and 3,222 mentions.

### Stage 1 — retrieval, and why the hand-off is atomic

Ingestion assembles a slice in a **staging directory** and moves it into
the hand-off directory only when one of two things is true: the slice is
**complete**, or its **retrieval deadline** has expired
(`SLICE_RETRIEVAL_DEADLINE`, 600 s measured from the slice's own
timestamp). While a file is missing and the deadline has not passed,
only *that* file is re-attempted, every `RETRY_TICK` (60 s), addressed
directly by its slice timestamp rather than by whatever `lastupdate.txt`
currently advertises.

Before staging existed, each CSV was written to the hand-off directory
the instant it was extracted, so a slice could be published
half-finished while its partner was still downloading. Staging closes
that window: files are moved with `os.replace()`, **mentions last**,
mirroring the ordering the parsing layer already relies on.

The consequence is the property the rest of the pipeline is built on:
**anything in the hand-off directory is final**. A lone file there is
not "half a slice still arriving" — it is all there will ever be. That
is what allows the layers below to act on a partial slice instead of
waiting for a partner that will never come, and a partial slice is still
useful:

- **events alone** update the store, because `gdelt_events` is a
  `ReplacingMergeTree` keyed on `GLOBALEVENTID` with `DATEADDED` as the
  version, so a re-published event supersedes the stored copy;
- **mentions alone** attach to events already stored, because the
  referential-integrity check resolves against *this events file **or**
  the store*.

The poll cycle then sleeps the **remainder** of the 15 minutes rather
than a fixed 15, so a cycle that spent time retrying does not push the
next one out to T+25 and drift further every time.

### Stage 2 — bronze to silver: two filters, applied in different layers

**The relevance filter (parsing).** `parser.passes_filter()` requires
`F1 AND (F2 OR F3) AND has_source_url`:

|   | Criterion | Backed by |
|----|----|----|
| **F1** | the 4-digit `EventCode` is a supply-chain-relevant CAMEO code, **or** the 2-digit `EventRootCode` is one of the relevant macro categories | 32 event codes; 6 root codes (14 protest, 15 force, 17 coerce, 18 assault, 19 fight, 20 mass violence) |
| **F2** | an actor carries a relevant type or known-group code | 5 type codes, 8 known groups |
| **F3** | a supply-chain word appears in either actor name or in the source URL | 35 keywords (*port, shipping, freight, customs, tariff, …*) |

F1 is mandatory, so an article about shipping that is not a relevant
*event* is still discarded. Roughly **31 of 979** events survive per
slice. This filter is independent of users: it decides what is worth
storing at all.

**The referential-integrity filter (validation).** A mention is kept
only if its `GLOBALEVENTID` exists **in the events file it arrived with,
or already in ClickHouse**. The store lookup is what makes a
mentions-only slice viable, and it also lets a mention attach to an
event detected hours earlier. Unmatched mentions are dropped and the
file on disk is rewritten without them.

**Enrichment sits between the two**, and only touches the survivors —
the referential filter runs first, so no rejected mention is ever
scraped. Newspaper3k fetches each unique URL once across 8 threads,
extracting `article_title` and, via NLTK, `article_keywords`. The whole
step is bounded by `ENRICH_TIMEOUT_SECONDS` (600 s **per slice**, not
per URL); anything unscraped when the budget expires is stored with
empty fields and `enriched = 0`. A mention counts as enriched when a
**title** was obtained.

### Stage 3 — silver to gold: the per-user filter

Two conditions, combined with **and** — an article must satisfy both:

**Territory.** An event qualifies if *either* code system matches: CAMEO
codes against the actor columns (`Actor1/Actor2CountryCode`), or FIPS
codes against the location columns
(`ActionGeo_`/`Actor1Geo_`/`Actor2Geo_CountryCode`). Keeping both is
deliberate — measured on the current gold, 84 articles matched on
location only, 17 on actor only and 63 on both.

**Keywords.** Every mention is searched in **all three** text fields —
the URL, the article title and the extracted keywords — regardless of
its `enriched` flag. A keyword matches when **all of its tokens** are
present, a token being a single word: `silicon wafers` needs both
*silicon* and *wafers*, but not adjacently. Singular and plural collapse
onto one another, so `chip` and `chips` are the same word. Matching is
whole-word, so `chip` does not match `chipotle`.

Two earlier rules were removed because they were measurably wrong.
Keywords were matched as **contiguous phrases**, so `silicon wafers`
matched 0 of 99,175 enriched mentions. And each row was routed to
exactly **one** field depending on `enriched`, which meant that of those
99,175 rows, the 170 whose URL contained a supply-chain term were never
checked against it — enrichment was actively destroying matches.

Because both filters are narrow, a single 15-minute slice frequently
matches nothing for a given user. That is expected behaviour, not a
fault.

Two earlier rules were removed because they were measurably wrong.
Keywords were matched as **contiguous phrases**, so `silicon wafers`
matched 0 of 99,175 enriched mentions — a headline says "chip firm buys
wafer plant", never the procurement phrase verbatim. And each row was
routed to exactly **one** field depending on its `enriched` flag, which
meant that of the 99,175 enriched rows, the 170 whose URL contained a
supply-chain term were never checked against it — enrichment was
actively destroying matches.

**Tables retained on failure.** The PySpark path writes to
`articles_stage` and `user_articles_stage`, which it creates at the
start of each run and drops after a successful publication. They are
deliberately **not** dropped when publication fails, so the staged
result remains available for inspection; the next run drops and
recreates them. Each carries a table comment recording its purpose and
that it may safely be dropped. These are the only database objects left
behind on purpose: unreferenced rows inside `articles` itself are
removed automatically, as described under [Orphaned gold
rows](#orphaned-gold-rows-and-why-they-can-be-removed-safely).

## Data persistence

All silver and gold data, and all user-linked data — profiles, keyword
sets, and the archived, needs-action and monitoring tags — reside in
**named Docker volumes**. `docker compose down` removes containers and
networks but does not remove volumes, so this data persists across
restarts. Only `docker compose down -v` deletes it, irreversibly.

------------------------------------------------------------------------

## Contributing

Each layer owns a single responsibility; per-layer dependencies belong
in that layer's `requirements.txt`.
