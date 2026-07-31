# Global News Event Radar — Geopolitical & Supply Risk

A five-layer pipeline that polls GDELT every 15 minutes, keeps supply-chain-relevant events, enriches and stores them, and serves each user a personalised briefing filtered by the **territories** and **supply-chain keywords** they registered. Python version: 3.11.

```         
1-ingestion → 2-parsing → 3-validation_and_storage → 4-processing → 5-serving (backend + frontend)
```

- **1-ingestion** — polls GDELT's `last15minutes` feed, drops raw events+mentions CSVs onto the shared volume.
- **2-parsing** — keeps supply-chain-relevant events, passes mentions through, publishes a slice to `latest_files`.
- **3-validation_and_storage** — referential-integrity check, Newspaper3k enrichment, writes the silver store (ClickHouse) and owns its schema + dedup.
- **4-processing** — per user, filters ClickHouse silver by territory codes + keywords and writes the Oracle gold (`articles`, `user_articles`, `pipeline_status`); also publishes the territory table to Mongo.
- **5-serving** — a **backend** (FastAPI, reads Oracle + Mongo) and a **frontend** (Streamlit dashboard).

### Stores

- **ClickHouse** — silver: `gdelt_events`/`gdelt_mentions`, 2 shards × 3 replicas + 3-node Keeper.
- **MongoDB** — replica set `rs0`: user profiles (`radar.users`) and the territory table (`radar.reference`).
- **Oracle** — gold sink the backend reads.

------------------------------------------------------------------------

## Deployment

The system is split into three independently-deployable tiers, each with its own lifecycle:

| Tier | What it is | Lifecycle / where it runs |
|----|----|----|
| **Stores** | ClickHouse + Keeper, Mongo `rs0`, Oracle (`docker-compose.stores.yml`) | Brought up **once** and left running. Owns every durable volume and the `pipeline_network`. |
| **Pipeline** | layers 1–4 + the serving **backend** (`docker-compose.yml`) | Disposable and replaceable, on the radar operator's machine(s). Owns no database. |
| **User frontend** | only the serving **frontend** (`5-serving/docker-compose.serving.yml`) | each user's own machine |

### A. Normal operation (multi-machine)

**Operator** — bring the stores up once, then the pipeline:

``` bash
git clone https://github.com/martinagobbi/Global-News-Event-Radar-for-Geopolitical-and-Supply-Risk
cd Global-News-Event-Radar-for-Geopolitical-and-Supply-Risk
docker compose -f docker-compose.stores.yml up -d   # once; long-lived
./download-backfill.sh                              # Load 30-day backfill so the dashboard has data immediately (optional)
docker compose up -d                                # layers 1–4 + backend
```

The stores tier creates the `pipeline_network` and runs its two one-time, idempotent setup steps (Mongo `rs.initiate`, and the Oracle gold schema). The pipeline tier attaches to that network, polls GDELT immediately and every 15 min after, and exposes the backend on port **8000**.

**Each user** — on their own machine:

``` bash
BACKEND_URL=http://<operator-host>:8000 \
  docker compose -f 5-serving/docker-compose.serving.yml up
```

The frontend (port **8501**) talks to the operator's backend over HTTP; it ships no pipeline code and no data.

#### Why this scales across machines

- **The stores are a separate tier with their own lifecycle.** ClickHouse, Mongo and Oracle live in `docker-compose.stores.yml` and are never created, re-initialised or deleted by the pipeline. A replacement pipeline machine simply connects to the *same* stores.
- **The stores are HA by design.** ClickHouse is 2 shards × 3 replicas + a Keeper ensemble; Mongo is a 3-node replica set. Deploy their replicas across hosts and the system survives a node loss.
- **The backend is stateless.** It only reads ClickHouse/Mongo/Oracle and holds no local data (the territory list it serves is read from Mongo, not from a local file), so you can run several backend replicas on several machines behind a load balancer.
- **The pipeline fails over by re-polling, not by sharing storage.** `shared_data` holds only *transient* scratch files (the in-flight raw and parsed slices) — it does **not** need to be shared or replicated across machines. If the active pipeline machine dies, a standby simply starts the operator stack with its own fresh local volume: at startup ingestion **polls the latest GDELT slice immediately**, so whatever the dead machine hadn't yet stored gets re-polled and flows through to ClickHouse. The persistent truth lives in the (HA) stores, never in `shared_data`.
- **Re-polling is idempotent.** Both silver tables dedup a re-ingested slice: `gdelt_events` is a `ReplacingMergeTree` keyed on `GLOBALEVENTID` (newest `DATEADDED` wins) and `gdelt_mentions` a `ReplacingMergeTree` keyed on `(GLOBALEVENTID, MentionIdentifier)` (the enriched row wins; readers use `FINAL`). So re-ingesting a slice the dead machine already stored collapses back to the same rows.

> Run the operator pipeline **active-passive** (one live instance at a time). Two live pipelines would both poll GDELT and double-ingest; the stores would dedup it away, but you'd be doing redundant work. There is no message broker (no Kafka) — the GDELT feed itself is the durable, re-pollable source.

### B. Run the whole thing locally (one machine)

To try everything on a single machine (e.g. cloned from GitHub):

``` bash
git clone https://github.com/martinagobbi/Global-News-Event-Radar-for-Geopolitical-and-Supply-Risk
cd Global-News-Event-Radar-for-Geopolitical-and-Supply-Risk
docker compose -f docker-compose.stores.yml up -d           # stores (once)
./download-backfill.sh                                      # Load 30-day backfill so the dashboard has data immediately (optional)
docker compose up -d                                        # pipeline + backend
docker compose -f 5-serving/docker-compose.serving.yml up   # frontend
```

The frontend defaults `BACKEND_URL` to `http://host.docker.internal:8000`, which resolves to the host (and the backend it published on 8000) on macOS, Windows, and — via the `extra_hosts` mapping in the serving compose — Linux. Open the dashboard at [**http://localhost:8501**](http://localhost:8501){.uri}.

------------------------------------------------------------------------

## One-time setup (automated — nothing manual)

Both steps belong to the **stores** tier, so the pipeline never re-runs them, and both are idempotent:

- **Mongo replica set** — the `mongo-init` service runs `rs.initiate(rs0)` guarded by `rs.status()`, so it is a no-op once the set exists.
- **Oracle gold schema** — `oracle-init/01_schema.sql` runs once, as SYSDBA, at first database creation. The `radar` user comes from the image's `APP_USER`; the service (PDB) is `FREEPDB1`. `articles` is keyed on `doc_id RAW(32)` = SHA-256 of the article URL, since the URL itself is too long to index as a primary key.
- The processing layer seeds the territory table into Mongo on startup; the frontend fetches it from the backend (`GET /territories`).

## Contributing

Each layer owns one responsibility; add per-layer dependencies to its `requirements.txt`.
