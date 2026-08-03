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
bash download-backfill.sh                           # optional 30-day backfill — see the note below
docker compose up -d --build                        # layers 1–4 + backend
```

> **`--build` matters.** Compose reuses an existing image and does **not** rebuild
> because you edited a file. Any change to Python code (or `.streamlit/config.toml`)
> only reaches a container when you pass `--build`. Files that are *mounted* rather
> than copied in — the compose files, `clickhouse/*.xml`, `oracle-init/*.sql` — are
> read live and need no rebuild.

The stores tier creates the `pipeline_network` and runs its two one-time, idempotent setup steps (Mongo `rs.initiate`, and the Oracle gold schema). The pipeline tier attaches to that network, polls GDELT immediately and every 15 min after, and exposes the backend on port **8000**.

**Each user** — on their own machine:

``` bash
BACKEND_URL=http://<operator-host>:8000 \
  docker compose -f 5-serving/docker-compose.serving.yml up
```

The frontend (port **8501**) talks to the operator's backend over HTTP; it ships no pipeline code and no data.

**What `BACKEND_URL` is.** The frontend holds no database credentials and never
touches ClickHouse, Mongo or Oracle itself — every piece of data it shows comes
from the serving backend's HTTP API (`/users/…/events`, `/territories`, …).
`BACKEND_URL` is simply the address it calls. Because the frontend runs on each
user's own machine while the backend runs on the operator's, that address has to
be the **operator machine's host and the backend's published port 8000**. Set it
to whatever the user's browser-side container can reach:

| Situation | Value |
|----|----|
| Frontend and backend on the same machine | `http://host.docker.internal:8000` (the default — no need to set it) |
| Frontend on a user's machine, backend on the operator's | `http://<operator-host>:8000` |

If it points somewhere unreachable, the dashboard shows *"The backend is
unreachable"* rather than any data.

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
docker compose -f docker-compose.stores.yml up -d                    # stores (once)
bash download-backfill.sh                                            # optional backfill — see note
docker compose up -d --build                                         # pipeline + backend
docker compose -f 5-serving/docker-compose.serving.yml up --build    # frontend
```

Give the stores a few minutes on their **first** run: Oracle builds its database
files from scratch and only then runs the gold schema. That happens once per
machine, per volume — later starts are fast. The first events reach the dashboard
roughly 15–25 minutes after the pipeline starts (poll → parse → validate +
enrich → ClickHouse → trigger → Oracle).

The frontend defaults `BACKEND_URL` to `http://host.docker.internal:8000`, which resolves to the host (and the backend it published on 8000) on macOS, Windows, and — via the `extra_hosts` mapping in the serving compose — Linux. Open the dashboard at [**http://localhost:8501**](http://localhost:8501){.uri}.

------------------------------------------------------------------------

## Shutting everything down (keeping all data)

Stop the tiers in the reverse of the order they were started — frontend, then
pipeline, then stores:

``` bash
docker compose -f 5-serving/docker-compose.serving.yml down   # frontend
docker compose down                                           # pipeline + backend
docker compose -f docker-compose.stores.yml down              # stores
```

`down` removes the **containers and the network**. It does **not** touch named
volumes, so every piece of durable data survives and is picked up again on the
next start:

| Data | Volume | Survives `down`? |
|----|----|----|
| User profiles — territories, keywords | `mongo1/2/3_data` (`radar.users`) | ✅ |
| Per-user tags — archived / needs action / monitoring | `mongo1/2/3_data` (`radar.tags`) | ✅ |
| Gold — `articles`, `user_articles` | `oracle_data` | ✅ |
| Silver — `gdelt_events`, `gdelt_mentions` | `ch_*_data` | ✅ |
| In-flight raw + parsed slices | `shared_data` | ✅ (and disposable anyway) |

> ⚠️ **Never add `-v`.** `docker compose … down -v` deletes the volumes as well,
> which **permanently destroys** every user profile, every tag and the whole
> silver and gold history. Only the most recent 15-minute GDELT slice could be
> re-polled; everything older is gone for good.

Adding `--build` on the next start-up is safe: it rebuilds images from your
source, and never touches volumes.

------------------------------------------------------------------------

## One-time setup (automated — nothing manual)

Both steps belong to the **stores** tier, so the pipeline never re-runs them, and both are idempotent:

- **Mongo replica set** — the `mongo-init` service runs `rs.initiate(rs0)` guarded by `rs.status()`, so it is a no-op once the set exists.
- **Oracle gold schema** — `oracle-init/01_schema.sql` runs once, as SYSDBA, at first database creation. The `radar` user comes from the image's `APP_USER`; the service (PDB) is `FREEPDB1`. `articles` is keyed on `doc_id RAW(32)` = SHA-256 of the article URL, since the URL itself is too long to index as a primary key.
- The processing layer seeds the territory table into Mongo on startup; the frontend fetches it from the backend (`GET /territories`).

### About the optional backfill

`download-backfill.sh` fetches a pre-built 30-day archive of GDELT ZIPs from the
project's GitHub Release, starts the ingestion container and copies them into
`/data/raw/zip/` on the shared volume. Two things to know:

- Run it with `bash download-backfill.sh` — the file is not marked executable, so
  `./download-backfill.sh` fails with *permission denied* (or `chmod +x` it once).
- **The copied ZIPs still have to be unpacked into `/data/raw/csv/` before the
  parsing layer can see them.** Only the backfill code path extracts them, and the
  ingestion container runs in `MODE=poller` by default, which just fetches the
  newest 15-minute slice. To unpack what the script copied:

``` bash
docker compose run --rm -e MODE=backfill ingestion --days 30
```

  That re-walks the last 30 days, finds each ZIP already present and extracts it
  (it skips the download when the file exists). If the archive's 30 days no longer
  overlap "the last 30 days from today", the timestamps will not line up and it
  will fetch current files instead — so the backfill is worth doing soon after
  cloning, or not at all.

## Contributing

Each layer owns one responsibility; add per-layer dependencies to its `requirements.txt`.
