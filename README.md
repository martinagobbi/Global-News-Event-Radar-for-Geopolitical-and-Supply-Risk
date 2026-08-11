# Global News Event Radar — Geopolitical & Supply Risk

A five-layer pipeline that polls GDELT every 15 minutes, retains supply-chain-relevant events, enriches and stores them, and serves each user a personalised briefing filtered by the **territories** and **supply-chain keywords** they registered. Python version: 3.11.

```
1-ingestion → 2-parsing → 3-validation_and_storage → 4-processing → 5-serving (backend + frontend)
```

- **1-ingestion** — polls GDELT's `lastupdate.txt` feed and writes the raw events and mentions CSVs to the shared volume.
- **2-parsing** — applies the supply-chain relevance filter to events, passes mentions through unchanged, and publishes each slice to `latest_files`.
- **3-validation_and_storage** — enforces referential integrity, performs Newspaper3k enrichment, and owns the silver store (ClickHouse), including its schema and deduplication.
- **4-processing** — filters silver per user by territory codes and keywords, writes the Oracle gold (`articles`, `user_articles`, `pipeline_status`), and publishes the territory table to MongoDB.
- **5-serving** — a **backend** (FastAPI, reads Oracle and MongoDB) and a **frontend** (Streamlit dashboard).

### Stores

- **ClickHouse** — silver: `gdelt_events` / `gdelt_mentions`, 2 shards × 3 replicas plus a 3-node Keeper ensemble.
- **MongoDB** — replica set `rs0`: user profiles (`radar.users`), per-user tags (`radar.tags`) and the territory table (`radar.reference`).
- **Oracle** — the gold sink the serving backend reads.

------------------------------------------------------------------------

## System requirements

The Docker memory allocation must be raised in **both** modes — the default is too
small for either. Set it in **Docker Desktop → Settings → Resources → Memory limit**.

| Mode | Store containers | Measured usage, everything running | Set the limit to |
|----|----|----|----|
| **Testing** | 4 | ≈ 4.3 GB | **at least 5 GB** |
| **Intended**, all tiers on one host | 13 | ≈ 6.1 GB at idle alone | **at least 8 GB** |
| **Intended**, tiers on separate machines | 13 on the stores machine | ≈ 6.1 GB there; ≈ 0.3 GB on the pipeline machine | **8 GB on the stores machine** |

Below these thresholds the ClickHouse nodes are terminated under memory pressure
and restart in a loop, which appears as queries timing out or returning nothing
rather than as an obvious error.

Testing mode exists precisely because the thirteen-container topology does not fit
comfortably on a machine with 8 GB of physical RAM: the six ClickHouse servers
alone occupy roughly 4.6 GB.

------------------------------------------------------------------------

## Deployment

The system is divided into three independently deployable tiers, each with its own lifecycle:

| Tier | Contents | Lifecycle and location |
|----|----|----|
| **Stores** | ClickHouse and Keeper, MongoDB `rs0`, Oracle (`docker-compose.stores.yml`) | Started once and left running. Owns every durable volume and the `pipeline_network`. |
| **Pipeline** | Layers 1–4 and the serving **backend** (`docker-compose.yml`) | Disposable and replaceable. Owns no database. |
| **User frontend** | The serving **frontend** only (`5-serving/docker-compose.serving.yml`) | One instance per user machine. |

### Two modes, selected by command-line arguments only

The system runs in one of two modes. **No file is ever edited to switch between
them**: the mode is chosen entirely by the arguments passed to `docker compose`.

| | **Testing** (one machine) | **Intended** (distributed) |
|----|----|----|
| ClickHouse | 1 server, 1 Keeper | 6 servers (2 shards × 3 replicas), 3 Keepers |
| MongoDB | 1-node replica set | 3-node replica set |
| Oracle | 1 instance | 1 instance |
| Store containers | **4** | **13** |
| Memory at idle | **≈ 3.6 GB** | **≈ 6.1 GB** |
| Insert quorum | 1 (no redundancy) | 2 of 3 replicas |
| Pipeline location | same machine as the stores | its own machine |
| Frontend | same machine | one instance per user machine |

Three mechanisms do the switching:

- **`--env-file`** selects the ClickHouse cluster and Keeper topology, the
  per-node memory cap, the insert quorum and the MongoDB member count.
- **`--profile full`** starts the nine redundant services (five extra ClickHouse
  servers, two extra Keepers, two extra MongoDB members). Without it they simply
  do not start.
- **`PIPELINE_NET_EXTERNAL`** (set in the env file) decides whether the pipeline
  joins the network the stores created, or creates its own on a machine that runs
  no stores.

The application code, the cluster name, the table names and the service names are
identical in both modes.

### A. Testing — everything on one machine

```bash
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

# 5. the dashboard
docker compose -f 5-serving/docker-compose.serving.yml up --build
```

**The order matters.** The silver tables are owned and created by the validation
layer, so the seed cannot be restored before step 2 has run — there would be
nothing to insert into. `restore` waits up to five minutes for the schema to
appear and tells you what to start if it does not. Step 4 is equally load-bearing:
the gold layer is built **per user**, so until at least one profile exists there
is nothing for the processing layer to build and the dashboard stays empty.

Step 4 is the only step that runs on the host rather than in a container, and its
only dependency is `requests`. It waits up to two minutes for the backend to
answer before giving up, so it can safely be run the moment step 2 returns.

**There is no virtual environment to activate, and none is needed.** Every layer
runs in its own container with its own `requirements.txt`, so the project has no
host-side Python environment at all — this one script is the sole exception. If
the host has no suitable Python, run it in a throwaway container instead, which
needs nothing installed and reaches the backend by service name:

```bash
docker run --rm --network pipeline_network -v "$PWD/5-serving:/seed:ro" \
  -e BACKEND_URL=http://radar-backend:8000 python:3.11-slim \
  sh -c "pip install -q requests && python /seed/seed_test_users.py"
```

Either way it prints one line per account and is safe to repeat — the profiles are
upserted, so re-running restores them to their seeded state.

The `restore` step loads `data/silver_seed/*.parquet` — 15 MiB committed to this
repository, holding a fully filtered and enriched 30-day history — straight into
the ClickHouse volume. It takes about **three seconds**. Gold follows on its own:
the processing layer's watermark trigger notices silver has grown and builds
`articles` and `user_articles` within a minute, so the dashboard has real content
almost immediately rather than after the days the 15-minute pipeline would need.

Restoring is safe to repeat: both silver tables are `ReplacingMergeTree`, so
re-inserting the same rows collapses back to the same counts.

> **If gold stays empty after a few minutes, force one rebuild.** The trigger
> watches `max(DATEADDED)` in silver and fires only when that value *increases*.
> On a fresh clone it always does, because the trigger starts with no previous
> value and the first poll reads `None -> 20260727171500`. But on a cluster that
> has already processed **newer** data — for instance one that ran the live
> pipeline before restoring the seed — inserting the seed's older rows leaves
> `max(DATEADDED)` untouched, so the watermark does not advance and the trigger
> correctly declines to fire. Gold then stays as it was. One command rebuilds it:
>
> ```bash
> docker exec pipeline_processing python3 -c "import main; main.recompute_all()"
> ```
>
> This is the same work the trigger performs, run on demand. It is safe to repeat:
> `user_articles` is rebuilt per user from scratch, and `articles` is upserted and
> then swept of rows no user still references — see
> [Orphaned gold rows](#orphaned-gold-rows-and-why-they-can-be-removed-safely).

This starts four store containers rather than thirteen, which is what makes the
stack usable on a machine with 8 GB of RAM.

> **`--env-file` is required on every command that talks to the stores.** Without
> it they fall back to `CLICKHOUSE_INSERT_QUORUM=2`, which a single-replica
> cluster can never satisfy, and every insert fails with ClickHouse error 285.


The dashboard is then available at **http://localhost:8501**.

#### Signing in

The dashboard shows a sign-in screen first and nothing else until you are
authenticated — there is no anonymous view, and account creation is deliberately
not implemented. The three fixed accounts come from step 4 above
(`seed_test_users.py`); **without that step the credentials below still sign in,
but the account has no profile, so it lands on the setup page and the gold layer
stays empty** — the gold is built per user, and there is no user to build it for.

| Username | Password | Supply chain | Territories |
|---|---|---|---|
| `radar_electronics` | `chips2026` | Semiconductors and electronics | Asia-Pacific |
| `radar_pharma` | `vials2026` | Pharmaceuticals and biologics | Europe |
| `radar_agrifood` | `grain2026` | Agri-food commodities | Americas and Africa |

The three profiles are mutually exclusive: no territory and no keyword appears in
more than one account. Inactive sessions are signed out after 15 minutes.

These are test credentials, published deliberately. The sign-in gates the
dashboard UI only — the serving backend has no authentication of its own, so
anyone who can reach it directly can query any user's data. It is not production
authentication.

#### What each account will show

Measured on the committed seed, immediately after the steps above:

| Account | Articles | Event cards (unique `GLOBALEVENTID`) |
|---|---|---|
| `radar_agrifood` | **127** | **124** |
| `radar_electronics` | **21** | **21** |
| `radar_pharma` | **16** | **16** |
| *total distinct articles in gold* | *164* | |

An event card groups the articles reporting the same GDELT event, so cards are
never more numerous than articles. Only `radar_agrifood` shows the difference
here — 127 articles across 124 cards, meaning three of its events were reported
by two outlets each. For the other two accounts every article is a distinct
event.

The three counts differ by roughly eight-fold because the profiles genuinely
differ in reach, not because anything is wrong: agri-food keywords (*grain*,
*wheat*, *port*, *tariff*) are ordinary news vocabulary, whereas the electronics
and pharmaceutical lists are procurement-register terms (*photoresist*, *epoxy
molding compound*, *bromobutyl stoppers*) that rarely reach a general-news
headline. A single 15-minute slice frequently matches nothing for a given user;
that is the filter working as intended.

**All of it is historical.** A fresh clone starts with the committed 30-day
history and nothing else: GDELT slices from **2026-06-27 17:15 UTC to
2026-07-27 17:15 UTC**, 2,881 slices in total. Live polling begins the moment the
pipeline starts, so newer articles accumulate from then on at roughly one slice
every 15 minutes — but everything present at first sign-in comes from that
30-day window.

> **The `--build` flag is required.** Compose reuses an existing image and does not rebuild because a source file changed. Any modification to Python code, or to `.streamlit/config.toml`, reaches a container only when `--build` is passed. Files that are *mounted* rather than copied into the image — the compose files, `clickhouse/*.xml`, `oracle-init/*.sql` — are read at container start and require no rebuild.

On the **first** run the stores need several minutes: Oracle creates its database files from scratch and only then executes the gold schema. This occurs once per machine per volume; subsequent starts are fast.

### B. Intended — one machine per tier

Every step below is a command. Nothing is edited except `.env.full`, which is a
template where the placeholder `STORES_HOST` is replaced with a real address.

**No volume-clearing step is required**, whichever mode this machine ran before.
Each topology owns its own ClickHouse and Keeper volumes — `radar-full_*` here,
`radar-testing_*` in testing mode — so the two never share coordination state and
neither disturbs the other's silver. See
[Switching modes](#switching-modes-state-that-persists-across-the-switch) for why
that separation is necessary.

**Step 1 — prepare the address file** (on both the stores and pipeline machines).
`STORES_HOST` becomes the stores machine's address, for example `10.0.0.5`:

```bash
sed -i 's/STORES_HOST/10.0.0.5/g' .env.full
```

On the pipeline machine, also uncomment the three store-address lines at the
bottom of `.env.full` (`MONGO_URI`, `CLICKHOUSE_HOST`, `ORACLE_HOST`). The
MongoDB URI must list the same addresses the replica set advertises, otherwise
the driver is redirected to a member it cannot reach.

**Step 2 — the stores machine** (13 containers):

```bash
docker compose --env-file .env.full -f docker-compose.stores.yml --profile full up -d
```

`--profile full` is what starts the redundant nodes. Ports 27017–27019, 9000 and
1521 must be reachable from the pipeline machine.

**Step 3 — the pipeline machine**:

```bash
docker compose --env-file .env.full up -d --build
```

`.env.full` sets `PIPELINE_NET_EXTERNAL=false`, so Compose creates the network
here instead of expecting the stores to have made it.

**Step 4 — load the seed** (on the stores machine, after Step 3):

```bash
./bootstrap/silver_snapshot.sh restore
```

**This must follow Step 3, not precede it.** The silver tables are created by the
validation layer, which runs on the *pipeline* machine, so before Step 3 there is
no schema to insert into. `restore` waits up to five minutes for it to appear.

It is needed here for the same reason as in testing mode: Docker volumes are not
part of the repository, so a newly created cluster starts empty.
`data/silver_seed/*.parquet` is the committed 30-day history, and skipping this
leaves the dashboard blank until the live pipeline has run for days. Because
intended mode has its own ClickHouse volumes, this runs once per machine per mode
— restoring in testing mode does not populate intended mode's silver, or the
other way round.

Repeating the restore is safe: both tables are `ReplacingMergeTree`, so
re-inserting the same rows collapses back to the same counts rather than
duplicating them. If this cluster has already processed data **newer** than the
seed, the watermark will not advance and gold will not rebuild by itself. Force
one rebuild:

```bash
docker exec pipeline_processing python3 -c "import main; main.recompute_all()"
```

**Step 5 — create the three test profiles** (once, from any machine that can
reach the backend). The gold layer is built per user, so until a profile exists
there is nothing to build and the dashboard stays empty:

```bash
BACKEND_URL=http://10.0.0.60:8000 python3 5-serving/seed_test_users.py
```

Credentials and expected contents are under
[Signing in](#signing-in).

**Step 6 — each user machine** (one frontend per machine, any number of them):

```bash
BACKEND_URL=http://10.0.0.60:8000 \
  docker compose -f 5-serving/docker-compose.serving.yml up --build
```

**Step 7 — optional: run the pipeline under Docker Swarm.** Steps 3 and 6 give a
working distributed system, but the pipeline is then restarted only on the machine
it already occupies. Swarm adds machine-level failover: if the whole pipeline
machine dies, the pipeline is re-created on another node. It cannot be enabled by
a compose argument, because it changes the Docker daemon's own state and requires
an image registry.

```bash
# 1. Build and push the five images (Swarm cannot build from a Dockerfile)
export REGISTRY=<your-dockerhub-user>          # or a private registry
for l in ingestion parsing validation processing; do
  docker build -t $REGISTRY/radar-$l:latest ./$(ls -d [1-4]-*| grep $l)
  docker push  $REGISTRY/radar-$l:latest
done
docker build -t $REGISTRY/radar-backend:latest ./5-serving/backend
docker push  $REGISTRY/radar-backend:latest

# 2. Form the swarm
docker swarm init --advertise-addr <manager-ip>          # on the manager
docker swarm join --token <printed-token> <manager-ip>:2377   # on each other machine

# 3. Mark the machines allowed to run the pipeline
docker node update --label-add role=pipeline <node-name>

# 4. Deploy
REGISTRY=$REGISTRY docker stack deploy -c docker-stack.pipeline.yml radar
docker stack services radar
```

Label **at least two** nodes `role=pipeline` if Swarm is to have somewhere to move
the pipeline to. Layers 1–4 share a local volume and therefore always run
together on one node; the backend is stateless and spreads across the swarm.
Leave the stores out of Swarm: they are stateful and already have their own
replication.

### Returning to testing mode

Stop the current stores, then start them without `--profile full` and with the
testing env file. Nothing is deleted, in either direction:

```bash
docker compose -f docker-compose.stores.yml --profile full down
docker compose --env-file .env.testing -f docker-compose.stores.yml up -d
docker compose --env-file .env.testing up -d --build
```

Testing mode reattaches to its **own** ClickHouse and Keeper volumes
(`radar-testing_*`), which still hold whatever silver they held when this mode
was last used; intended mode's `radar-full_*` volumes are left exactly as they
were. If this is the first time testing mode has run on this machine, its silver
starts empty and needs one `./bootstrap/silver_snapshot.sh restore`.

MongoDB and Oracle are shared between the modes, so **user profiles, tags and the
gold layer carry across the switch**. `mongo-init` detects that the replica set is
still configured for three members while only one is running — a state in which no
primary can be elected and every write fails — and reconfigures it to a single
member automatically. The reverse switch is handled the same way.

**What `BACKEND_URL` is.** The frontend holds no database credentials and never contacts ClickHouse, MongoDB or Oracle directly; every value it displays is retrieved from the serving backend's HTTP API. `BACKEND_URL` is the address of that API. Because the frontend runs on each user's machine while the backend runs on the operator's, it must be set to the operator machine's address and the backend's published port 8000.

| Situation | Value |
|----|----|
| Frontend and backend on the same machine | `http://host.docker.internal:8000` (the default; no configuration required) |
| Frontend on a user machine, backend on the operator machine | `http://<operator-host>:8000` |

If the address is unreachable, the dashboard reports that the backend is unavailable rather than displaying data.

**One frontend runs per user machine.** The frontend is stateless and holds no data; any number of instances may run concurrently against a single backend. Ports 8000 (backend) and 27017–27019, 9000, 1521 (stores) must be reachable between the machines.

------------------------------------------------------------------------

## What data is present, and when

**Present in the volumes before the pipeline starts.** The bootstrap step loads a body of historical GDELT slices directly into the silver layer (ClickHouse). This exists because the live pipeline processes one 15-minute slice at a time, at approximately one minute per slice; a 30-day history would otherwise require two to four days to appear. The bootstrap applies the same filters as the live pipeline and writes through the validation layer's own storage class, so the result is indistinguishable from data that arrived live. It does not write gold: the processing layer's watermark trigger detects the growth in silver and builds the gold itself.

**Downloaded every 15 minutes once the pipeline is running.** The ingestion layer reads `http://data.gdeltproject.org/gdeltv2/lastupdate.txt`, which lists the current 15-minute release, and downloads two files from it: `<timestamp>.export.CSV.zip` (events) and `<timestamp>.mentions.CSV.zip` (mentions). A representative slice contains approximately 979 events and 3,222 mentions before filtering.

There is a gap between the end of the bootstrap history and the moment the pipeline is started. Data published by GDELT during that interval is not retrieved.

------------------------------------------------------------------------

## Shutting down while preserving all data

Stop the tiers in the reverse of their start order. The commands differ per mode,
because a tier must be shut down with the same arguments that started it: omitting
`--profile full`, for instance, leaves the nine redundant containers running.

**Optional first — keep only the shipped 30-day history.** The pipeline polls
continuously, so a store that has been running holds the seed *plus* everything
since. Stop the ingest path before trimming, or the next poll lands mid-clean-up
and puts the excess straight back:

```bash
docker compose --env-file .env.testing stop ingestion parsing validation        # stop new data
./bootstrap/silver_snapshot.sh trim seed                                        # silver
docker exec pipeline_processing python3 -c "import main; main.recompute_all()"  # gold
```

`seed` resolves to the last slice in `data/silver_seed`; `trim <YYYYMMDDHHMMSS>`
takes an explicit cutoff. Skip all three to keep the live history. In intended
mode the first command uses `--env-file .env.full`.

**What the third command does to the gold layer, precisely.** `trim` only touches
silver, so the rebuild is what propagates the change into gold:

- **`user_articles` is rebuilt from scratch per user** (`DELETE … WHERE user_id`,
  then re-insert), so every article that no longer survives the trim disappears
  from that user's pool.
- **`articles` is upserted** (`MERGE INTO`) and then **swept**: any row no
  `user_articles` row still references is deleted.

The sweep is what stops `articles` growing forever. It runs at the end of every
recompute — full or single-user — so it also fires when someone narrows their own
preferences on the dashboard, not only after a trim. See
[Orphaned gold rows](#orphaned-gold-rows-and-why-they-can-be-removed-safely).

**Testing mode:**

```bash
docker compose -f 5-serving/docker-compose.serving.yml down
docker compose --env-file .env.testing down
docker compose --env-file .env.testing -f docker-compose.stores.yml down
```

**Intended mode** — each command on the machine that runs that tier:

```bash
# each user machine
docker compose -f 5-serving/docker-compose.serving.yml down

# pipeline machine
docker compose --env-file .env.full down

# stores machine — --profile full is required, or the nine extra containers remain
docker compose --env-file .env.full -f docker-compose.stores.yml --profile full down
```

**If the pipeline was deployed under Swarm** (step 5 of the intended-mode setup),
remove the stack instead. Leave the swarm itself only if the machines are being
repurposed:

```bash
docker stack rm radar          # on the manager
docker swarm leave --force     # optional, on each machine
```

`down` removes containers and networks. It does not affect named volumes, so all durable data survives and is available at the next start:

| Data | Volume | Preserved |
|----|----|----|
| User profiles — territories, keywords | `mongo1/2/3_data` (`radar.users`) | Yes |
| Per-user tags — archived, needs action, monitoring | `mongo1/2/3_data` (`radar.tags`) | Yes |
| Gold — `articles`, `user_articles` | `oracle_data` | Yes |
| Silver — `gdelt_events`, `gdelt_mentions` | `radar-testing_ch_*_data` / `radar-full_ch_*_data` (per mode) | Yes |
| In-flight raw and parsed slices | `shared_data` | Yes, and disposable in any case |

> **The `-v` flag must never be used.** `docker compose … down -v` deletes the volumes, permanently destroying every user profile, every tag, and the entire silver and gold history. Only the most recent 15-minute GDELT slice could be re-retrieved.

Passing `--build` at the next start is safe: it rebuilds images from source and does not affect volumes.

------------------------------------------------------------------------

## One-time setup (automated)

Both steps belong to the **stores** tier, so the pipeline never repeats them, and both are idempotent:

- **MongoDB replica set** — the `mongo-init` service executes `rs.initiate(rs0)`, guarded by `rs.status()`, and is a no-operation once the set exists.
- **Oracle gold schema** — `oracle-init/01_schema.sql` is executed once, as SYSDBA, at first database creation. The `radar` user is created by the image from `APP_USER`; the service (PDB) is `FREEPDB1`.
- The processing layer publishes the territory table to MongoDB at startup, retrying until MongoDB is available; the frontend retrieves it from the backend via `GET /territories`.

Three test accounts are defined in `5-serving/frontend/auth.py`. Their profiles are created by running, once, after the backend is available:

```bash
python3 5-serving/seed_test_users.py
```

The gold layer is built per user. Until at least one profile exists, `articles` and `user_articles` remain empty by design.

------------------------------------------------------------------------

# Design notes

The following section records the reasoning behind the storage and pipeline decisions.

## Every difference between testing mode and intended mode

The two modes run **identical application code**. No Python file, table name,
cluster name or service name differs between them. Everything that changes is
configuration, and all of it is selected by command-line arguments.

### Services that exist only in intended mode

Nine services carry `profiles: ["full"]` in `docker-compose.stores.yml` and
therefore start only when `--profile full` is passed:

| Service | Purpose |
|----|----|
| `clickhouse-s1r2`, `clickhouse-s1r3` | replicas 2 and 3 of shard 1 |
| `clickhouse-s2r1`, `clickhouse-s2r2`, `clickhouse-s2r3` | the whole of shard 2 |
| `clickhouse-keeper-2`, `clickhouse-keeper-3` | the other two Keeper nodes |
| `mongo2`, `mongo3` | the other two replica-set members |

Compose refuses to start a service whose dependency is disabled by a profile, so
`depends_on` was narrowed: every ClickHouse server now depends on
`clickhouse-keeper-1` alone, and `mongo-init` on `mongo1` alone. The additional
nodes join the ensemble and the replica set when they start; nothing needs to
wait for them.

### Configuration files that exist in two versions

| Setting | Testing (`*.local.xml`) | Intended | Consequence |
|----|----|----|----|
| `clickhouse/cluster*.xml` — `<remote_servers>` | 1 shard, 1 replica | 2 shards, 3 replicas each | Testing has neither sharding nor redundancy; a query scans one node rather than two shards in parallel |
| `clickhouse/cluster*.xml` — `<zookeeper>` | 1 Keeper node listed | 3 Keeper nodes listed | The list must match the running ensemble, or servers block trying to reach absent coordinators |
| `clickhouse/keeper-1*.xml` — `<raft_configuration>` | 1 server | 3 servers | A single-server ensemble forms a quorum of one and elects itself; the three-server ensemble survives losing one member |
| `clickhouse/memory*.xml` — `max_server_memory_usage` | 2.5 GB | 1.1 GB | Six nodes must share the host, so each is capped tightly; one node may take far more. Using the cluster cap on a single node causes ClickHouse error 241 (memory limit exceeded) under normal load |
| `clickhouse/memory*.xml` — `mark_cache_size` | 256 MB | 128 MB | Same reasoning: the cache is per node |

The cluster is named `gnews_cluster` in both versions, so every
`ON CLUSTER gnews_cluster` statement in the validation layer works unchanged.
`internal_replication` remains `true` in both.

### Settings that differ in value only

| Variable | Testing | Intended | Why |
|----|----|----|----|
| `CLICKHOUSE_INSERT_QUORUM` | `1` | `2` | The number of replicas that must acknowledge an append. With one replica a quorum of two can never be met, and every insert fails with ClickHouse error 285, *"Number of alive replicas (1) is less than requested quorum (2)"*. It is set on the validation and processing services in `docker-compose.yml`, on the loader in `docker-compose.bootstrap.yml`, and in the shared environment of `docker-stack.pipeline.yml`, so all four paths agree |
| `MONGO_MEMBERS` | `1` | `3` | Drives whether `mongo-init` configures a one- or three-member replica set |
| `MONGO_MEMBER_0/1/2` | Docker service names | the stores machine's real addresses | Clients are redirected to the addresses the set advertises, so they must be reachable from the pipeline machine |
| `MONGO_URI`, `CLICKHOUSE_HOST`, `ORACLE_HOST` | unset (Docker service names apply) | the stores machine's address | Only needed when the tiers are on separate machines |
| `BACKEND_URL` | `host.docker.internal:8000` | `http://<pipeline-host>:8000` | The frontend runs on a different machine from the backend |
| `STORE_VOLUMES` | `radar-testing` | `radar-full` | Names this mode's ClickHouse and Keeper volumes, so the two topologies never share coordination state. See [Switching modes](#switching-modes-state-that-persists-across-the-switch) |

### Structural differences

- **The network.** `pipeline_network` is declared
  `external: ${PIPELINE_NET_EXTERNAL:-true}`. When the variable is `true` the
  pipeline joins the network the stores created; when `.env.full` sets it to
  `false`, the `external` key disappears from the merged configuration entirely
  and Compose creates the network itself, which is what a machine running no
  stores requires. This is done with a variable rather than an override file
  because Compose **merges** the files given with `-f`: an override can add or
  change a key, but there is no way to remove one, so an override could never
  turn `external: true` off.
- **Frontend instances.** One in testing; one per user machine in intended mode.
  The frontend is stateless, so any number may run against a single backend.
- **`mongo-init` behaviour.** It no longer only initiates a new set. It compares
  the configured member count against `MONGO_MEMBERS` and, when they differ, runs
  `rs.reconfig(…, {force: true})`. This matters when switching modes: a set still
  configured for three members with only one running has no majority, so no
  primary is elected and **every write fails**. `force` is required precisely
  because there is no primary to accept an ordinary reconfiguration.

### Switching modes: state that persists across the switch

Two stores record their own cluster membership on disk. That record survives a
mode change, and if it disagrees with the topology now running, the store fails
in a way that looks like a network fault rather than a configuration error.

**MongoDB — handled automatically, no action required.** A replica set still
configured for three members while only one is running has no majority, so no
primary is elected and every write fails with `NotWritablePrimary`. `mongo-init`
compares the configured member count against `MONGO_MEMBERS` on every start and
issues `rs.reconfig(…, {force: true})` when they differ. `force` is required
precisely because there is no primary to accept an ordinary reconfiguration.

**ClickHouse Keeper — resolved by giving each mode its own volumes.** Keeper
persists its Raft membership in its data volume. A Keeper whose volume says it
belongs to a three-node ensemble cannot elect a leader when started alone, and
rejects every connection with `Coordination::Exception: Keeper server rejected the
connection during the handshake … doesn't see leader or stale`. ClickHouse then
retries indefinitely, so the symptom is a pipeline that appears to hang at 0% CPU
rather than an error. The ClickHouse data volumes are affected too, because the
replicated tables' coordination lives in Keeper and their metadata is orphaned
without it.

This cannot be fixed by *clearing* the volumes from a compose argument, because
Compose has no conditional "empty this volume" directive: a volume is either
mounted or it is not. It can, however, be fixed by never sharing them. Each of the
nine ClickHouse and Keeper volumes is declared with an interpolated name:

```yaml
  ch_s1r1_data:
    name: ${STORE_VOLUMES:-radar-testing}_ch_s1r1_data
```

`.env.testing` sets `STORE_VOLUMES=radar-testing` and `.env.full` sets
`radar-full`, so the two topologies address entirely separate volumes. Switching
modes is then purely a matter of compose arguments, with **no manual step and no
data destroyed in either direction** — each mode simply reattaches to the silver
it last had. The cost is that each mode keeps its own copy of silver, filled
independently by `./bootstrap/silver_snapshot.sh restore`; at 15 MiB and about
three seconds from the committed seed, that is a better trade than a destructive
manual step.

MongoDB and Oracle are deliberately **not** mode-scoped. MongoDB reconciles its
own member count as described above, and Oracle runs as a single instance in both
modes, so both can safely share one volume — which means user profiles, tags and
the gold layer survive a mode switch intact.

Docker Swarm is the third thing that cannot be a compose argument: it changes the
Docker daemon's own state and requires an image registry, so it is an explicit
operator step (Step 5 of the intended-mode setup).

### Capabilities that testing mode does not have

- **No redundancy.** Losing the single ClickHouse node loses the silver layer;
  losing the single MongoDB node loses profiles and tags. In intended mode a
  shard survives losing two of its three replicas, and MongoDB retains a majority
  when one member is lost.
- **No sharding**, so no parallel scan across shards.
- **No automatic MongoDB failover**, as there is no second member to promote.
- **No quorum-acknowledged writes**, since there is no second replica to
  acknowledge them.
- **A single Keeper**, which is a single point of failure for coordination.

Oracle is a single instance in **both** modes: the free edition supports neither
RAC nor Data Guard, so the gold layer is not replicated in either configuration.

Everything else — the filters, the deduplication rules, the retry behaviour, the
triggers, the schemas and the enrichment — is byte-for-byte identical.

## Why three different databases

Each store was selected for the access pattern of the data it holds.

**ClickHouse holds the silver layer** — every event and every article mention GDELT publishes, appended continuously and never updated in place. Queries over it are analytical: filter millions of rows by country code, by keyword, by date. A columnar engine reads only the columns a query touches, and the repeated low-cardinality values that dominate this data (country codes, CAMEO event codes) compress extremely well. Scans are parallelised across shards, and skip indexes prune blocks that cannot contain a match. This is the canonical OLAP workload.

**Oracle holds the gold layer** — the finished, per-user article sets the dashboard reads. Those queries are selective point lookups: retrieve one user's articles, by index, returning complete rows. They are transactional, and they upsert. A row-oriented store with B-tree indexes is the correct instrument for retrieving whole rows by key. This is the canonical OLTP workload.

The division can be stated in one sentence: **columnar storage where the system scans, row storage where it looks up.** The expensive columnar scan is paid once, during processing, so that every dashboard read is an inexpensive indexed lookup.

**MongoDB holds user profiles, per-user tags and the territory reference table.** These are small, self-contained documents whose shape varies: a profile contains a list of territories and a nested dictionary of five keyword groups, none of which is naturally relational. A document store fits without an object-relational mapping. MongoDB was also chosen for a second, operational reason: its **change streams** allow the processing layer to react the instant a user modifies their preferences, without polling.

## Node counts, fault tolerance and distribution

The system is distributed at the storage layer.

- **ClickHouse — 6 data nodes plus 3 Keeper nodes.** The data is divided into **2 shards** by `cityHash64(GLOBALEVENTID)`, and each shard is held by **3 replicas** running `ReplicatedReplacingMergeTree`. A shard therefore survives the loss of two of its three nodes without data loss. The 3-node Keeper ensemble coordinates replication and `ON CLUSTER` DDL, and retains a quorum when one node is lost. The sharding key is deliberate: an event and all of its mentions hash to the same shard, so joins are local rather than cross-node, and repeated copies of an event from successive batches land on the same node, where the `ReplacingMergeTree` can collapse them.
- **MongoDB — 3 nodes** in replica set `rs0`. A majority is retained when one node is lost, and PyMongo performs primary failover automatically. Writes are issued with `w="majority"`.
- **Oracle — a single node.** This is a deliberate and acknowledged limitation: the free Oracle edition supports neither RAC nor Data Guard, so the gold layer cannot be replicated across machines without a licensed edition. Its resilience is restricted to container restart with a persistent volume. Were multi-machine redundancy of the gold layer required, PostgreSQL with streaming replication would be the pragmatic substitute; nothing in the design depends on Oracle specifically.
- **Hand-offs.** The pipeline writes to ClickHouse with `insert_quorum`, so an append is acknowledged only once it has reached a quorum of replicas. MongoDB writes use majority acknowledgement. Oracle writes are committed transactionally, and the row counts the server reports are compared against the counts submitted.

**PySpark** provides the horizontally distributed path from silver to gold, as an alternative to the in-process implementation. Its parallelism is genuine at all three stages: the read is a **partitioned JDBC read**, in which Spark divides the `GLOBALEVENTID` range into `numPartitions` disjoint ranges and issues one concurrent query per range, so each executor retrieves only its own slice; the events-to-mentions join is a distributed shuffle join; and `df.write.jdbc()` opens one connection per partition, so the write is executed by the executors in parallel. Because no result is materialised centrally, no row cap is required. Spark's JDBC writer offers only `append` and `overwrite` and cannot upsert, so the job writes to staging tables that it creates and drops itself, and a single subsequent SQL statement publishes them into the live tables with precisely the semantics of the in-process path: `MERGE` for `articles`, delete-and-reinsert per user for `user_articles`, and then the same anti-join sweep of orphaned `articles` rows — inside the one publish transaction, after `user_articles` has been rebuilt, so both paths leave byte-for-byte the same gold.

## Why the pre-loaded silver is small, and why it took so long to produce

The silver seed committed to this repository is around 15 MiB, which is easy to
mistake for a small amount of work. It is not. Even though the volume's ready-made
data looks small in size, querying it from GDELT and letting the pipeline turn it
into silver took about half a week. This is because the bronze layer being put
together — which is removed progressively once turned to silver — was by far
larger.

The figures for the 30-day window shipped here:

| Stage | Size |
|----|----|
| Bronze — the raw GDELT archives that had to be downloaded | ≈ 410 MB |
| Silver — after filtering, validation and enrichment | **15 MiB** |

Two reductions compound. The supply-chain relevance filter discards roughly 97%
of events, keeping about 31 of every 979 in a slice; and Parquet's columnar
compression is very effective on the low-cardinality codes that dominate what
remains. The result is a 26-fold reduction.

The time went almost entirely into work that leaves no trace in the final size:
downloading 5,762 archives, and then fetching roughly 85,000 individual article
pages to extract titles and keywords. Enrichment alone accounts for the bulk of
it, and it is bounded by how quickly remote news servers respond rather than by
any local resource.

This is precisely why the seed is committed. The expensive work is done once, by
the maintainers, and every clone restores the result in seconds instead of
repeating it.

## How the silver seed is implemented

Docker volumes are not part of a git repository. They live in Docker's own storage
on each machine, outside the project directory, so a clone always creates **empty**
volumes. The seed is the mechanism that bridges that gap: ordinary files that git
can carry, plus a command that loads them into the volume on whatever machine runs
it.

**The files.** `data/silver_seed/` holds two Parquet files, `gdelt_events.parquet`
(7.0 MiB, 103,972 rows) and `gdelt_mentions.parquet` (8.1 MiB, 111,430 rows) — 15 MiB
in total, against the 410 MB of raw archives they were distilled from. They are
committed; `data/release/` and `data/raw/` are not.

**The tool.** `bootstrap/silver_snapshot.sh` has three verbs:

| Verb | Action |
|----|----|
| `export` | `SELECT * FROM <table> FINAL FORMAT Parquet` for each silver table, written to `data/silver_seed/`. `FINAL` collapses the `ReplacingMergeTree` duplicates, so the snapshot holds one row per key. |
| `restore` | `INSERT INTO <table> FORMAT Parquet` for each file, streamed through the **Distributed** table, so rows are routed to their shard exactly as a live write would be. Waits for the validation layer to have created the schema. |
| `wipe` | `TRUNCATE TABLE <table>_local ON CLUSTER` — used when rebuilding from scratch. |
| `trim <slice>` | Deletes every row published after a given 15-minute slice. Needed when rebuilding the seed, because the live pipeline keeps polling while a backfill runs; without it the exported seed would carry an arbitrary tail of "today" and ship it to everyone who clones the repository. |

**Rebuilding the seed from the raw archives** is a four-step sequence, and the
order matters for the same reason `trim` exists:

```bash
docker compose --env-file .env.testing stop ingestion parsing validation  # stop live writes
./bootstrap/silver_snapshot.sh wipe
ENRICH=1 docker compose -f docker-compose.bootstrap.yml run --rm bootstrap
./bootstrap/silver_snapshot.sh trim 20260727171500   # the last slice in the release
./bootstrap/silver_snapshot.sh export
```

> **On macOS, copy the archive into a named volume first.** Docker Desktop's file
> sharing is very slow for per-file metadata, and slowest under `~/Desktop` and
> `~/Documents`, which macOS additionally guards. Measured here, simply *listing*
> the 5,762 archives took **0.04 s** from a named volume or on the host, against
> **more than 25 minutes** through a bind mount of `./data/release` — during which
> the loader sits at 0% CPU and looks frozen. `docker-compose.bootstrap.yml`
> documents the one-line `tar` that moves the archive inside Docker, after which
> `RELEASE_SOURCE=gdelt_release` runs the load at full speed.

**Why restore is safe to repeat.** Both tables are `ReplacingMergeTree`:
`gdelt_events` keyed on `GLOBALEVENTID`, `gdelt_mentions` on
`(GLOBALEVENTID, MentionIdentifier)`. Re-inserting identical rows collapses back to
the same counts rather than duplicating them — measured: a second restore left the
totals unchanged at 103,972 and 111,430.

**Why gold is not snapshotted.** Only silver is captured. Gold is derived, and
derived per user: it depends on the territories and keywords in each profile, which
differ per installation. Shipping a gold snapshot would bake one set of users'
preferences into every clone. Instead the processing layer rebuilds gold from the
restored silver through its normal trigger, so the result always matches the
profiles that actually exist on that machine and can never drift from what the
pipeline would have produced.

**Cost.** `restore` takes about three seconds; gold follows within the trigger's
60-second poll. Producing the seed took 213 minutes of enrichment plus the archive
download.

## What causes the gold layer to update

Four things, two automatic and two manual. All of them call the same two functions,
`recompute_all()` and `recompute_user()`, so the result is identical whichever
path is taken.

| Trigger | Scope | Latency |
|----|----|----|
| **MongoDB change stream** on `radar.users` — fires on `insert`, `update` or `replace` of a profile | that one user (`recompute_user`) | **immediate**, typically under a second |
| **ClickHouse watermark poll** — compares `max(DATEADDED)` in silver against the last value seen | every user (`recompute_all`) | up to `WATERMARK_POLL_SECONDS`, default **60 s** |
| `POST /process/{user_id}` on the processing service | that one user | on demand |
| `POST /process-all` on the processing service | every user | on demand |

The change stream is what makes a preference change take effect immediately: the
moment a user saves new territories or keywords, their profile document is written
to MongoDB, the stream delivers the change, and `recompute_user` re-runs that
user's filter against the whole of silver. Articles that were previously in silver
but matched nobody are therefore promoted into that user's gold at once, without
waiting for new data to arrive. This is also why MongoDB runs as a replica set even
in testing mode: change streams are unavailable on a standalone server.

The watermark trigger covers the other direction — new data arriving for existing
preferences. It deliberately fires only when the watermark **increases**, so
restoring an older snapshot onto a cluster that has already seen newer data will
not trigger it; that case needs one manual `recompute_all()`.

## Why enrichment never reaches 100%

Enrichment fetches each article's page and extracts its title and keywords. A
consistent minority of URLs cannot be enriched.

A URL yields no title when the page is a dead link, sits behind a paywall or a
consent wall, is blocked to automated clients, redirects to a section front page
rather than an article, or is not an article at all. GDELT indexes URLs as
published; it does not guarantee that they remain reachable, and a proportion of
news links are unreachable within days of publication.

Failures are handled without loss: a mention that cannot be enriched is stored
with an empty title, empty keywords and `enriched = 0`. It remains a fully valid
silver row and is still matched by the keyword filter, which falls back to
searching the URL itself for rows where `enriched = 0`. Nothing is discarded, and
the dashboard falls back to displaying the URL where a headline is missing.

## Fault tolerance, layer by layer

### The pipeline

Five independent mechanisms, none of which depends on shared storage:

1. **Container restart.** Every pipeline service is declared `restart:
   unless-stopped`, so a process that crashes is restarted by Docker without
   intervention.
2. **Recovery by re-polling rather than by shared state.** `shared_data` holds
   only the slice currently in flight. A replacement machine starts with an empty
   volume and loses nothing, because the durable state lives entirely in the
   stores, which are a separate tier.
3. **Immediate poll at startup.** Ingestion fetches the current GDELT release the
   moment it starts rather than waiting for its next 15-minute tick, so a
   replacement machine begins contributing at once.
4. **Idempotent re-ingestion.** Both silver tables are `ReplacingMergeTree`, so a
   slice ingested twice collapses to the same rows. This is what makes blind
   re-polling safe after a failure.
5. **Endless, bounded retries at every boundary** — see the section below.

The pipeline is operated **active-passive**: one live instance at a time. Two
concurrent instances would poll GDELT twice and duplicate work that the stores
would then have to deduplicate.

### What actually happens when the pipeline fails, in each mode

The recovery behaviour is **not** the same in the two modes, and the difference
matters.

| Failure | Testing mode (plain `docker compose`) | Intended mode (Docker Swarm) |
|----|----|----|
| A single container crashes | Docker restarts it **on the same machine**, because every pipeline service declares `restart: unless-stopped` | Swarm restarts the task, by the same principle |
| The Docker daemon is restarted | Containers come back automatically | Same |
| **The whole machine fails** | **Nothing happens.** There is no second machine, and no data is lost — but the pipeline stops until the machine returns | Swarm detects the node is gone and **re-creates the whole pipeline on another node** carrying the `role=pipeline` label |

In testing mode the machine is a single point of failure for *processing*, though
never for *data*: the durable state is in the stores, and a restarted pipeline
re-polls GDELT and continues. That is an acceptable trade for a single-machine
test environment.

### Docker Swarm — a blueprint, not currently active

`docker-stack.pipeline.yml` describes the pipeline as a Swarm stack, which is
what provides machine-level failover in intended mode. **It is not deployed at
present** (`docker info` reports `Swarm: inactive`); the file is a prepared
description that has to be activated deliberately.

Each service declares `replicas` and `restart_policy: any`, so when a task or an
entire node is lost, Swarm re-creates it on another node satisfying the placement
constraint. Layers 1 to 4 all carry the same constraint,
`node.labels.role == pipeline`, because they hand data to one another through a
local volume and must therefore stay co-located. The backend, being stateless,
declares `replicas: 3` and is spread across the swarm behind the routing mesh. A
rescheduled pipeline starts with a fresh, empty volume and re-polls GDELT, which
is precisely the recovery model described above.

Activating it requires three steps that plain Compose does not:

```bash
docker swarm init --advertise-addr <manager-ip>     # on the manager
docker swarm join --token <...> <manager-ip>:2377   # on each other machine
docker node update --label-add role=pipeline <node> # label the eligible nodes
docker stack deploy -c docker-stack.pipeline.yml radar
```

Swarm cannot build from a `Dockerfile`, so the images must be built once and
pushed to a registry before deploying; the stack refers to them through the
`REGISTRY` variable. Label more than one node `role=pipeline` if Swarm is to have
somewhere to move the pipeline to.

The stores are deliberately **not** placed in Swarm: stateful clustered services
are considerably harder to orchestrate, and they already have their own
replication and failover.

### The stores

- **ClickHouse.** Each shard holds three replicas of its data under
  `ReplicatedReplacingMergeTree`, so a shard survives losing two of its three
  nodes. Writes are issued with `insert_quorum`, meaning an append is
  acknowledged only once a quorum of replicas holds it, so an acknowledged write
  survives the loss of a node. Coordination runs on a three-node Keeper ensemble,
  which retains a majority when one member is lost. Each node's memory is capped
  explicitly, preventing the cluster-wide restart loop that occurs when several
  servers each assume they own the machine.
- **MongoDB.** A three-member replica set: a majority survives the loss of one
  member, and the driver performs primary failover automatically. Writes use
  `w="majority"`, so an acknowledged write is held by a majority. `mongo-init`
  additionally repairs the configuration when the member count changes, a state
  in which no primary can be elected and every write would otherwise fail.
- **Oracle.** A single instance in both modes; the free edition supports neither
  RAC nor Data Guard. Its resilience is limited to container restart with a
  persistent volume. Writes are committed transactionally and the row counts the
  server reports are compared against the counts submitted, so a partial write is
  detected rather than assumed successful.
- **All three** keep their data in named Docker volumes, which survive
  `docker compose down`.

## Why the pipeline itself runs on a single machine

Layers 1 to 4 hand data to one another as **files on a shared volume**: ingestion writes to `/data/raw/csv`, parsing publishes to `/data/latest_files`, validation consumes from there. A local volume exists on exactly one host, so those four layers must be co-located.

This is a deliberate choice rather than an oversight, because the volume of traffic does not justify anything more elaborate: four CSV files every fifteen minutes. The relevant question is not how to distribute that trickle, but what happens when the machine carrying it fails.

**The answer is that the pipeline recovers by re-polling, not by sharing storage.** The shared volume holds only transient scratch data: the raw slice currently being processed, the parsed pair awaiting validation, and a status marker. None of it is a source of truth. If the machine fails, a replacement starts the same stack with an empty local volume; ingestion polls the current GDELT slice immediately at startup, and processing resumes. The durable state is held entirely by the three stores, which are a separate tier with a separate lifecycle and are never re-created by the pipeline.

Re-ingestion is idempotent, which is what makes this safe. `gdelt_events` is a `ReplacingMergeTree` keyed on `GLOBALEVENTID`, retaining the newest `DATEADDED`; `gdelt_mentions` is a `ReplacingMergeTree` keyed on `(GLOBALEVENTID, MentionIdentifier)`, retaining the enriched row. A slice ingested twice collapses back to the same rows.

The pipeline should be operated **active-passive**, with one live instance at a time. Two concurrent instances would both poll GDELT and perform duplicate work, which the stores would deduplicate but which serves no purpose.

## Why Kafka would have been excessive

A message broker addresses problems this system does not have. The hand-off is four CSV files per fifteen minutes — a trickle, not a stream requiring buffering. Fan-out is not required, because each stage has exactly one consumer. Replay is not required, because **GDELT itself is the durable, replayable source**: any slice can be re-retrieved from its published URL. Introducing Kafka would add a broker to operate, and would add a second durable store whose contents would have to be reconciled with the one already in place. The file volume performs the same hand-off with no operational cost, and the recovery model above supplies the durability a broker would otherwise provide.

## Why the bronze layer is ephemeral

Raw GDELT CSVs are deleted as soon as parsing has published the corresponding slice, and the parsed pair is deleted as soon as validation has stored it. Nothing raw is retained.

This is sound because the bronze layer is not a source of truth but a staging area for data that is already durably published elsewhere and can be retrieved again from GDELT at any time. Retaining it would consume storage at a rate of roughly 400 MB per month per copy in exchange for no recovery capability that re-polling does not already provide. The layer that must not be lost is silver, which is replicated three ways per shard.

Deletion is also what applies back-pressure: parsing publishes a new pair only when `latest_files` is empty, so a slow validation cycle throttles the upstream stages instead of allowing work to accumulate.

## Oracle B-trees and the choice of index key

Oracle's primary keys are B-tree indexes, and a B-tree key has a maximum length of approximately 6,398 bytes on a default 8 KB block.

The natural key for an article is its URL, held as `document_identifier VARCHAR2(2000)`. Under the `AL32UTF8` character set a single character may occupy up to four bytes, so 2,000 characters may occupy 8,000 bytes — in excess of the limit, raising `ORA-01450`. The composite key `(user_id, document_identifier)` in `user_articles` would be larger still.

The key is therefore **`doc_id RAW(32)`**, the SHA-256 digest of the URL: a fixed 32 bytes irrespective of URL length, deterministic, and collision-resistant. The full URL is retained alongside it as ordinary, non-indexed data, so nothing is lost for display purposes. The hash is computed at the Oracle boundary only; every other layer continues to handle URLs.

## Keeping validation within the 15-minute cadence

The validation cycle must complete within the interval between GDELT releases, otherwise slices accumulate. Two bounds enforce this.

**Enrichment** is bounded by `ENRICH_TIMEOUT_SECONDS` (600 s per mentions file), a time budget rather than a fixed cost: article scraping proceeds across eight workers. In the offchance that the budget is exhausted, the remaining mentions are stored unenriched rather than delaying the cycle (which cannot be allowed to take longer than 15 minutes, since new GDELT data arrives at that cadence).

**Every ClickHouse operation** is bounded by `CLICKHOUSE_OP_TIMEOUT` (120 s), applied as the server-side `max_execution_time` and matched by a slightly longer socket timeout. This covers the referential-integrity lookup, both appends, and the deduplication `OPTIMIZE`, which is otherwise the operation most likely to run long as the table grows. The total cycle is therefore bounded by the enrichment budget plus a bounded number of bounded database operations.

## Country and territory code coverage

Territories are matched by two independent code systems: **CAMEO** three-letter codes, which identify the *actors* in an event, and **FIPS** two-letter codes, which identify the *location* of an event. An event matches a user's perimeter if either system matches.

The two published lookups do not cover an identical set of places. Reconciling them produced 237 entries, of which **eight have a FIPS code but no CAMEO code** — that is, they can be matched as the location of an event, but never as an actor. Two are sovereign states: **Kosovo** and **South Sudan** (the CAMEO list predates South Sudan's independence). The remaining six are territories: the British Indian Ocean Territory, the French Southern and Antarctic Lands, Guernsey, Jersey, Saint Martin and Saint-Barthélemy. Two further entries have the converse limitation, holding a CAMEO code but no FIPS code: the Åland Islands and Palestine.

Reconciliation also required correcting errors in the published data — the FIPS list mislabels Guinea's code as Equatorial Guinea, and labels Slovakia's code as Czechoslovakia — and merging divergent spellings of the same place, such as `Cote dIvoire` against `Ivory Coast`, and `Columbia` against `Colombia`. The Palestinian territories are consolidated into a single entry carrying all five related codes, so that selecting it matches both actor and location.

## Retention: the ten-year rule

Everything else in the pipeline either appends or rebuilds. Silver is
append-only, gold's `articles` is upserted, and until this rule existed **nothing
in either store ever aged out** — both grew for as long as the pipeline ran, and
the only removal was the manual `silver_snapshot.sh trim`.

A daily job now deletes events that have gone quiet for a decade, together with
everything hanging off them:

| Store | What is removed |
|----|----|
| ClickHouse | the event from `gdelt_events`, and all of its `gdelt_mentions` |
| Oracle | its rows in `user_articles`, then in `articles` |
| MongoDB | any user's tag pointing at it |

**Ten years is a starting point, not a fixed constant.** It is set by
`RETENTION_YEARS`, so shortening it is a one-line change requiring no migration
and no code edit — set the variable on the processing service and the next run
uses the new cutoff. A shorter window is likely to be wanted once the store has
real volume behind it; ten years was chosen simply because it cannot delete
anything the project currently holds, which makes it a safe default to ship.

**The clock read is `MentionTimeDate`, the article's own timestamp.** Expiry is
`max(MentionTimeDate)` over an event's mentions — the newest article about it.

**Measured from the newest article, not the event date.** A long-running story
keeps attracting coverage: the event row is stamped once, but articles arrive for
as long as anyone is still writing. Measuring from the event date would delete a
story that is still being reported. Measuring from its most recent article means
an event survives exactly as long as the world keeps talking about it, and ages
out ten years after the last word. An event that never had a mention at all has
no "most recent article", so it falls back to its own `DATEADDED` — otherwise
such rows could never expire.

**Why deleting mentions by `GLOBALEVENTID` is the same thing as checking each
one's own `MentionTimeDate`.** Once an event is condemned, its mentions are
removed by event id rather than re-tested individually. That is not a shortcut
with different behaviour: if the *maximum* `MentionTimeDate` for an event is below
the cutoff, then every one of its mentions is below it too, because the maximum
is the largest. **No mention younger than the cutoff can be deleted.**

The converse is deliberate: an old article belonging to a *still-active* event is
**kept**. A 2015 report on a story that received fresh coverage last week survives
with its event. Pruning it independently would also corrupt the card ordering,
which is keyed on the oldest article a card holds — that key would drift forwards
as a story's earliest coverage aged out from under it.

**Order matters in both stores.** Mentions are deleted before events, so a crash
between the two statements leaves a harmless mention-less event rather than
mentions whose event has vanished — the state the referential-integrity check
assumes cannot happen. In Oracle, `user_articles` goes before `articles`, so the
join never briefly points at rows that are already gone.

**Schedule.** Daily at 00:00, plus a catch-up on startup when a midnight was
missed — a laptop that sleeps overnight would otherwise never clean up at all.
The last run is recorded in `/data/state/retention.json`, written atomically, so
a restart cannot lose or repeat it. `ENABLE_RETENTION=0` disables the job
entirely; it is gated separately from the other triggers precisely because it is
the only thing in the pipeline that deletes data.

**On the shipped seed it deletes nothing.** The seed spans June–July 2026, so a
ten-year cutoff lands in 2016 and nothing qualifies. That is expected: the rule is
forward-looking. `RETENTION_YEARS` exists so the behaviour can be exercised
without waiting — moving the cutoff to 2026-06-30 expired 8,939 events and 9,571
mentions, left no survivor past the cutoff, and left no mention orphaned from its
event.

Every page that lists event cards says so in small print, because a card
disappearing is normal behaviour rather than a fault.

## Orphaned gold rows, and why they can be removed safely

The gold layer is normalised into `articles` (one row per document) and
`user_articles` (the `(user_id, doc_id)` join). The two are written with different
semantics, and that asymmetry used to leave rubbish behind:

- `user_articles` is **rebuilt** per user on every recompute, so a user's set is
  always exactly what their current preferences select;
- `articles` is written with **MERGE**, which only ever inserts or updates.

So whenever a user narrowed their territories or keywords — or silver was trimmed
— the articles they dropped stayed in `articles`, referenced by nobody. They were
unreachable, because serving joins `user_articles → articles`, but they
accumulated without limit.

They are now swept at the end of every recompute:

```sql
DELETE FROM articles a
WHERE NOT EXISTS (SELECT 1 FROM user_articles ua WHERE ua.doc_id = a.doc_id)
```

**This destroys nothing but the orphans.** A row referenced by *any* user is kept
by the anti-join, which is why the sweep is safe even after a single-user
recompute: user A dropping an article that user B still holds does not remove it.
`user_articles` is untouched, and so are MongoDB (profiles, tags) and the silver
layer. No volume has to be recreated — an earlier version of this document
claimed otherwise, and was wrong.

**An orphan is never removed if any user has tagged it.** This is the one
exception to the rule above, and it is not optional. If a user has filed an event
as **red** (*Needs action from us*), **yellow** (*Look out for developments*) or
archived it, every article of that event is kept — whether or not it still
appears in anyone's `user_articles`, and whether the tag belongs to the user
whose recompute triggered the sweep or to somebody else entirely.

The reason is how the triage pages read. They fetch cards **straight from
`articles`, deliberately without joining `user_articles`**, so that a filed card
survives the user later dropping the territory that first brought it in. Such a
row is legitimately unreferenced — precisely what the anti-join targets. Without
this guard the sweep would delete it and leave the tag in MongoDB pointing at
nothing: the card would vanish from the red or yellow page while still counted as
tagged. The query therefore also excludes any article whose `global_event_id`
appears in any user's tag set.

Measured: narrowing one account's perimeter until its pool was empty removed 126
rows and kept the 1 belonging to its tagged event, and the card stayed on the
Needs action page.

If the tag list cannot be read, the sweep is **skipped entirely** rather than run
unguarded. Leaving a few unreachable rows costs a little space; deleting a card
someone filed loses their work.

`NOT EXISTS` rather than `NOT IN`: Oracle optimises the anti-join, and `NOT IN`
would silently match nothing at all if any `doc_id` were ever NULL.

**Measured.** Narrowing `radar_agrifood` to a single territory and one keyword
that matches nothing reduced its pool from 127 articles to none. The change
stream fired a single-user recompute, which removed **exactly its 127 rows** and
left **37** behind — precisely `radar_electronics` (21) plus `radar_pharma` (16),
whose articles were still referenced. Restoring the profile returned the gold
layer to 164 articles with zero orphans.

**When the sweep runs.** At the end of `recompute_all()` and of
`recompute_user()`. Because the MongoDB change stream calls the latter, editing
preferences on the dashboard cleans up after itself within seconds; and because
the documented shutdown sequence ends in a recompute, `trim seed` leaves no
residue either.

The same sweep runs in the PySpark path, inside the publish transaction and after
`user_articles` has been rebuilt, so both paths leave the same state.

## How event cards are ordered

Cards are ordered by **the timestamp of their oldest article, most recent first**
— so the story that *started* most recently leads. The key answers "when did this
begin?", which for supply-chain risk distinguishes a situation that emerged this
morning from one that has been developing for a fortnight.

The oldest article is taken across a card's **whole** article list, not the three
shown before it is opened, so the ordering does not shift with the preview length.

This needs a per-**article** timestamp, which the gold layer originally lacked:
`event_date` comes from the event and is therefore identical for every article on
a card. `articles.mention_time` carries silver's `MentionTimeDate` across to make
the ordering possible. Rows written before that column existed hold `NULL` until
the next recompute refills them, and cards with no timestamp at all sort last
rather than breaking the comparison.

Ordering is applied in the backend, in both `get_events_for_user()` and
`get_events_by_ids()`, which between them feed the Radar View, Archive, Needs
action and Looking out for developments pages — the frontend sorts nothing.

*Previously* the order was `global_event_id` ascending: GDELT's internal
allocation order, arbitrary to a reader. The confidence and tone keys that used to
sit beside it still order the articles **within** a card, which is why the top
article is the highest-confidence one.

## Why an article never appears twice in the gold layer

The gold layer is normalised. **`articles` holds one row per document**, keyed on `doc_id`; the article's content and metadata are therefore stored exactly once, regardless of how many users receive it. **`user_articles` is a join table** holding only `(user_id, doc_id)` — approximately 32 bytes per interested user. Adding a user to an article adds one narrow row, never a copy.

## Deduplication, in full

Duplicates are eliminated at four distinct points, because they arise for four distinct reasons.

1. **Re-ingested slices.** Both silver tables are `ReplacingMergeTree`. `gdelt_events` is keyed on `GLOBALEVENTID` with `DATEADDED` as the version, so the most recent copy of an event survives. `gdelt_mentions` is keyed on `(GLOBALEVENTID, MentionIdentifier)` with `enriched` as the version, so an enriched row supersedes an unenriched one. All readers query with `FINAL`, which collapses duplicates at query time rather than waiting for a background merge.
2. **The same URL cited repeatedly.** When gold rows are constructed, a URL already seen is skipped, so one article yields one row.
3. **Syndicated stories.** The same report is frequently republished under different URLs, which produce different keys but an identical headline. Rows are therefore also deduplicated by `(event, normalised headline)`, compared case-insensitively and with whitespace collapsed. The same filter is applied on the read path, so gold written before this rule was introduced also displays correctly.
4. **The `user_articles` primary key.** Document identifiers are deduplicated before insertion, since a URL may be reachable through several events.

## Watermarking: how the pipeline knows it is making progress

New data is expected every 15 minutes. The question a watermark answers is what
should happen when a particular 15-minute point **does not** arrive, or arrives
and cannot be processed. Waiting for it indefinitely is the failure this section
exists to prevent.

**Progress is measured in event time.** GDELT names every file after its own
15-minute slice (`20260810121500`), and that identifier — not the time the file
happened to be handled — is what the pipeline records. Slice ids can be compared
and ordered, so a gap in the feed is detectable; a URL alone cannot be.

**The watermark only moves forwards.** A slice arriving late never drags it back
over ground already covered, and the processing layer recomputes only when
`max(DATEADDED)` in silver **increases**. An equal value means nothing new
arrived; a lower one means the store was rebuilt behind it — for instance a seed
restored over newer data — and rebuilding gold from a shorter history than it
already reflects would be a regression, so it declines and says so.

**Lateness is bounded, and failure is bounded with it.** A slice that cannot be
processed is retried three times and then moved to `/data/dead_letter/<slice>/`.
This matters more than it sounds: slices are handled oldest-first and one at a
time, so before this bound existed a single malformed file was retried every few
seconds **forever**, and every later slice queued behind it — the pipeline
stopped without ever reporting an error. The same bound applies in the validation
layer, where a failing pair would otherwise block parsing from publishing at all,
since parsing waits for `latest_files` to drain.

Nothing is deleted by any of this. An abandoned slice is **moved**, so it remains
available for inspection, and can be replayed by copying it back or loaded
deliberately with `bootstrap/bulk_load.py`.

**Silence is reported rather than assumed to be health.** Four checks cover the
four ways the pipeline can go quiet:

| Check | Where | Threshold | What it catches |
|----|----|----|----|
| No new files in `latest_files` | validation | `STALE_LIMIT_SECONDS` 35 min | parsing or ingestion has stopped |
| `latest_files` never drains | parsing | `BACKPRESSURE_MAX_WAIT` 30 min | validation has stalled; publishing is blocked |
| Silver watermark not advancing | processing | `SILVER_STALE_SECONDS` 45 min | nothing is reaching the store; gold is frozen |
| Gold older than the clock allows | serving backend | `PIPELINE_STALE_SECONDS` 60 min | the processing layer itself is down, so nobody is left to report |

The last of these is the reason the backend does not simply trust the stored
status. The processing layer writes `ERROR` when it *notices* silver has stopped
advancing — but if that layer is the thing that died, it writes nothing at all,
and the last row it wrote says `OK` for as long as Oracle keeps answering.
Comparing the timestamp against the clock is what catches a pipeline with nobody
left to report on it.

### Why these thresholds are the right size

Every threshold is derived from the **15-minute release cadence**, and each sits
in the same band: long enough that ordinary jitter cannot trip it, short enough
that a real stall is noticed within the hour.

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

The ordering is the point: **30 → 35 → 45 → 60 minutes**, escalating outward from
the layer closest to the problem to the one furthest from it. Whichever component
is still alive reports first, and the backend's clock comparison catches the case
where none of them are.

**Bounded attempts, not bounded time, for processing.** Retrieval has a wall-clock
deadline because a file that has not been published will never be published.
*Processing* is bounded by **attempt count** instead (`MAX_SLICE_ATTEMPTS` and
`MAX_PAIR_ATTEMPTS`, both 3). A wall-clock limit there would discard slices that
were merely slow — a transient ClickHouse pause would start throwing away good
data — whereas three failures of the same file is evidence about the file itself.

**Gaps are stated, not silently back-filled.** The poller fetches whatever
`lastupdate.txt` currently points at, so slices published while it was stopped are
missed. It now names them in the log instead of passing over them. Back-filling
them automatically is deliberately *not* done: a 15-minute poller quietly
reloading hours of history is exactly the unbounded lateness the rest of this
design rules out. `bootstrap/bulk_load.py` exists to load a known period on
purpose.

## Retries: nothing waits indefinitely

Every dependency that may be temporarily unavailable is retried rather than allowed to block or fail permanently.

- **Validation → ClickHouse.** Table creation retries every five seconds until the cluster responds, tolerating Compose start-up ordering.
- **Processing → MongoDB.** The territory table is published from a background thread that retries every five seconds until the replica set has elected a primary. A single attempt would frequently lose that race and leave the picker empty.
- **Processing triggers.** Both the MongoDB change stream and the ClickHouse watermark poll run in supervised loops that log and continue rather than terminating.
- **Backend → Oracle and MongoDB.** Both clients retry with exponential backoff, distinguishing transient failures (connection loss, timeout) from permanent ones (invalid SQL, authentication), which are re-raised immediately.
- **Ingestion → GDELT.** A 404 for a release that is still being published is retried three times at five-second intervals, since the file appears shortly afterwards.
- **Frontend → backend.** Every call is wrapped, and an unreachable backend produces an explanatory banner rather than an error page.
- **Bootstrap loader.** Waits for ClickHouse in the same five-second retry loop before loading.

Retrying is bounded where the thing being retried may never succeed. A transient
dependency is worth waiting for indefinitely; a malformed file is not, because
retrying it forever blocks every slice behind it. Parsing and validation therefore
give a slice three attempts and then set it aside — see
[Watermarking](#watermarking-how-the-pipeline-knows-it-is-making-progress).

Fail-soft behaviour complements this. A failure to read tags leaves events untagged rather than hiding them; a failure to read the pipeline status is reported explicitly as a database outage rather than being silently reported as healthy. One deliberate exception exists: a failure to read a user profile is raised rather than substituted with an empty default, because an empty profile is indistinguishable from a new user and could otherwise be saved over a real one.

## Inspecting why an article was selected

Because the filters are narrow and applied in sequence, "why is this pool so
small?" is not answerable by reading the code alone — it depends on which rule
each surviving article actually satisfied.
[`dev_tools_for_filter_diagnostics/`](dev_tools_for_filter_diagnostics/) answers
it by re-deriving the decision for every article in gold:

```bash
docker compose -f docker-compose.diagnostics.yml run --rm diagnostics
```

It reads all three stores read-only, writes no store, and produces
`gold_provenance.csv` — one row per article per user, naming the parsing
criterion that admitted it, the territory code and code system that matched, and
the keyword and field that selected it. It imports the pipeline's own filter
functions rather than restating them, so it cannot fall out of step with the
filters in force.

## What is retrieved, and what is filtered at each stage

**Every 15 minutes**, ingestion reads `lastupdate.txt` and retrieves the current events and mentions archives. A representative slice contains approximately 979 events and 3,222 mentions.

### Stage 1 — retrieval, and why the hand-off is atomic

Ingestion assembles a slice in a **staging directory** and moves it into the
hand-off directory only when one of two things is true: the slice is **complete**,
or its **retrieval deadline** has expired (`SLICE_RETRIEVAL_DEADLINE`, 600 s
measured from the slice's own timestamp). While a file is missing and the deadline
has not passed, only *that* file is re-attempted, every `RETRY_TICK` (60 s),
addressed directly by its slice timestamp rather than by whatever `lastupdate.txt`
currently advertises.

Before staging existed, each CSV was written to the hand-off directory the instant
it was extracted, so a slice could be published half-finished while its partner was
still downloading. Staging closes that window: files are moved with `os.replace()`,
**mentions last**, mirroring the ordering the parsing layer already relies on.

The consequence is the property the rest of the pipeline is built on:
**anything in the hand-off directory is final**. A lone file there is not "half a
slice still arriving" — it is all there will ever be. That is what allows the
layers below to act on a partial slice instead of waiting for a partner that will
never come, and a partial slice is still useful:

- **events alone** update the store, because `gdelt_events` is a
  `ReplacingMergeTree` keyed on `GLOBALEVENTID` with `DATEADDED` as the version,
  so a re-published event supersedes the stored copy;
- **mentions alone** attach to events already stored, because the
  referential-integrity check resolves against *this events file **or** the store*.

The poll cycle then sleeps the **remainder** of the 15 minutes rather than a fixed
15, so a cycle that spent time retrying does not push the next one out to T+25 and
drift further every time.

### Stage 2 — bronze to silver: two filters, applied in different layers

**The relevance filter (parsing).** `parser.passes_filter()` requires
`F1 AND (F2 OR F3) AND has_source_url`:

| | Criterion | Backed by |
|----|----|----|
| **F1** | the 4-digit `EventCode` is a supply-chain-relevant CAMEO code, **or** the 2-digit `EventRootCode` is one of the relevant macro categories | 32 event codes; 6 root codes (14 protest, 15 force, 17 coerce, 18 assault, 19 fight, 20 mass violence) |
| **F2** | an actor carries a relevant type or known-group code | 5 type codes, 8 known groups |
| **F3** | a supply-chain word appears in either actor name or in the source URL | 35 keywords (*port, shipping, freight, customs, tariff, …*) |

F1 is mandatory, so an article about shipping that is not a relevant *event* is
still discarded. Roughly **31 of 979** events survive per slice. This filter is
independent of users: it decides what is worth storing at all.

**The referential-integrity filter (validation).** A mention is kept only if its
`GLOBALEVENTID` exists **in the events file it arrived with, or already in
ClickHouse**. The store lookup is what makes a mentions-only slice viable, and it
also lets a mention attach to an event detected hours earlier. Unmatched mentions
are dropped and the file on disk is rewritten without them.

**Enrichment sits between the two**, and only touches the survivors — the
referential filter runs first, so no rejected mention is ever scraped. Newspaper3k
fetches each unique URL once across 8 threads, extracting `article_title` and,
via NLTK, `article_keywords`. The whole step is bounded by
`ENRICH_TIMEOUT_SECONDS` (600 s **per slice**, not per URL); anything unscraped
when the budget expires is stored with empty fields and `enriched = 0`. A mention
counts as enriched when a **title** was obtained.

### Stage 3 — silver to gold: the per-user filter

Two conditions, combined with **and** — an article must satisfy both:

**Territory.** An event qualifies if *either* code system matches: CAMEO codes
against the actor columns (`Actor1/Actor2CountryCode`), or FIPS codes against the
location columns (`ActionGeo_`/`Actor1Geo_`/`Actor2Geo_CountryCode`). Keeping both
is deliberate — measured on the current gold, 84 articles matched on location
only, 17 on actor only and 63 on both.

**Keywords.** Every mention is searched in **all three** text fields — the URL, the
article title and the extracted keywords — regardless of its `enriched` flag. A
keyword matches when **all of its tokens** are present, a token being a single
word: `silicon wafers` needs both *silicon* and *wafers*, but not adjacently.
Singular and plural collapse onto one another, so `chip` and `chips` are the same
word. Matching is whole-word, so `chip` does not match `chipotle`.

Two earlier rules were removed because they were measurably wrong. Keywords were
matched as **contiguous phrases**, so `silicon wafers` matched 0 of 99,175 enriched
mentions. And each row was routed to exactly **one** field depending on `enriched`,
which meant that of those 99,175 rows, the 170 whose URL contained a supply-chain
term were never checked against it — enrichment was actively destroying matches.

Because both filters are narrow, a single 15-minute slice frequently matches
nothing for a given user. That is expected behaviour, not a fault.

Two earlier rules were removed because they were measurably wrong. Keywords were matched as **contiguous phrases**, so `silicon wafers` matched 0 of 99,175 enriched mentions — a headline says "chip firm buys wafer plant", never the procurement phrase verbatim. And each row was routed to exactly **one** field depending on its `enriched` flag, which meant that of the 99,175 enriched rows, the 170 whose URL contained a supply-chain term were never checked against it — enrichment was actively destroying matches.

**Tables retained on failure.** The PySpark path writes to `articles_stage` and `user_articles_stage`, which it creates at the start of each run and drops after a successful publication. They are deliberately **not** dropped when publication fails, so the staged result remains available for inspection; the next run drops and recreates them. Each carries an Oracle table comment recording its purpose and that it may safely be dropped. These are the only Oracle objects left behind on purpose: unreferenced rows inside `articles` itself are removed automatically, as described under [Orphaned gold rows](#orphaned-gold-rows-and-why-they-can-be-removed-safely).

## Data persistence

All silver and gold data, and all user-linked data — profiles, keyword sets, and the archived, needs-action and monitoring tags — reside in **named Docker volumes**. `docker compose down` removes containers and networks but does not remove volumes, so this data persists across restarts. Only `docker compose down -v` deletes it, irreversibly.

------------------------------------------------------------------------

## Contributing

Each layer owns a single responsibility; per-layer dependencies belong in that layer's `requirements.txt`.
