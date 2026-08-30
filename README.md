# Introduction to and startup instructions for a production-level design of a global-news event radar for geopolitical and supply risk

This app gives users a customised and informative briefing of news of events that are of interest to their supply chain's stability (this will tend to be risks associated with the supply chain).

This repository is "production-level" meaning that it deliberately avoids using one Docker app that puts together the data pipeline, the LTS systems, and the frontend into one Docker app to run `docker compose` on. Rather, these aspects are entirely separate, each with its own set of Docker containers making one Docker app to call `docker compose` on. Like in a production-level design, the only point of contact between the pipeline (which involves ingestion, parsing, validation, processing, and the backend of the storage) and the LTS systems is `pipeline_network` (a DNS with no published ports). Also like in a production-level design, the Docker app for the frontend of the service is even more separate from the other two Docker apps, with its only point of contact being a URL to the backend (`BACKEND_URL`): this way, multiple such frontend Docker apps can be run connected to the same backend. Starting up every part of the radar together is thus deliberately and necessarily a multi-step process that avoids using one Docker app for everything, even if Docker apps can already have multiple containers.

Two modes are in place: single-machine mode and intended mode. **Only single-machine mode should be used by any examiner. Even though no problems were observed with intended mode either, intended mode should be ignored simply because it requires multiple machines.** In contrast, intended mode is boilerplate to make single-machine mode's data distributed across machines: intended mode was curated at every relevant step of the creation of single-machine mode, but to test intended mode, significant hardware is required, without which Docker Swarm will refuse to start intended mode. In a nutshell, single-machine mode is the only mode that can run on one machine, and intended mode is a curated but untested draft of the setup to make the same pipeline run on multiple machines.

## Single-machine mode

In single-machine mode, the pipeline, the stores, and the serving frontend are all on the same machines. But each was still given its own Docker stack for easier scalability and for better emulation of real-world pipelines.

### Single-machine mode startup

**Note: step 3 will continue retrying automatically until ClickHouse is available, even 10 or 20 times. This is fully normal and does not mean there is any problem. ClickHouse will become available by itself.**

REQUIREMENT: have Docker up and running.

Preferrably, you may set "Settings -> Resources -> Advanced -> Resource Allocation -> Memory limit" to at least 6 GB, even though testing revealed that this pipeline can work with less.

```bash
# On macOS, Linux, and Windows with WSL2, steps 1 through 4 below can be run as one line, separated with "&&"'s like so.
docker compose --env-file .env.single_machine -f docker-compose.stores.yml up -d && docker compose --env-file .env.single_machine up -d --build && ./bootstrap/silver_snapshot.sh restore && docker compose -f 5-serving/docker-compose.serving.yml up --build
```

```bash
# ...On Windows without WSL2, please run this instead.
docker compose --env-file .env.single_machine -f docker-compose.stores.yml up -d && docker compose --env-file .env.single_machine up -d --build && ./bootstrap/silver_snapshot.ps1 restore && docker compose -f 5-serving/docker-compose.serving.yml up --build
```

Those that follow are the same steps, broken down.

```bash
# 1. Stores
docker compose --env-file .env.single_machine -f docker-compose.stores.yml up -d

# 2. Pipeline (Ingestion; Parsing; Validation and Storage; Processing; Serving Backend)
# Especially if running the code in this README multiple times, this may mention "orphans", and that's fine: those are idempotently-created intended-mode versions of what you will be opening. They need to stay in place in case intended mode is every run, but intended mode is too heavy to work on one machine.
# NOTE: This step now includes an automated one-shot "seeder" container. It will wait for the backend to be ready and automatically create the three test profiles idempotently. 
docker compose --env-file .env.single_machine up -d --build

# 3. (ONLY MACOS, LINUX, OR WINDOWS WITH WSL2) OPTIONAL BUT NECESSARY FOR PROPER TESTING: Silver data from articles spanning 27/06/2026 at 17:15 to 27/07/2026 at 17:15.
# IMPORTANT: this will continue retrying automatically until ClickHouse is available. This is fully normal and does not mean there is any problem. ClickHouse will become available by itself.
# This "seeded" data was chosen to make the testing-mode radar not empty at startup: the radar gets updated with the latest news every 15 minutes, and automatically drops news older than 185 days every midnight. Even seeding over a year of data will leave a gap in single-machine mode: all the per-15-minutes slices between the latest seeded/stored data and the data from the moment a tester starts up the testing-mode radar with these instructions.
./bootstrap/silver_snapshot.sh restore

# 3. (ONLY WINDOWS WITHOUT WSL2) Same as the other version of step 3 above.
.\bootstrap\silver_snapshot.ps1 restore

# 4. Service frontend.
# While all previous steps just need to be run on the backend/operator machine, this is the only code that each frontend machine will need.
docker compose -f 5-serving/docker-compose.serving.yml up --build

# 5. You may then view the radar's UI via this link:
# http://localhost:8501

# 6. You may then log in with one of these test profiles, each of which has different supply-chain-related preferences.
# |For login: Username       |For login: Password |FYI: Territories       |
# |--------------------------|--------------------|-----------------------|
# | radar_electronics        | chips2026          | Asia-Pacific          |
# | radar_pharma             | vials2026          | Europe                |
# | radar_agrifood           | grain2026          | Americas and Africa   |
```

Here in single-machine mode, backend machines and frontend machines are the same one machine, but these steps are still kept separate to keep the production-level design (to also have distribution across machines, see intended mode below).

### ONLY FOR THIS PIPELINE'S DEVELOPERS, NOT NEEDED FOR TESTING: Rebuilding the 30-day test seed

This loads some example data for single-machine mode. It is already going to be seeded by default, so that Startup can use the seed.

``` bash
# REQUIRES ingestion, parsing, and validation to be on first. (So, first perform at least steps 1 and 2 of Startup)

# 1.
# WARNING: this command can take a while, because it downloads at a much greater pace than the pipeline normally would (30 days of data rather than 15 minutes of it!). It might keep your computer awake for a long time unless you stop the process or close the computer.
caffeinate -is bash -c "docker compose --env-file .env.single_machine stop ingestion parsing validation && ./bootstrap/silver_snapshot.sh wipe && env ENRICH=1 docker compose --env-file .env.single_machine -f docker-compose.bootstrap.yml run --rm --build bootstrap && ./bootstrap/silver_snapshot.sh trim 20260727171500 && ./bootstrap/silver_snapshot.sh export && docker compose --env-file .env.single_machine start ingestion parsing validation"
# If available in the present `data` Docker volume, can add `RELEASE_SOURCE=gdelt_release` right after `ENRICH=1`
# (strongly recommended on macOS: a bind mount of ./data/release takes >25 min just to LIST the 5,762 ZIPs, versus 0.04 s from the named volume.)

# 2.
# To ensure everything is operational again, you may continue from point 3 of Startup (not of this list of points!) onwards.
```

### Shutdown

For convenience, we include for single-machine mode also shutdown instructions.

All "**OPTIONAL**" and "**NOT OPTIONAL**" (see below) shutdown steps together.

```bash
docker compose --env-file .env.single_machine stop ingestion parsing validation && ./bootstrap/silver_snapshot.sh wipe && docker exec pipeline_processing python3 -c "import main; main.recompute_all()" && docker compose -f 5-serving/docker-compose.serving.yml down && docker compose --env-file .env.single_machine down && docker compose --env-file .env.single_machine -f docker-compose.stores.yml down
```

**OPTIONAL**: Reset silver so the next startup can restore the seed cleanly.

``` bash
# All steps below as one line:
docker compose --env-file .env.single_machine stop ingestion parsing validation && ./bootstrap/silver_snapshot.sh wipe && docker exec pipeline_processing python3 -c "import main; main.recompute_all()"
```

```bash
# 1.
docker compose --env-file .env.single_machine stop ingestion parsing validation        # Stops live data from being ingested

# 2.
./bootstrap/silver_snapshot.sh wipe                                             # Removes all silver data and prevents repeated restores accumulating physical duplicates

# 3.
docker exec pipeline_processing python3 -c "import main; main.recompute_all()"  # Recomputes gold from the now-empty silver layer (can take a few minutes, but this is indeed not an action the system would normally perform under any circumstance)
```

**NOT OPTIONAL**: shutdown procedure (that does not destroy the volumes, so user preferences and data for users stay intact).

```bash
docker compose -f 5-serving/docker-compose.serving.yml down && docker compose --env-file .env.single_machine down && docker compose --env-file .env.single_machine -f docker-compose.stores.yml down
```

## Intended mode

In intended mode, machines have the following jobs. This was designed holding real-world constraints in mind: imagining that money is not a limit, the jobs below would be spread out across even more machines.

| Machine | ClickHouse | Keeper | MongoDB | PostgreSQL+Patroni | etcd | Docker Swarm Role | Assigned Workloads |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **store1** | s1r1 | keeper-1 | — | leader | etcd-1 | **manager** | Stores Tier (Shard 1, Replica 1) |
| **store2** | s1r2 | keeper-2 | — | replica | etcd-2 | **manager** | Stores Tier (Shard 1, Replica 2) |
| **store3** | s1r3 | keeper-3 | — | replica | etcd-3 | **manager** | Stores Tier (Shard 1, Replica 3) |
| **store4** | s2r1 | — | mongo1 | — | — | worker | Stores Tier (Shard 2, Replica 1) |
| **store5** | s2r2 | — | mongo2 | — | — | worker | Stores Tier (Shard 2, Replica 2) |
| **store6** | s2r3 | — | mongo3 | — | — | worker | Stores Tier (Shard 2, Replica 3) |
| **pipeline1** | — | — | — | — | — | worker (`role=pipeline`) | Ingestion through Serving Backend, `spark-master`, `spark-worker` |

The logic is that ClickHouse's shard 1 lives on store1–3 and shard 2 on store4–6, so a shard survives losing two machines. store1–3 carry four separate quorums between them — Keeper, etcd, the Swarm managers and Patroni's PostgreSQL trio — each of which tolerates losing one of its three. Three Swarm managers, not one: Swarm coordinates through Raft exactly as Keeper does, so a single manager would be a single point of failure for orchestration, and nothing could be rescheduled while it was down.

### Intended-mode startup

As explained above, **intended mode cannot be run on one machine and should be ignored**. Intended mode's current set up is only a boilerplate to make single-machine mode scalable to multiple machines in theory, and re-uses a lot of the same code. Nonetheless, instructions to start it up are provided here.

REQUIREMENTS: For all machines: Docker and a clone of the project repository.

```bash
# 1. (On store1): Form the Docker Swarm
docker swarm init --advertise-addr <store1-ip>
docker network create -d overlay --attachable pipeline_network

# 2. (On all machines except store1) `docker swarm init` above printed a join command. Run it on every other machine.

# 3. (On store1) Promote store2 and store3 so there are three managers.
docker node promote <store2-hostname> <store3-hostname>

# 4. (On store1): Label the machines, for the placement constraints to work
docker node update --label-add store=store1 <store1-hostname>
docker node update --label-add store=store2 <store2-hostname>
docker node update --label-add store=store3 <store3-hostname>
docker node update --label-add store=store4 <store4-hostname>
docker node update --label-add store=store5 <store5-hostname>
docker node update --label-add store=store6 <store6-hostname>
docker node update --label-add role=pipeline <pipeline1-hostname>

# 5. (On store1, store2, or store3, the Docker Swarm managers) Build and push the images.
export REGISTRY=<your-dockerhub-user>
for l in ingestion parsing validation processing; do
  docker build -t $REGISTRY/radar-$l:latest ./$(ls -d [1-4]-* | grep $l)
  docker push  $REGISTRY/radar-$l:latest
done
docker build -t $REGISTRY/radar-backend:latest ./5-serving/backend
docker push  $REGISTRY/radar-backend:latest

# 6. (On store1, store2, or store3, the Docker Swarm managers) Deploy both stacks.
set -a; . ./.env.intended; set +a
docker stack deploy -c docker-stack.stores.yml   radar-stores
docker stack deploy -c docker-stack.pipeline.yml radar
docker stack ps radar-stores

# 7. Watch the latter until every task says Running.

# 8. (On store1, the machine hosting clickhouse-s1r1) Load the seed.
./bootstrap/silver_snapshot.sh restore

# 9. (On any machine) Create the three test profiles. (This also triggers the building of the gold layer.)
BACKEND_URL=http://<any-swarm-node>:8000 python3 5-serving/seed_test_users.py

# 10. (On any machine) Start the frontend to view the results.
BACKEND_URL=http://<any-swarm-node>:8000 \
  docker compose -f 5-serving/docker-compose.serving.yml up --build
```