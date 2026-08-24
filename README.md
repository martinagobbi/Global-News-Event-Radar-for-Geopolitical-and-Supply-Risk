# Production-level design of the "Global-News Event Radar for Geopolitical and Supply Risk"

This app gives users a customised and informative briefing of news of events that are of interest to their supply chain's stability (this will tend to be risks associated with the supply chain).

This repository is "production-level" meaning that it deliberately avoids using one Docker app that puts together the data pipeline, the LTS systems, and the frontend into one Docker app to run `docker compose` on. Rather, these aspects are entirely separate, each with its own set of Docker containers making one Docker app to call `docker compose` on. Like in a production-level design, the only point of contact between the pipeline (which involves ingestion, parsing, validation, processing, and the backend of the storage) and the LTS systems is `pipeline_network` (a DNS with no published ports). Also like in a production-level design, the Docker app for the frontend of the service is even more separate from the other two Docker apps, with its only point of contact being a URL to the backend (`BACKEND_URL`): this way, multiple such frontend Docker apps can be run connected to the same backend. Starting up every part of the radar together is thus deliberately and necessarily a multi-step process that avoids using one Docker app for everything, even if Docker apps can already have multiple containers.

Two modes are in place: testing mode and intended mode. **Only testing mode can be run on one machine, so only testing mode can be used when running the whole radar to observe and evaluate how the radar operates.** In contrast, intended mode is boilerplate to make testing mode's data distributed across machines: intended mode was curated at every relevant step of the creation of testing mode, but to test intended mode, significant hardware is required, without which Docker Swarm will refuse to start intended mode. In a nutshell, testing mode is the only mode that can run on one machine, and intended mode is a curated but untested draft of the setup to make the same pipeline run on multiple machines.

## Testing mode: startup, shutdown

### Startup

```bash
# Steps 1 through 5 below can be run as one line, separated with "&&"'s like so:
docker compose --env-file .env.testing -f docker-compose.stores.yml up -d && docker compose --env-file .env.testing up -d --build && ./bootstrap/silver_snapshot.sh restore && docker compose -f 5-serving/docker-compose.serving.yml up --build

# 1. Stores
docker compose --env-file .env.testing -f docker-compose.stores.yml up -d

# 2. Pipeline (Ingestion; Parsing; Validation and Storage; Processing; Serving Backend)
# Especially if running the code in this README multiple times, this may mention "orphans", and that's fine: those are idempotently-created intended-mode versions of what you will be opening. They need to stay in place in case intended mode is every run, but intended mode is too heavy to work on one machine.
# NOTE: This step now includes an automated one-shot "seeder" container. It will wait for the backend to be ready and automatically create the three test profiles idempotently. 
docker compose --env-file .env.testing up -d --build

# 3. OPTIONAL BUT NECESSARY FOR PROPER TESTING: Silver data from articles spanning 27/06/2026 at 17:15 to 27/07/2026 at 17:15.
# This "seeded" data was chosen to make the testing-mode radar not empty at startup: the radar gets updated with the latest news every 15 minutes, and automatically drops news older than 365 days every midnight. Even seeding over a year of data will leave a gap in testing mode: all the per-15-minutes slices between the latest seeded/stored data and the data from the moment a tester starts up the testing-mode radar with these instructions.
./bootstrap/silver_snapshot.sh restore

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

Here in testing mode, backend machines and frontend machines are the same one machine, but these steps are still kept separate to keep the production-level design (to also have distribution across machines, see intended mode below).

### ONLY IF NEEDED: Rebuilding the 30-day test seed

``` bash
# REQUIRES ingestion, parsing, and validation to be on first. (So, first perform at least steps 1 and 2 of Startup)

# Steps 1 through 6 below as one line:
# WARNING: might keep your computer awake for hours unless you stop the process or close the computer.
caffeinate -is bash -c "docker compose --env-file .env.testing stop ingestion parsing validation && ./bootstrap/silver_snapshot.sh wipe && env ENRICH=1 docker compose --env-file .env.testing -f docker-compose.bootstrap.yml run --rm bootstrap && ./bootstrap/silver_snapshot.sh trim 20260727171500 && ./bootstrap/silver_snapshot.sh export && docker compose --env-file .env.testing start ingestion parsing validation"
# 1.
docker compose --env-file .env.testing stop ingestion parsing validation  # stop live writes

# 2.
./bootstrap/silver_snapshot.sh wipe

# 3.
ENRICH=1 docker compose --env-file .env.testing -f docker-compose.bootstrap.yml run --rm bootstrap # WARNING: enrichment makes it take hours to download the full 30 days --- you might want to wrap this command: `caffeinate -is env ENRICH=1 docker compose --env-file .env.testing -f docker-compose.bootstrap.yml run --rm bootstrap`.

# 4.
./bootstrap/silver_snapshot.sh trim 20260727171500   # the last slice in the release

# 5.
./bootstrap/silver_snapshot.sh export

# 6.
docker compose --env-file .env.testing start ingestion parsing validation  # re-start live writes

# 7.
# To ensure everything is operational again, you may continue from point 3 of Startup (not of this list of points!) onwards.
```

### Shutdown

**OPTIONAL**: Reset silver so the next startup can restore the seed cleanly
``` bash
# All steps below as one line:
docker compose --env-file .env.testing stop ingestion parsing validation && ./bootstrap/silver_snapshot.sh wipe && docker exec pipeline_processing python3 -c "import main; main.recompute_all()"

docker compose --env-file .env.testing stop ingestion parsing validation        # Stops live data from being ingested

./bootstrap/silver_snapshot.sh wipe                                             # Removes all silver data and prevents repeated restores accumulating physical duplicates

docker exec pipeline_processing python3 -c "import main; main.recompute_all()"  # Recomputes gold from the now-empty silver layer (can take a few minutes, but this is indeed not an action the system would normally perform under any circumstance)
```

**NOT OPTIONAL**: shutdown procedure (that does not destroy the volumes, so user preferences and data for users stay intact)
``` bash
docker compose -f 5-serving/docker-compose.serving.yml down && docker compose --env-file .env.testing down && docker compose --env-file .env.testing -f docker-compose.stores.yml down
```

## Intended mode: startup, shutdown

### Startup

### Shutdown

