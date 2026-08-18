# Production-level design of the "Global-News Event Radar for Geopolitical and Supply Risk"

This app gives users a customised and informative briefing of news of events that may threatent their supply chain's stability.

This repository is "production-level" meaning that it deliberately avoids using one Docker app that puts together the data pipeline, the LTS systems, and the frontend into one Docker app to run `docker compose` on. Rather, these aspects are entirely separate, each with its own set of Docker containers making one Docker app to call `docker compose` on. Like in a production-level design, the only point of contact between the pipeline (which involves ingestion, parsing, validation, processing, and the backend of the storage) and the LTS systems is `pipeline_network` (a DNS with no published ports). Also like in a production-level design, the Docker app for the frontend of the service is even more separate from the other two Docker apps, with its only point of contact being a URL to the backend (`BACKEND_URL`): this way, multiple such frontend Docker apps can be run connected to the same backend. Starting up every part of the radar together is thus deliberately and necessarily a multi-step process that avoids using one Docker app for everything, even if Docker apps can already have multiple containers.

Two modes are in place: testing mode and intended mode. **Only testing mode can be run on one machine, so only testing mode can be used when running the whole radar to observe and evaluate how the radar operates.** In contrast, intended mode is boilerplate to make testing mode's data distributed across machines: intended mode was curated at every relevant step of the creation of testing mode, but to test intended mode, significant hardware is required, without which Docker Swarm will refuse to start intended mode. In a nutshell, testing mode is the only mode that can run on one machine, and intended mode is a curated but untested draft of the setup to make the same pipeline run on multiple machines.

## Testing mode: startup, shutdown, explanation of the processes

### Startup


```bash
# 1. Stores

docker compose --env-file .env.testing -f docker-compose.stores.yml up -d

# 2. Pipeline (Ingestion; Parsing; Validation and Storage; Processing; Serving Backend)
docker compose --env-file .env.testing up -d --build

# 3. OPTIONAL: Silver data from articles spanning 27/06/2026 at 17:15 to 27/07/2026 at 17:15.
# This "seeded" data was chosen to make the testing-mode radar not empty at startup: the radar gets updated with the latest news every 15 minutes, and automatically drops news older than 365 days every midnight. Even seeding over a year of data will leave a gap in testing mode: all the per-15-minutes slices between the latest seeded/stored data and the data from the moment a tester starts up the testing-mode radar with these instructions.
./bootstrap/silver_snapshot.sh restore

# 4. Three test profiles.
# Without any profiles, the gold layer stays empty. With any profiles at all, the PostgreSQL store for gold-layer news data and the MongoDB store for user preferences are idempotently created.
python3 5-serving/seed_test_users.py          # Needs `requests` on the host. Also, may have to type `python` instead of `python3`.

# 5. OPTIONAL: Gold data from articles spanning 27/06/2026 at 17:15 to 27/07/2026 at 17:15.
# If you ran step 3., this will be computed anyways, but will take around 2 minutes. This command's execution might take less than 10 seconds.
./bootstrap/gold_snapshot.sh restore

# 6. Service frontend.
# While all previous steps just need to be run on the backend, this is the only code that each frontend machine will need.
docker compose -f 5-serving/docker-compose.serving.yml up --build
```

Here in testing mode, backend machines and frontend machines are the same one machine, but these steps are still kept separate to keep the production-level design (to also have distribution across machines, see intended mode below).

### Shutdown

### Explanation of the process

## Intended mode: startup, shutdown, explanation of the processes

### Startup

### Shutdown

### Explanation of the process

