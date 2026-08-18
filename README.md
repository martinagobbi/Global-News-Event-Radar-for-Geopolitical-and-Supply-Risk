# Production-level design of the "Global-News Event Radar for Geopolitical and Supply Risk"

This app gives users a customised and informative briefing of news of events that may threatent their supply chain's stability.

This repository is "production-level" meaning that it deliberately avoids using one Docker app that puts together the data pipeline, the LTS systems, and the frontend into one Docker app to run `docker compose` on. Rather, these aspects are entirely separate, each with its own set of Docker containers making one Docker app to call `docker compose` on. Like in a production-level design, the only point of contact between the pipeline (which involves ingestion, parsing, validation, processing, and the backend of the storage) and the LTS systems is `pipeline_network` (a DNS with no published ports). Also like in a production-level design, the Docker app for the frontend of the service is even more separate from the other two Docker apps, with its only point of contact being a URL to the backend (`BACKEND_URL`): this way, multiple such frontend Docker apps can be run connected to the same backend. Starting up every part of the radar together is thus deliberately and necessarily a multi-step process that avoids using one Docker app for everything, even if Docker apps can already have multiple containers.

Two modes are in place: testing mode and intended mode. **Only testing mode can be run on one machine, so only testing mode can be used when running the whole radar to observe and evaluate how the radar operates.** In contrast, intended mode is boilerplate to make testing mode's data distributed across machines: intended mode was curated at every relevant step of the creation of testing mode, but to test intended mode, significant hardware is required, without which Docker Swarm will refuse to start intended mode. In a nutshell, testing mode is the only mode that can run on one machine, and intended mode is a curated but untested draft of the setup to make the same pipeline run on multiple machines.

## Testing mode: startup, shutdown, explanation of the processes, memory use

### Startup


```bash
# 1. stores (these create pipeline_network)

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

### Shutdown

### Explanation of the process

### Memory use

## Intended mode: startup, shutdown, explanation of the processes, memory use

### Startup

### Shutdown

### Explanation of the process

### Memory use
