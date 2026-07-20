# dev_tool_for_time_gauging — TEMPORARY, DELETE WHEN DONE

Throwaway measurement tool. **Not part of the pipeline** — nothing imports it,
no service depends on it. Delete this whole folder when you no longer need it.

## What it answers

> For each 15-minute GDELT CSV, how long does it take to reach the silver layer
> (ClickHouse), and how does that depend on the CSV's size?

## Run it

The pipeline's `/data` lives in a Docker volume (not readable directly from the
host on macOS), so run the tool in a container that mounts it **read-only**,
with this folder bind-mounted so the report lands here:

```bash
VOL=$(docker volume ls -q | grep shared_data | head -1)
docker run --rm -it \
  -v "$VOL":/data:ro \
  -v "$PWD/dev_tool_for_time_gauging":/out \
  -e DATA_DIR=/data -e OUT_DIR=/out \
  -w /out python:3.11-slim python pipeline_timing.py
```

Run it from the repo root, while the pipeline is up. Ctrl-C to stop; re-running
appends to the same report.

## Output

`pipeline_timing_report.csv`, in this folder — one line per CSV:

| column | meaning |
|---|---|
| `csv` | which file: `events`, `mentions`, `translation.events`, `translation.mentions` |
| `slice` | the GDELT 15-minute slice id (`YYYYMMDDHHMMSS`) |
| `gdelt_publish_utc` | that slice id as UTC — GDELT's reference publish time |
| `rows` | rows in that CSV |
| `rows_source` | `raw` = original count; `latest` = counted post-filter (tool started mid-flight) |
| `seconds_gdelt_to_silver` | publish time → in silver. **Includes GDELT's own lag + the poller's up-to-15-min wait** — the true "age of the data". |
| `seconds_pipeline_to_silver` | first seen by the pipeline → in silver. **This is the one to plot against `rows`.** |

## How it detects "reached silver"

No database access needed — it reads the pipeline's own progress markers:

1. ingestion writes the CSV into `/data/raw/csv`
2. parsing republishes it into `/data/latest_files` and deletes the raw copy
3. validation appends it to ClickHouse **and then deletes it from `latest_files`**

So the file *disappearing from `latest_files`* is the silver-arrival signal.

## Two caveats worth knowing when reading the numbers

- **Back-pressure is included.** Parsing only publishes a new pair once
  `latest_files` is empty, so `seconds_pipeline_to_silver` includes any time the
  slice spent queued behind the previous one. That's real latency, but it isn't
  "work" — if it dominates, validation (enrichment) is your bottleneck, not size.
- **Enrichment is network-bound.** Validation scrapes article text with a 10-min
  budget, so latency will correlate with *mention count* far more than with raw
  file size.
