# Ingestion Layer

This layer fetches GDELT v2 Events and Mentions files and hands raw CSV files to
the parsing layer through the shared Docker volume.

## Runtime Contract

The important contract with `2-parsing` is stable:

- ZIP staging: `/data/raw/zip`
- CSV hand-off: `/data/raw/csv`
- state file: `/data/state/last_seen.json`

In local execution, the same structure is created under the project-level
`data/` directory. The base directory can be overridden with
`INGESTION_DATA_DIR`.

## Files

- `paths.py`: centralizes all ingestion filesystem paths.
- `poller.py`: polls GDELT's `last15minutes.txt`, downloads Events and Mentions,
  extracts CSV files, and leaves them in the hand-off directory.
- `backfill.py`: downloads historical Events and Mentions ZIP files and extracts
  their CSV files into the same hand-off directory used by the poller.
- `gdelt_urls.py`: generates historical GDELT URLs for backfill.

`2-parsing/main.py` watches `/data/raw/csv`, waits for matching
`<timestamp>.export.CSV` and `<timestamp>.mentions.CSV` files, then publishes the
pair to `/data/latest_files` for validation.
