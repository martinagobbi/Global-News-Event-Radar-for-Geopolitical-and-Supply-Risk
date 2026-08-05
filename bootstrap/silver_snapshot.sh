#!/usr/bin/env bash
# ═════════════════════════════════════════════════════════════════════════════
# Silver snapshot — export the silver layer to Parquet, or restore it.
#
# The snapshot is what fills the silver volume when the repository is cloned.
# It is committed to the repository (data/silver_seed/), which is practical
# because the bronze-to-silver filter discards roughly 97% of each GDELT slice:
# 30 days of raw archives occupy ~410 MB, while the resulting silver is a few
# tens of MB. Restoring it is a single bulk INSERT per table, so the silver layer
# is populated in seconds rather than the days the 15-minute pipeline would need.
#
# Gold is deliberately NOT snapshotted. Once silver is restored, the processing
# layer's watermark trigger detects the change and builds the gold itself, so the
# gold can never drift from what the pipeline would have produced.
#
#   ./bootstrap/silver_snapshot.sh export    # silver volume  -> data/silver_seed
#   ./bootstrap/silver_snapshot.sh restore   # data/silver_seed -> silver volume
#   ./bootstrap/silver_snapshot.sh wipe      # empty the silver tables
#
# Requires the stores tier to be running.
# ═════════════════════════════════════════════════════════════════════════════
set -euo pipefail

CH_CONTAINER="${CH_CONTAINER:-pipeline_clickhouse_s1r1}"
SEED_DIR="${SEED_DIR:-data/silver_seed}"
TABLES=(gdelt_events gdelt_mentions)

ch() { docker exec -i "$CH_CONTAINER" clickhouse-client "$@"; }

case "${1:-}" in

  export)
    mkdir -p "$SEED_DIR"
    for t in "${TABLES[@]}"; do
      echo "exporting $t ..."
      # FINAL collapses the ReplacingMergeTree duplicates, so the snapshot holds
      # one row per key and needs no deduplication when restored.
      docker exec "$CH_CONTAINER" clickhouse-client \
        --query "SELECT * FROM $t FINAL FORMAT Parquet" > "$SEED_DIR/$t.parquet"
      rows=$(ch --query "SELECT count() FROM $t FINAL")
      printf '  %-16s %8s rows  %s\n' "$t" "$rows" \
        "$(du -h "$SEED_DIR/$t.parquet" | cut -f1)"
    done
    echo "snapshot written to $SEED_DIR"
    ;;

  restore)
    for t in "${TABLES[@]}"; do
      f="$SEED_DIR/$t.parquet"
      [ -s "$f" ] || { echo "missing or empty: $f — skipped"; continue; }
      echo "restoring $t ..."
      # The Distributed table routes each row to its shard, exactly as a live
      # write would, so the sharding stays consistent with the cluster layout.
      ch --query "INSERT INTO $t FORMAT Parquet" < "$f"
      rows=$(ch --query "SELECT count() FROM $t FINAL")
      printf '  %-16s now %s rows\n' "$t" "$rows"
    done
    echo "silver restored — the processing watermark trigger will build the gold"
    ;;

  wipe)
    for t in "${TABLES[@]}"; do
      ch --query "TRUNCATE TABLE ${t}_local ON CLUSTER gnews_cluster" >/dev/null
      printf '  %-16s emptied\n' "$t"
    done
    ;;

  *)
    echo "usage: $0 {export|restore|wipe}" >&2
    exit 1
    ;;
esac
