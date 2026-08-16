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

# Which container to run clickhouse-client in. Testing mode uses plain Compose,
# where the container is named exactly this. Intended mode runs the stores as a
# SWARM STACK, and Swarm ignores `container_name` — the task is called something
# like radar-stores_clickhouse-s1r1.1.<taskid>, with a different id every time it
# is rescheduled, so the name cannot be hard-coded. Resolve it by service label
# instead, and fall back to the Compose name.
#
# Run this on the machine hosting s1r1: `docker exec` is local to one daemon, so
# the container has to be here. (Which machine that is, is fixed by the placement
# constraint in docker-stack.stores.yml.)
_resolve_ch_container() {
  if [ -n "${CH_CONTAINER:-}" ]; then printf '%s' "$CH_CONTAINER"; return; fi
  local swarm_task
  swarm_task=$(docker ps -q \
    --filter "label=com.docker.swarm.service.name=${STORES_STACK:-radar-stores}_clickhouse-s1r1" \
    | head -1)
  if [ -n "$swarm_task" ]; then printf '%s' "$swarm_task"; return; fi
  printf 'pipeline_clickhouse_s1r1'
}
CH_CONTAINER="$(_resolve_ch_container)"
SEED_DIR="${SEED_DIR:-data/silver_seed}"
TABLES=(gdelt_events gdelt_mentions)
# The last 15-minute slice covered by the committed seed. `trim seed` uses it, so
# the window need not be remembered. Update it if the seed is ever rebuilt over a
# different period.
SEED_LAST_SLICE="${SEED_LAST_SLICE:-20260727171500}"

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
    # The silver schema is owned by the validation layer, which creates it
    # ON CLUSTER the first time it reaches ClickHouse. On a fresh clone that
    # happens seconds after the pipeline starts, so wait rather than failing with
    # "Unknown table expression" if this is run the moment the containers are up.
    printf 'waiting for the silver schema (created by the validation layer) '
    for attempt in $(seq 1 60); do
      if ch --query "EXISTS TABLE ${TABLES[0]}" 2>/dev/null | grep -q '^1$'; then
        printf ' ready after %ds\n' "$(( (attempt - 1) * 5 ))"
        break
      fi
      # A dot per attempt: this wait can last minutes on a cold start, and silence
      # for that long is indistinguishable from a hang.
      printf '.'
      sleep 5
    done
    printf '\n'
    if ! ch --query "EXISTS TABLE ${TABLES[0]}" 2>/dev/null | grep -q '^1$'; then
      echo "ERROR: ${TABLES[0]} still does not exist after 5 minutes." >&2
      echo "       Is the pipeline running? Start it with:" >&2
      echo "         docker compose --env-file .env.testing up -d --build" >&2
      exit 1
    fi

    for t in "${TABLES[@]}"; do
      f="$SEED_DIR/$t.parquet"
      [ -s "$f" ] || { echo "missing or empty: $f — skipped"; continue; }
      echo "restoring $t ..."
      # The Distributed table routes each row to its shard, exactly as a live
      # write would, so the sharding stays consistent with the cluster layout.
      # insert_deduplicate=0 is REQUIRED, not an optimisation. ReplicatedMergeTree
      # remembers the checksums of recently inserted blocks and silently skips a
      # block it has seen before. Restoring the same seed file after rows were
      # deleted — by `trim`, or by the retention job — inserts byte-identical
      # blocks, which ClickHouse would drop as duplicates: the command reports
      # success and restores NOTHING. Correctness does not depend on this
      # de-duplication anyway, because both tables are ReplacingMergeTree and
      # collapse genuine duplicate rows by key at merge/FINAL time.
      ch --query "INSERT INTO $t SETTINGS insert_deduplicate = 0 FORMAT Parquet" < "$f"
      rows=$(ch --query "SELECT count() FROM $t FINAL")
      # On a MULTI-SHARD cluster this count can read LOW — it is taken the moment
      # the insert returns, while the Distributed table is still handing rows to
      # the second shard and ReplicatedMergeTree is still copying them between
      # replicas. Measured on the six-node cluster: it printed 51,914 immediately
      # after restoring, and 103,972 (the true total) a few seconds later. The
      # data is not lost and nothing needs re-running; only the figure below is
      # premature. Testing mode has one shard and one replica, so it is exact
      # there. Re-run `SELECT count() FROM gdelt_events FINAL` to confirm.
      printf '  %-16s now %s rows\n' "$t" "$rows"
    done
    echo "silver restored — the processing watermark trigger will build the gold"
    ;;

  recreate)
    # Apply a CHANGED table definition — a new ORDER BY, a new column, a new
    # index — to a volume that already holds the old one.
    #
    # Needed because the schema is created with CREATE TABLE IF NOT EXISTS, so on
    # an existing volume a changed definition is simply ignored: the tables keep
    # whatever shape they were first created with. ClickHouse also cannot ALTER a
    # sorting key into a different order (only append columns to it), so the
    # tables have to be dropped and rebuilt.
    #
    # Safe, because silver is reproducible: the committed seed restores in seconds
    # and the live pipeline re-polls anything newer from GDELT.
    echo "dropping the silver tables (both local and Distributed, ON CLUSTER) ..."
    for t in gdelt_events gdelt_mentions; do
      ch --query "DROP TABLE IF EXISTS ${t} ON CLUSTER gnews_cluster SYNC" >/dev/null
      ch --query "DROP TABLE IF EXISTS ${t}_local ON CLUSTER gnews_cluster SYNC" >/dev/null
      printf '  %-16s dropped\n' "$t"
    done
    echo
    echo "Now restart the VALIDATION layer — it owns the schema and calls"
    echo "ensure_tables() once, at startup, so nothing recreates the tables until"
    echo "it restarts:"
    echo
    echo "    docker compose --env-file .env.testing restart validation     # testing"
    echo "    docker service update --force radar_validation                # intended"
    echo
    echo "then re-fill silver:"
    echo
    echo "    ./bootstrap/silver_snapshot.sh restore"
    ;;

  wipe)
    for t in "${TABLES[@]}"; do
      ch --query "TRUNCATE TABLE ${t}_local ON CLUSTER gnews_cluster" >/dev/null
      printf '  %-16s emptied\n' "$t"
    done
    ;;

  trim)
    # Drop everything published AFTER a given slice, so the store holds exactly
    # one known period. Used when rebuilding the seed: the live pipeline keeps
    # polling while a backfill runs, so silver ends up holding the backfill
    # window PLUS whatever arrived meanwhile, and a seed built from that would
    # ship an arbitrary slice of "today" to everyone who clones the repository.
    #
    # Events and mentions carry the slice timestamp in different columns, and both
    # are strings of fixed width, so a lexicographic comparison is also chronological.
    cutoff="${2:-}"
    # `trim seed` is the common case: keep exactly the window the committed seed
    # covers, discarding whatever the live pipeline has polled since.
    [ "$cutoff" = "seed" ] && cutoff="$SEED_LAST_SLICE"
    case "$cutoff" in
      ??????????????) ;;
      *) echo "usage: $0 trim {seed|<YYYYMMDDHHMMSS>}   (e.g. 20260727171500)" >&2; exit 1 ;;
    esac
    echo "removing rows published after $cutoff ..."
    # mutations_sync=2 waits for every replica, so the counts printed below are final.
    ch --query "ALTER TABLE gdelt_events_local ON CLUSTER gnews_cluster
                DELETE WHERE DATEADDED > '$cutoff' SETTINGS mutations_sync = 2" >/dev/null
    ch --query "ALTER TABLE gdelt_mentions_local ON CLUSTER gnews_cluster
                DELETE WHERE MentionTimeDate > '$cutoff' SETTINGS mutations_sync = 2" >/dev/null
    for t in "${TABLES[@]}"; do
      rows=$(ch --query "SELECT count() FROM $t FINAL")
      printf '  %-16s %8s rows remain\n' "$t" "$rows"
    done
    echo "events  now span: $(ch --query "SELECT concat(min(DATEADDED),' .. ',max(DATEADDED)) FROM gdelt_events FINAL")"
    echo "mentions now span: $(ch --query "SELECT concat(min(MentionTimeDate),' .. ',max(MentionTimeDate)) FROM gdelt_mentions FINAL")"
    ;;

  *)
    echo "usage: $0 {export|restore|recreate|wipe|trim {seed|<YYYYMMDDHHMMSS>}}" >&2
    exit 1
    ;;
esac
