#!/usr/bin/env bash
# ═════════════════════════════════════════════════════════════════════════════
# Gold snapshot — export the gold layer to SQL, or restore it.
#
# The companion of silver_snapshot.sh, and it exists for the same reason: so a
# fresh clone has content in seconds instead of waiting for the pipeline to
# compute it. Restoring silver takes ~7 s; rebuilding gold from that silver takes
# ~2 minutes, because the Spark job runs once per user profile. This skips that
# wait.
#
#   ./bootstrap/gold_snapshot.sh export    # gold tables -> data/gold_seed
#   ./bootstrap/gold_snapshot.sh restore   # data/gold_seed -> gold tables
#
# ── Why gold CAN be snapshotted, when the design says it should not be ───────
# silver_snapshot.sh says gold is deliberately never snapshotted, "so the gold
# can never drift from what the pipeline would have produced". This does not break
# that rule, but the reason is narrower than it first appears.
#
# On a FIRST RUN the snapshot is only a head start: restoring silver advances the
# watermark, which fires a recompute, which replaces everything here with a freshly
# computed result. The pipeline wins, automatically.
#
# That argument holds ONLY when gold is empty. Restoring over a gold layer the
# pipeline has already built is destructive and does NOT self-correct, because the
# watermark advances on new SILVER, and restoring gold leaves silver untouched.
# `restore` therefore refuses when gold is non-empty — see the guard below.
#
# What you get is a dashboard with real cards immediately, instead of an empty
# one that fills in later.
#
# ── The one thing that must be fixed on restore: age_days ────────────────────
# Every other column is absolute (timestamps, ids, text) and keeps its meaning
# forever. `age_days` does not: it is `today - event_date`, computed when the row
# was written, and the serving layer filters on it (`age_days <= briefing_days`).
# A dump taken a month ago would place every article a month too young and show
# cards that should have aged out. So `restore` recomputes it from `event_date`,
# which IS absolute. Without that line this snapshot would silently rot.
#
# ── When to re-export ────────────────────────────────────────────────────────
# Gold is DERIVED, so this file is only valid for the filter logic that produced
# it. Re-export after changing anything that decides what reaches gold: the
# parsing filters, the per-user geo/keyword predicate, the de-duplication rules,
# or the gold schema. Otherwise a clone briefly shows results the current code
# would not produce — briefly, because the recompute corrects it.
#
# Requires the stores tier to be running, and the gold tables to exist.
# ═════════════════════════════════════════════════════════════════════════════
set -euo pipefail

# Testing mode uses a fixed container name. Intended mode runs PostgreSQL under
# Patroni in a Swarm stack, where names carry a task id, so resolve by service
# label and fall back to the compose name — the same approach silver_snapshot.sh
# takes for ClickHouse.
_resolve_pg_container() {
  if [ -n "${PG_CONTAINER:-}" ]; then printf '%s' "$PG_CONTAINER"; return; fi
  local task
  task=$(docker ps -q \
    --filter "label=com.docker.swarm.service.name=${STORES_STACK:-radar-stores}_postgres-1" \
    | head -1)
  if [ -n "$task" ]; then printf '%s' "$task"; return; fi
  printf 'pipeline_postgres'
}

PG_CONTAINER="$(_resolve_pg_container)"
SEED_DIR="${SEED_DIR:-data/gold_seed}"
PG_USER="${POSTGRES_USER:-radar}"
PG_DB="${POSTGRES_DB:-radar}"
DUMP="$SEED_DIR/gold.sql"

pg() { docker exec -i "$PG_CONTAINER" "$@"; }

case "${1:-}" in

  export)
    mkdir -p "$SEED_DIR"
    echo "exporting gold from $PG_CONTAINER ..."
    # --data-only: the schema is owned by postgres-init/01_schema.sql (and, in
    # intended mode, by Patroni's bootstrap hook). Shipping CREATE TABLE here too
    # would give two sources of truth for the same tables.
    # --column-inserts: portable, readable, and immune to column-order changes
    # between the machine that exported and the one that restores.
    docker exec "$PG_CONTAINER" pg_dump -U "$PG_USER" -d "$PG_DB" \
        --data-only --column-inserts \
        --table=articles --table=user_articles > "$DUMP"
    rows=$(pg psql -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT (SELECT count(*) FROM articles) || ' articles, ' ||
                (SELECT count(*) FROM user_articles) || ' user_articles'")
    printf '  %s  ->  %s (%s)\n' "$rows" "$DUMP" "$(du -h "$DUMP" | cut -f1)"
    echo "re-export this whenever the filter logic or the gold schema changes"
    ;;

  restore)
    [ -s "$DUMP" ] || { echo "missing or empty: $DUMP" >&2; exit 1; }

    printf 'waiting for the gold schema '
    for attempt in $(seq 1 60); do
      if pg psql -U "$PG_USER" -d "$PG_DB" -tAc \
           "SELECT to_regclass('public.articles') IS NOT NULL" 2>/dev/null | grep -q '^t$'; then
        printf ' ready after %ds\n' "$(( (attempt - 1) * 2 ))"
        break
      fi
      printf '.'
      sleep 2
    done
    if ! pg psql -U "$PG_USER" -d "$PG_DB" -tAc \
           "SELECT to_regclass('public.articles') IS NOT NULL" 2>/dev/null | grep -q '^t$'; then
      echo "ERROR: the gold tables do not exist." >&2
      echo "       They are created at first database start by" >&2
      echo "       postgres-init/01_schema.sql. Is the stores tier running?" >&2
      exit 1
    fi

    # ── Refuse to overwrite a gold layer the pipeline has already built ──────
    # This is a FIRST-RUN accelerator. If gold already holds rows, the pipeline
    # has computed something from the silver actually present — which may include
    # live slices beyond the 30-day seed, and profiles beyond the three seeded
    # ones. The seed is then strictly WORSE than what is already there, and
    # restoring would:
    #   * delete every card derived from live data, and
    #   * empty the dashboard for any user who is not one of the three seeded
    #     accounts, since the dump contains no rows for them.
    #
    # And it would NOT self-correct promptly. The watermark trigger fires only
    # when max(DATEADDED) in silver moves strictly FORWARDS; restoring gold does
    # not touch silver, so nothing recomputes until the next new GDELT slice
    # arrives — or never, if the pipeline is stopped.
    existing=$(pg psql -U "$PG_USER" -d "$PG_DB" -tAc "SELECT count(*) FROM articles")
    if [ "$existing" -gt 0 ] && [ "${FORCE:-0}" != "1" ]; then
      echo "REFUSING: gold already holds $existing article rows." >&2
      echo >&2
      echo "  The seed is a first-run accelerator and would REPLACE them with the" >&2
      echo "  $(grep -c '^INSERT INTO public.articles' "$DUMP") rows captured when it was exported —" >&2
      echo "  losing anything derived from live data, and emptying the dashboard" >&2
      echo "  for any account that is not one of the three seeded ones." >&2
      echo >&2
      echo "  To rebuild gold from the silver you actually have (usually what you" >&2
      echo "  want), ask the pipeline to recompute:" >&2
      echo "    docker exec pipeline_processing python3 -c 'import main; main.recompute_all()'" >&2
      echo >&2
      echo "  To overwrite anyway:  FORCE=1 $0 restore" >&2
      exit 1
    fi
    [ "$existing" -gt 0 ] && echo "FORCE=1: replacing $existing existing article rows"

    echo "restoring gold ..."
    # Emptied first, so restoring twice cannot violate the primary keys or leave
    # rows from an older export behind. Safe because gold is derived: a recompute
    # rebuilds whatever this discards.
    pg psql -U "$PG_USER" -d "$PG_DB" -q -c "TRUNCATE articles, user_articles"
    # -o /dev/null: the dump opens with a set_config() SELECT whose result table
    # would otherwise be printed as if it were output.
    pg psql -U "$PG_USER" -d "$PG_DB" -q -o /dev/null -v ON_ERROR_STOP=1 < "$DUMP"

    # age_days is the one relative value in the table — see the header. Recompute
    # it from event_date so a snapshot taken any time ago still ages correctly.
    fixed=$(pg psql -U "$PG_USER" -d "$PG_DB" -tAc \
        "WITH u AS (UPDATE articles
                    SET age_days = (CURRENT_DATE - event_date::date)
                    WHERE event_date IS NOT NULL RETURNING 1)
         SELECT count(*) FROM u")
    rows=$(pg psql -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT (SELECT count(*) FROM articles) || ' articles, ' ||
                (SELECT count(*) FROM user_articles) || ' user_articles'")
    printf '  %s  (age_days recomputed for %s rows)\n' "$rows" "$fixed"
    echo "gold restored."
    echo "The next recompute replaces this with a freshly computed result — but"
    echo "note it fires when SILVER advances, which restoring gold does not do."
    echo "With the pipeline running, the next GDELT slice (<= 15 min) triggers it."
    echo "To rebuild immediately:"
    echo "  docker exec pipeline_processing python3 -c 'import main; main.recompute_all()'"
    ;;

  *)
    echo "usage: $0 {export|restore}" >&2
    exit 1
    ;;
esac
