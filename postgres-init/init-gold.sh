#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════
# Patroni bootstrap hook — creates the gold role, database and schema, once.
#
# Single-machine mode gets the schema from the postgres image's own
# /docker-entrypoint-initdb.d hook. Patroni does NOT use that hook: it runs
# initdb itself when it bootstraps the cluster, so the image's entrypoint
# scripts never execute. `bootstrap.post_init` is Patroni's equivalent, and it
# fires exactly once — on the node that wins the initial leader election, before
# the cluster accepts client connections. The other two nodes are cloned from
# that leader with pg_basebackup, so they receive all of this as part of the
# clone rather than by running it again.
#
# ORDERING, which is easy to get wrong: post_init runs BEFORE Patroni creates the
# accounts listed under `bootstrap.users`. So the role cannot be assumed to exist
# here and is created by this script — otherwise CREATE DATABASE ... OWNER radar
# fails with `role "radar" does not exist`, Patroni treats the bootstrap as
# failed, renames the data directory to data.failed, and the cluster never
# initialises at all.
#
# Patroni passes a libpq connection string as $1, pointing at the freshly
# initialised local cluster.
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

CONNSTR="$1"
DB="${POSTGRES_DB:-radar}"
OWNER="${POSTGRES_USER:-radar}"
OWNER_PW="${POSTGRES_PASSWORD:-radar}"

echo "post_init: creating role ${OWNER}"
# Idempotent: a redeploy onto an existing data directory must not fail here.
psql "$CONNSTR" -v ON_ERROR_STOP=1 -c "
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${OWNER}') THEN
    CREATE ROLE ${OWNER} LOGIN CREATEDB PASSWORD '${OWNER_PW}';
  END IF;
END
\$\$;"

echo "post_init: creating database ${DB} owned by ${OWNER}"
# CREATE DATABASE cannot run inside a DO block or a transaction, and has no
# IF NOT EXISTS, so absorb the "already exists" case explicitly.
psql "$CONNSTR" -v ON_ERROR_STOP=1 -tAc \
     "SELECT 1 FROM pg_database WHERE datname = '${DB}'" | grep -q 1 \
  || psql "$CONNSTR" -v ON_ERROR_STOP=1 -c "CREATE DATABASE ${DB} OWNER ${OWNER}"

echo "post_init: applying the gold schema"
# Applied AS the radar role, so the three tables are owned by the account the
# pipeline and serving layer connect as, exactly as in single-machine mode.
psql "${CONNSTR} dbname=${DB}" -v ON_ERROR_STOP=1 \
     -c "SET ROLE ${OWNER}" -f /bootstrap/01_schema.sql

echo "post_init: gold schema ready"
