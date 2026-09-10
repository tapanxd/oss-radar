#!/usr/bin/env bash
# Seed the local dev warehouse from the Neon production `raw` schema.
#
# Develop against real data, not an empty schema. Re-run whenever you want
# fresher observations.
#
# pg_dump runs INSIDE the postgres container rather than on the host: no
# Postgres client tools are needed on Windows, and the client version is
# guaranteed to match the server (pg_dump refuses to dump a newer server).
#
# READ-ONLY against Neon. This script never writes to production.
#
# Usage:  bash scripts/seed_dev.sh
set -euo pipefail

CONTAINER=radar-postgres
# Always 5432: this URL is used from INSIDE the container, where the port is
# fixed regardless of what RADAR_PG_PORT publishes on the host.
LOCAL_URL="postgresql://radar:radar@localhost:5432/warehouse"

if [ -f .env ]; then
  set -a; . ./.env; set +a
fi

if [ -z "${DATABASE_URL:-}" ]; then
  echo "DATABASE_URL is not set (expected in .env) — aborting." >&2
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "Container '$CONTAINER' is not running. Run: docker compose up -d" >&2
  exit 1
fi

# --- Preflight: major-version compatibility -------------------------------
# pg_dump refuses to dump a server whose MAJOR version is newer than its own.
# When Neon upgrades its major, this script would otherwise fail with a bare
# "server version mismatch" that reads like a connection problem. Check it up
# front and say exactly what to do.
CLIENT_MAJOR="$(docker exec "$CONTAINER" pg_dump --version | grep -oE '[0-9]+' | head -1)"
SERVER_MAJOR="$(docker exec "$CONTAINER" psql "$DATABASE_URL" -At -c 'show server_version' | grep -oE '^[0-9]+')"

echo "==> Postgres major: local client ${CLIENT_MAJOR}, Neon server ${SERVER_MAJOR}"

if [ "$SERVER_MAJOR" -gt "$CLIENT_MAJOR" ]; then
  cat >&2 <<MSG

ERROR: Neon is on Postgres ${SERVER_MAJOR}; the local client is ${CLIENT_MAJOR}.
pg_dump cannot dump a server newer than itself, so the seed cannot run.

Fix: bump the image tag in docker-compose.yml to postgres:${SERVER_MAJOR}, then

    docker compose down -v && docker compose up -d && bash scripts/seed_dev.sh

Note that -v wipes the local volume. That is fine: the dev warehouse is
disposable and is rebuilt from this dump. Production is untouched.

MSG
  exit 1
fi

if [ "$CLIENT_MAJOR" -gt "$SERVER_MAJOR" ]; then
  echo "WARNING: local Postgres (${CLIENT_MAJOR}) is a newer major than Neon (${SERVER_MAJOR})." >&2
  echo "         The dump will work, but dev no longer matches prod. Consider pinning" >&2
  echo "         docker-compose.yml to postgres:${SERVER_MAJOR}." >&2
fi

# --- Drop dependents, then reload ------------------------------------------
# pg_dump --clean emits plain DROP TABLE, which fails once dbt has built views
# on top of raw ("cannot drop table ... because other objects depend on it").
# So the schema is dropped with CASCADE here instead, and the dump is restored
# clean. This makes re-seeding repeatable no matter what has been built.
#
# CASCADE also drops any dbt view sitting on raw. That is safe - they are
# rebuilt by `dbt build` - but the script says so rather than doing it silently.
DEPENDENTS="$(docker exec "$CONTAINER" psql "$LOCAL_URL" -At -c "
  select count(*)
  from pg_depend d
  join pg_rewrite r on r.oid = d.objid
  join pg_class v on v.oid = r.ev_class
  join pg_class t on t.oid = d.refobjid
  join pg_namespace tn on tn.oid = t.relnamespace
  join pg_namespace vn on vn.oid = v.relnamespace
  where tn.nspname = 'raw' and vn.nspname <> 'raw';")"

if [ "${DEPENDENTS:-0}" -gt 0 ]; then
  echo "==> ${DEPENDENTS} dependent object(s) on raw will be dropped and must be rebuilt with 'make build'."
fi

echo "==> Dropping local raw schema..."
docker exec "$CONTAINER" psql "$LOCAL_URL" -v ON_ERROR_STOP=1 -q   -c "drop schema if exists raw cascade;"

echo "==> Dumping raw schema from Neon and restoring into local warehouse..."
# Streamed dump -> restore. No intermediate file, deliberately: Git Bash on
# Windows rewrites a container path like /tmp/x.sql into C:/Users/.../tmp/x.sql
# (MSYS path conversion) and pg_dump then fails on a path that does not exist
# in the container. Streaming sidesteps path translation entirely.
#
# No --clean: the schema was just dropped above. --schema=raw so nothing
# outside raw is touched. pipefail (set at the top) makes a pg_dump failure
# fail the whole pipeline rather than being masked by a successful psql.
docker exec "$CONTAINER"   pg_dump "$DATABASE_URL"     --schema=raw     --no-owner     --no-privileges | docker exec -i "$CONTAINER" psql "$LOCAL_URL" -v ON_ERROR_STOP=1 -q

echo "==> Verifying..."
docker exec "$CONTAINER" psql "$LOCAL_URL" -At -c \
  "select 'observations=' || count(*) || ' repos=' || count(distinct repo_full_name)
        || ' from=' || min(observed_date) || ' to=' || max(observed_date)
   from raw.repo_observations;"
docker exec "$CONTAINER" psql "$LOCAL_URL" -At -c \
  "select 'collection_runs=' || count(*) from raw.collection_runs;"

echo "==> Dev warehouse seeded."
