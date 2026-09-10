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
LOCAL_URL="postgresql://radar:radar@localhost:5432/warehouse"   # inside the container

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

echo "==> Dumping raw schema from Neon and restoring into local warehouse..."
# Streamed dump -> restore. No intermediate file, deliberately: Git Bash on
# Windows rewrites a container path like /tmp/x.sql into C:/Users/.../tmp/x.sql
# (MSYS path conversion) and pg_dump then fails on a path that does not exist
# in the container. Streaming sidesteps path translation entirely.
#
# --clean --if-exists so re-running replaces the schema rather than erroring on
# existing objects. --schema=raw so nothing outside raw is touched.
# pipefail (set above) makes a pg_dump failure fail the whole pipeline rather
# than being masked by a successful psql.
docker exec "$CONTAINER"   pg_dump "$DATABASE_URL"     --schema=raw     --no-owner     --no-privileges     --clean     --if-exists | docker exec -i "$CONTAINER" psql "$LOCAL_URL" -v ON_ERROR_STOP=1 -q

echo "==> Verifying..."
docker exec "$CONTAINER" psql "$LOCAL_URL" -At -c \
  "select 'observations=' || count(*) || ' repos=' || count(distinct repo_full_name)
        || ' from=' || min(observed_date) || ' to=' || max(observed_date)
   from raw.repo_observations;"
docker exec "$CONTAINER" psql "$LOCAL_URL" -At -c \
  "select 'collection_runs=' || count(*) from raw.collection_runs;"

echo "==> Dev warehouse seeded."
