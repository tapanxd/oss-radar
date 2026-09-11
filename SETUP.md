# SETUP — oss-radar

Two parts. **Part A is urgent** — it starts history accumulating, and history cannot be backfilled. Do it today, in about 30 minutes. Part B can wait until you're ready to build the warehouse.

---

# Part A — get the collector running (do this first)

## A1. Create the repo

```bash
mkdir oss-radar && cd oss-radar
git init
mkdir -p collector .github/workflows digests
```

Drop in the files:

```
collector/collect.py
collector/schema.sql
collector/repos.yml
collector/requirements.txt
.github/workflows/collect.yml     ← the file named collect.yml
```

```bash
cat > .gitignore <<'EOF'
.env
__pycache__/
*.pyc
.venv/
EOF
```

## A2. GitHub token

Settings → Developer settings → Personal access tokens → **Fine-grained tokens**.

- Repository access: **Public repositories (read-only)**
- Expiration: none. A token that expires silently kills collection, and the
  gap in history cannot be backfilled. If you do set an expiry, calendar a
  reminder a week out.
- No account permissions needed

Copy it now; you can't view it again.

## A3. Neon database

Sign up at neon.tech, create a project, and copy the connection string from the dashboard. It looks like:

```
postgresql://user:pass@ep-xxx.region.aws.neon.tech/neondb?sslmode=require
```

Keep `?sslmode=require` — Neon rejects unencrypted connections.

Create the schema:

```bash
psql "postgresql://...your-neon-url..." -f collector/schema.sql
```

No local psql? Paste `schema.sql` into Neon's SQL Editor in the dashboard.

## A4. Fill in your repo list

Edit `collector/repos.yml`. Replace the four placeholders with **30–40 repos you actually care about**, each with a category and priority.

This is the one step worth doing carefully rather than fast. The tracked set shapes every downstream metric, and the project is far more convincing when it reflects a real interest than a scraped top-N list.

## A5. Test locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r collector/requirements.txt

export GH_TOKEN="your_token"
export DATABASE_URL="your_neon_url"

# Fetch and print, write nothing
python collector/collect.py --config collector/repos.yml --dry-run
```

Check the first log line: it should report a rate limit of **5000**, not 60. If it says 60, the token isn't being applied — the script exits with that error rather than silently collecting at the unauthenticated rate.

When the dry run looks right:

```bash
python collector/collect.py --config collector/repos.yml
```

Verify:

```sql
select count(*), max(observed_date) from raw.repo_observations;
select * from raw.collection_runs order by run_id desc limit 1;
```

Then run it a **second time** and confirm the count doesn't change. That's the upsert working — the idempotency property everything downstream depends on.

## A6. Schedule it

Push, then add two secrets under Settings → Secrets and variables → Actions:

| Name | Value |
|---|---|
| `GH_TOKEN` | your token |
| `DATABASE_URL` | your Neon URL |

> Secret names cannot start with `GITHUB_` — that prefix is reserved. Hence `GH_TOKEN`.

Trigger it manually first: Actions tab → **collect** → Run workflow. Confirm it goes green and rows land. Only then trust the cron.

## A7. Confirm it's alive tomorrow

Check the Actions tab the next day. A scheduled run should have fired around 06:15 UTC.

**Then leave it alone for three to four weeks** while you build. That accumulation is the entire reason Part A comes first.

---

# Part B — development environment

Not urgent. Start when you're ready for Phase 1.

## B1. Prerequisites

- Docker Desktop with **6GB+ available to the VM**. Check with
  `docker info --format '{{.MemTotal}}'` and divide by 1024^3.
  - On macOS/Hyper-V there is a Resources → Memory slider defaulting to 2GB,
    which kills Airflow containers silently. Raise it.
  - On Windows/WSL2 there is no slider: WSL2 takes 50% of host RAM by default,
    which clears the bar on a 16GB machine without doing anything. To cap it,
    create `%USERPROFILE%\.wslconfig` with `[wsl2]` / `memory=8GB`, then
    `wsl --shutdown`. Note this applies to every WSL distro, not just Docker.
  - The **Docker Engine** settings page edits `daemon.json` and has nothing to
    do with memory; its `defaultKeepStorage` is build-cache disk.
- Python 3.13 (what the collector and dbt are running on here)
- `make`, `git`. On Windows: `choco install make`.

## B2. Local Postgres

Everything is in `docker-compose.yml` and driven by the `Makefile`:

```bash
make up      # starts Postgres, waits for the healthcheck
```

Two databases in one container. `airflow` for Airflow's metadata, `warehouse`
for dbt. **Never point dbt at the metadata DB.**

Three things differ from what an older version of this doc said, each for a
reason worth knowing:

**The image is `postgres:18`, not `postgres:16`.** Neon production runs 18.6.
`pg_dump` refuses to dump a server whose major version is newer than its own,
so a pg16 client cannot seed from Neon at all. The major tag (`18`, not
`18.6`) keeps picking up patch releases, and any 18.x can dump any other 18.x.
The only thing that breaks this is Neon moving to a new major -
`scripts/seed_dev.sh` preflights exactly that and tells you which tag to bump
to.

**postgres:18 moved its data directory.** It stores data under a
major-version subdirectory so `pg_upgrade --link` works without crossing a
mount boundary, so the volume mounts at `/var/lib/postgresql`, not
`/var/lib/postgresql/data`. Mounting at the old path makes the container
crash-loop with a confusing "unused mount/volume" error.

**The host port is 5433, not 5432,** so a native Postgres install does not
collide. It comes from `RADAR_PG_PORT` in `.env`; `docker-compose.yml` and
dbt's `profiles.yml` both read it with the same default, so changing it in
`.env` is enough. Inside the compose network the port is always 5432.

## B3. Seed dev from production

Develop against real data, not an empty schema:

```bash
make seed
```

No Postgres client tools are needed on Windows: `pg_dump` runs inside the
container, which also guarantees the client version matches the server. The
dump is streamed rather than written to a file, because Git Bash rewrites a
container path like `/tmp/x.sql` into a Windows path and `pg_dump` then fails
on a path that does not exist in the container.

The script is safe to re-run. It drops the local `raw` schema with CASCADE
before restoring, so it works even after dbt has built views on top of `raw` -
a plain `pg_dump --clean` fails there with "cannot drop table ... because
other objects depend on it". Anything CASCADE drops is rebuilt by `make build`,
and the script tells you when that is needed.

It is **read-only against Neon** and never writes to production.

Neon's branching feature can do this more elegantly later - worth using for CI.

## B4. dbt

Already installed into `.venv` and configured. Verify with:

```bash
make debug     # both connection targets
make build     # run and test every model
make fresh     # source freshness against the collector's output
```

`profiles.yml` lives in `dbt_project/`, **not** `~/.dbt/`. dbt resolves
profiles from `--profiles-dir`, then `DBT_PROFILES_DIR`, then the working
directory, then `~/.dbt/`. Keeping it in the repo means a reviewer can clone,
`make up && make seed`, and run dbt with no hidden machine-local setup - worth
more here than following the `~/.dbt` convention.

No secrets are in it. The `dev` credentials are the throwaway ones from
`docker-compose.yml`; `prod` reads the Neon connection from `NEON_*`
environment variables supplied by `.env` or CI secrets.

**`.env` values must be quoted** (`KEY="value"`). `DATABASE_URL` contains an
`&`, and an unquoted value makes `source` background the assignment and
silently lose the variable.

`make reset` rebuilds the whole dev environment from nothing: wipe the volume,
start Postgres, reseed from Neon, build and test every model.

## B5. Airflow

Built. Everything is in `docker-compose.yml` under the `airflow` profile, so
`make up` still starts only Postgres.

```bash
make airflow-build   # once: apache/airflow:3.3.1 + dbt + collector deps
make airflow-up      # api-server, scheduler, dag-processor -> http://localhost:8081
make airflow-down    # stop Airflow, keep Postgres
make airflow-logs    # tail scheduler + dag-processor
```

No login locally (`SIMPLE_AUTH_MANAGER_ALL_ADMINS=true`; never set that anywhere
public). Port 8081 because `dbt docs serve` holds 8080.

What was decided, and why it differs from the older text above:

- **Three services, not the official compose file's seven.** LocalExecutor
  runs tasks inside the scheduler, so there is no Celery worker, Redis or
  Flower. 3.x split DAG parsing into its own `dag-processor` service, so that
  one is present. No triggerer: nothing uses deferrable operators.
- **Metadata in the `airflow` database, dbt in `warehouse`.** Same container,
  never the same database.
- **dbt is baked into the Airflow image.** `BashOperator` calls it directly.
- **Pinned to 3.3.1.** `from airflow.sdk import dag, task, Asset, Param`.
- **DAGs start paused.** Unpausing one runs its most recent missed interval
  *immediately*, even with `catchup=False`. `radar_collect` therefore defaults
  `dry_run` to true unless `DBT_TARGET=prod`; it wrote to Neon once before
  that guard existed.
- **`DBT_TARGET`** (default `dev`) decides whether Airflow builds the local
  warehouse or Neon. Set it in `.env` to switch.

Three DAGs: `radar_collect` (49 mapped tasks), `radar_transform_daily` (dbt,
emits an Asset), `radar_digest_weekly` (Asset-triggered, renders `digests/`).
Their module docstrings are the reference.

## Verification checklist

**Part A** — the only part with a deadline:

- [ ] `schema.sql` applied to Neon
- [ ] `repos.yml` has your real 30–40 repos
- [ ] Local run wrote rows; second run didn't duplicate them
- [ ] Rate limit reported 5000, not 60
- [ ] Both Actions secrets set
- [ ] Manual workflow run green
- [ ] Scheduled run confirmed the following day

**Part B**, when you get there:

- [x] Two separate databases, dbt pointed at `warehouse`
- [x] `make debug` passes on both dev and prod targets
- [x] `make seed` reloads from Neon and is safe to re-run
- [x] `make build` green
- [x] `make reset` rebuilds the whole environment from an empty volume
- [x] Airflow UI reachable at :8081, LocalExecutor, metadata DB separate
- [x] All three DAGs have run green; Asset trigger observed

---

## Things that will bite you

**Actions disables scheduled workflows after ~60 days of repo inactivity.** You'll be committing while building so it's unlikely — but if you pause, the cron dies *silently* and history gets a permanent hole. Check the Actions tab whenever you return after a break.

**Neon scales compute to zero when idle.** The first connection after a quiet period takes a few seconds. `connect_timeout` is set to 30 in `collect.py` and in `profiles.yml`'s prod target for this reason — don't lower it.

**`make include` and `.env` quoting pull in opposite directions.** Make's `include` does not strip quotes, so `RADAR_PG_PORT="5433"` reaches docker compose as six characters and it rejects the port. The quotes cannot be dropped either, because `DATABASE_URL` contains an `&`. The Makefile therefore sources `.env` through bash per-recipe instead of including it.

**GitHub's secondary rate limits are separate from the 5,000/hour budget.** You can have 4,000 remaining and still get throttled for firing too fast. The collector handles this; remember it when you migrate to mapped Airflow tasks and are tempted to raise concurrency.
