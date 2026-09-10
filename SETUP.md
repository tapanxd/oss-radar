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
- Expiration: 90 days or longer — an expired token silently kills collection
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

- Docker Desktop with **6GB+ allocated to the VM**. The default 2GB on macOS kills Airflow containers silently — this is the most common first-day failure.
- Python 3.11
- `make`, `git`

## B2. Local Postgres

Two databases in one container. `airflow` for Airflow's metadata, `warehouse` for dbt. **Never point dbt at the metadata DB.**

`docker-compose.yml`:

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: radar
      POSTGRES_PASSWORD: radar
      POSTGRES_DB: warehouse
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./init:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "radar"]
      interval: 5s
      retries: 10

volumes:
  pgdata:
```

`init/01-airflow-db.sql`:

```sql
create database airflow;
```

## B3. Seed dev from production

Develop against real data, not an empty schema:

```bash
pg_dump "$NEON_URL" --schema=raw --no-owner --no-privileges \
  | psql "postgresql://radar:radar@localhost:5432/warehouse"
```

Re-run whenever you want fresher data. Neon's branching feature can do this more elegantly later — worth using for CI.

## B4. dbt

```bash
pip install dbt-core dbt-postgres
dbt init radar          # choose postgres
```

`~/.dbt/profiles.yml`:

```yaml
radar:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: radar
      password: radar
      dbname: warehouse
      schema: dbt_tapan        # your own schema — this is the convention
      threads: 4
    prod:
      type: postgres
      host: "{{ env_var('NEON_HOST') }}"
      user: "{{ env_var('NEON_USER') }}"
      password: "{{ env_var('NEON_PASSWORD') }}"
      dbname: "{{ env_var('NEON_DB') }}"
      schema: analytics
      threads: 4
      sslmode: require
```

Verify with `dbt debug` before writing a single model.

## B5. Airflow

Pull the official compose file and **strip it to LocalExecutor** — the default ships CeleryExecutor with Redis and a separate worker, which triples memory use for no benefit here.

- Set `AIRFLOW__CORE__EXECUTOR: LocalExecutor`
- Remove the `redis` and `airflow-worker` services
- Point `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` at the `airflow` database, not `warehouse`
- **On Linux:** set `AIRFLOW_UID=$(id -u)` in `.env`, or DAG files end up root-owned and unwritable
- Pin an exact Airflow version. 2.x and 3.x differ on `Dataset` vs `Asset` and some import paths.

Install dbt into the Airflow image so `BashOperator` can call it directly. A separate dbt container means solving networking and volume mounts you don't need to solve.

---

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

- [ ] Two separate databases, dbt pointed at `warehouse`
- [ ] `dbt debug` passes
- [ ] Airflow UI reachable, LocalExecutor, metadata DB separate

---

## Things that will bite you

**Actions disables scheduled workflows after ~60 days of repo inactivity.** You'll be committing while building so it's unlikely — but if you pause, the cron dies *silently* and history gets a permanent hole. Check the Actions tab whenever you return after a break.

**Neon scales compute to zero when idle.** The first connection after a quiet period takes a few seconds. `connect_timeout` is set to 30 in `collect.py` for this reason — don't lower it.

**Token expiry kills collection silently.** Calendar a reminder for a week before expiry.

**GitHub's secondary rate limits are separate from the 5,000/hour budget.** You can have 4,000 remaining and still get throttled for firing too fast. The collector handles this; remember it when you migrate to mapped Airflow tasks and are tempted to raise concurrency.
