# CLAUDE.md

This file is auto-loaded by Claude Code at the start of every session in this
repo. Keep it current — it's the substitute for conversation history that
doesn't carry over between sessions.

---

## What this project is

`oss-radar` tracks a categorized set of ~49 GitHub repos (coding agents, MCP
servers, skills/plugins, eval harnesses, orchestration, model-serving) and
emits a weekly digest of **material** changes — archived, relicensed,
transferred, major/breaking releases, maintainer churn, going stale. Framed
against "why not just GitHub notifications" — ranking by materiality and
queryable history are the differentiators. See `DESIGN.md` §1 for the full
pitch and non-goals.

**Full design authority: `DESIGN.md`.** If this file and `DESIGN.md` ever
disagree, `DESIGN.md` wins — update this file to match, don't code against
the conflict.

---

## Current status

- **Phase 0 (collector) — BUILT AND RUNNING.** Do not modify without reason.
  Daily GitHub Actions cron writes to Neon (`raw.repo_observations`,
  `raw.collection_runs`). It has been running since 2026-09-09 and
  must not be interrupted — the whole project depends on uninterrupted daily
  history that cannot be backfilled.
- **Phase 1 (dbt warehouse) — COMPLETE.** All 13 models built: staging, the
  five intermediate detectors, the union, and five marts. 174 dbt tests + 13
  pytest tests pass. sqlfluff clean. `make ci` runs the whole CI sequence
  locally; `make reset` rebuilds from an empty volume.
- **CI is green and production exists on Neon.** `analytics_staging`,
  `analytics_intermediate`, `analytics_marts` and `analytics_seeds` are built
  and match dev exactly. The `prod-manifest` artifact is published on every
  push to `main`, so Slim CI has something to defer against. **Slim CI's
  deferral path has executed on a real PR** (#1, 2026-09-12): found the prod
  manifest, built 7 of 13 models into `ci_pr_1_*` on Neon, deferred the
  other 6 to `analytics_*`, fallback step skipped, schemas dropped after.
- **Phase 2 (Airflow) — BUILT AND RUNNING LOCALLY.** Three DAGs on Airflow
  3.3.1, LocalExecutor, metadata in the `airflow` database. All three have run
  green: `radar_transform_daily` (dbt layer by layer, emits an Asset),
  `radar_digest_weekly` (Asset-triggered, renders `digests/`), and
  `radar_collect` (49 dynamically mapped tasks, one per repo, 25s end to end).
  Asset triggering verified: a transform success produced an
  `asset_triggered__` digest run. `make airflow-up` → http://localhost:8081.
- **Phase 3 (polish) — README and dashboard done; DAG screenshots not yet.**
  Metabase runs under the `dashboard` compose profile with its app DB in a
  third database (`metabase`) in the local Postgres. `make dashboard` builds
  the whole thing through the API from `scripts/metabase_setup.py`;
  idempotent, re-run after editing a query. Login is in `.env.example`.

Before touching anything, run this and don't proceed if it looks wrong:

```sql
select count(*), max(observed_date), min(observed_date)
from raw.repo_observations;
```

### What is not done yet

- **Only one digest committed** (`digests/2026-W37.md`, partial week).
  DESIGN.md §11 wants three or more from real accumulated changes.
- **No Airflow screenshots in the README** — the 49-task mapped grid and the
  Asset dependency between the two DAGs. Take them from http://localhost:8081.
- **`astronomer-cosmos`** not adopted; dbt runs via BashOperator. Nice-to-have.
- **Digest `publish` does not commit.** It reports files written; committing
  `digests/` is a human step (git inside a Windows-mounted container is
  fragile). Documented in the DAG.

### Start here next session

1. `make up && make airflow-up` (Docker Desktop must be running).
2. Run the sanity query below; expect ≥4 days of observations.
3. `make seed && make build` — 175 tests. Then `make digest` to refresh
   `digests/2026-W37.md` (still partial until 2026-09-13).
4. Phase 3 remaining: the two README screenshots (Airflow grid + Assets page
   at http://localhost:8081). `make metabase-up && make dashboard` brings the
   dashboard back at http://localhost:3000/dashboard/2.
5. ~~Confirm the Neon password was rotated.~~ Done 2026-09-12: reset in
   Neon, `DATABASE_URL` secret and `.env` updated, verified by a manual
   `collect.yml` run (run 7, 49/49) and an Airflow restart.

Every DAG is currently UNPAUSED locally. `radar_collect` is in dry-run and
`radar_transform_daily` builds the local warehouse only, so nothing writes to
Neon from Airflow unless `DBT_TARGET=prod` is set.

### Airflow gotcha that already bit once

**Unpausing a DAG runs its most recent missed interval immediately, even with
`catchup=False`.** The first time `radar_collect` was unpaused in dev it wrote
49 rows to Neon before anyone chose to. Harmless (upsert; identical to the
cron's rows) but unintended. `dry_run` now defaults to `True` unless
`DBT_TARGET=prod`. The same applies to `radar_transform_daily`: unpausing it
creates a `scheduled__` run straight away.

### Security note

The Neon password was briefly exposed in a public Actions log on 2026-09-10:
CI derived `NEON_*` from the `DATABASE_URL` secret and wrote them to
`$GITHUB_ENV`, and GitHub echoes env vars in a step's log header. GitHub masks
`secrets.*` automatically but NOT values derived from them. The run was
deleted and `::add-mask::` is now applied before anything is written to
`$GITHUB_ENV`. The password was rotated on 2026-09-12; the exposed one is
dead.

Anything that derives a value from a secret must `::add-mask::` it first.

### Data reality check

Three days of history (2026-09-09 to 2026-09-11). Every windowed signal is
correctly refusing to report: 0 star spikes, caveat "insufficient history:
3 of 7 days". Models that need no history already work — 10 digest lines
including the first genuine **critical metadata event**: `dbt-labs/dbt-core`
was renamed to `dbt-labs/dbt` on 2026-09-11, caught by the numeric-id
partitioning as a state change on one repo. Also 2 breaking releases, 3
archived repos, 5 transferred repos, 1 stale repo. Do not "fix" the empty
velocity output; it is the guard working.

The digest grain is one row per repo per change type per week — cline/cline
shipping two patch releases in one week collapses to "2 patch releases
v0.0.24 -> v0.0.26". This was forced by real data on day 3.

---

## Stack (locked — do not substitute without discussion)

dbt Core 1.12 · Apache Airflow · Docker · PostgreSQL 18 · Neon · Metabase

- **Two Postgres instances.** Neon = production, holds the observation
  history, collector writes here, never point destructive dbt runs at it
  casually. Local Postgres in Docker = dev, seeded from a Neon dump, plus
  Airflow's metadata DB. **Airflow's metadata DB and the dbt warehouse DB are
  never the same database.**
- **LocalExecutor for Airflow**, not the default Celery/Redis setup in the
  official docker-compose — strip those services out.
- **dbt version 1.8+** (needed for unit tests / `contract: enforced`). Running
  dbt-core 1.12.4 / dbt-postgres 1.11.0 on Python 3.13. Note 1.12 wants generic
  test args nested under an `arguments:` key; the old flat form still works but
  emits a deprecation.
- **Local Postgres is `postgres:18`, matching Neon (18.6).** Not the
  `postgres:16` older docs mention — `pg_dump` cannot dump a server newer than
  itself, so pg16 cannot seed from Neon at all. `scripts/seed_dev.sh`
  preflights this and says which tag to bump to if Neon changes major.
- **Host port is `RADAR_PG_PORT` (default 5433)**, read by both
  `docker-compose.yml` and `profiles.yml`. Change it in `.env` only.
- Install dbt into the Airflow image so `BashOperator` can call it directly.
  Don't stand up a separate dbt container.

---

## Non-negotiable conventions

- Layers: `staging/` (`stg_`, views, cast/rename only) → `intermediate/`
  (`int_`, the real logic) → `marts/` (`dim_`/`fct_`/`agg_`, consumable).
- Every model gets a `.yml` entry with a description and ≥1 test. A model
  with no description does not get merged.
- Every mart model states its **grain** as the first line of its description.
- All mart models: `contract: enforced`.
- Every `fct_change_events` row must have non-null `evidence`. This is
  tested, not optional — see `DESIGN.md` §6.
- Secrets via `.env` (gitignored) locally, repo secrets in Actions. Never in
  YAML, never in a DAG file.

---

## The one design decision to know before writing any model

**We are NOT using `dbt snapshot`.** The collector's append-only observation
log makes native dbt snapshots redundant and strictly worse (a missed
snapshot run loses a change permanently and can't be backfilled; the
observation log can always be recomputed from). SCD2 is built manually in
`int_repo_state_history` using window functions over the observation log.

If you find yourself reaching for `snapshots/` — stop, re-read `DESIGN.md`
§5, and use the derived-SCD2 pattern instead.

---

## Build order

Follow `DESIGN.md` §9 phase-by-phase. Within Phase 1, build in this order —
each layer depends on the one before it and should have passing tests before
moving on:

1. `stg_repo_observations`, `stg_collection_runs`
2. `int_repo_state_history` (the SCD2 spine — everything else depends on this)
3. `int_metadata_changes`, `int_release_events`, `int_activity_signals`,
   `int_collection_gaps`
4. `int_change_events` (the union)
5. Marts: `dim_repos`, `fct_change_events`, then the `agg_*` models
6. Tests from `DESIGN.md` §8, then Slim CI

Semver parsing and breaking-change detection are gnarly — `DESIGN.md` §6 has
real observed tag examples (`2026.8.31`, `2026-07-28`, unprefixed `3.3.1`)
pulled from an actual dry run. Use those as test fixtures, not invented ones.

---

## Repo layout

```
collector/          Phase 0 — built, don't touch casually
  collect.py
  schema.sql
  repos.yml         edit this to add/remove tracked repos, nothing else
  .env.example
.github/workflows/
  collect.yml       daily cron — built
  ci.yml            Slim CI — deferral path proven on PR #1
Makefile             every workflow: up/seed/build/reset. `make help` lists them
docker-compose.yml   dev Postgres: `warehouse` + `airflow` + `metabase` databases; Airflow; Metabase
init/                first-boot SQL for the dev container
scripts/seed_dev.sh  reload dev warehouse from Neon; read-only against prod
scripts/metabase_setup.py  the dashboard, as code; `make dashboard`
dbt_project/         all 13 models built and tested
  profiles.yml       in-repo, not ~/.dbt; targets dev / prod / ci
  macros/            parse_semver.sql, materiality.sql (ranking weights)
  seeds/             breaking_change_markers.csv
tests/               pytest: collector idempotency, rate limits, config
.sqlfluff            lint config; `make lint` / `make fix`
pytest.ini
dags/                three DAGs: radar_transform_daily, radar_digest_weekly, radar_collect
include/             render_digest.py (the digest renderer), radar_assets.py (shared Asset)
airflow/Dockerfile   apache/airflow:3.3.1 + dbt + collector deps
digests/             weekly digests land here — commit them, don't gitignore
DESIGN.md            source of truth
```

---

## Things that will bite you (already learned the hard way)

- GitHub Actions disables scheduled workflows after ~60 days of repo
  inactivity. Silent failure — check the Actions tab if the repo goes quiet.
- Neon scales compute to zero when idle; first connection after a quiet
  period has a few seconds of cold-start latency. Don't set aggressive
  timeouts on connections to it.
- GitHub's secondary rate limits are enforced separately from the primary
  5,000/hour budget — you can have budget remaining and still get throttled
  for firing requests too fast. Relevant when Phase 2 raises mapped-task
  concurrency.
- Airflow 3.x renamed `Dataset` to `Asset` and changed import paths. We are
  on 3.3.1: `from airflow.sdk import dag, task, Asset, Param`;
  `from airflow.providers.standard.operators.bash import BashOperator`.
- Never import one DAG file from another. The dag-processor executes the
  imported file too and attributes the DAG to whichever it parsed last. Shared
  objects (the Asset) live in `include/radar_assets.py`.
- Inside the Airflow container Postgres is `postgres:5432`, not
  `localhost:5433`. The dbt dev target reads `RADAR_PG_HOST`/`RADAR_PG_PORT`
  and compose sets them; the renderer and DAGs do the same.
- `airflow db migrate` must run before the api-server starts; `airflow-init`
  does it and the others `depends_on` its completion.
- We're on `psycopg` v3 (`psycopg[binary]`), not `psycopg2` — the
  collector's Python is 3.13 and `psycopg2-binary` doesn't reliably have
  wheels for it. Don't reintroduce `psycopg2` imports. (dbt-postgres pulls its
  own `psycopg2-binary`; that's dbt's internal driver, not our code, and is
  not a violation of this rule.)
- `.env` values must be QUOTED. `DATABASE_URL` contains an `&`; unquoted, a
  shell `source` backgrounds the assignment and silently loses the variable.
- Don't `include .env` in the Makefile — Make doesn't strip the quotes and
  docker compose then rejects `RADAR_PG_PORT` as `"5433"`. Recipes source it
  through bash instead.
- Git Bash rewrites container paths (`/tmp/x.sql` -> `C:/Users/.../tmp/x.sql`).
  Anything passing a container-side path to `docker exec` needs
  `MSYS_NO_PATHCONV=1` or, better, no absolute path at all.
- Re-seeding fails once dbt has built views on `raw` unless the schema is
  dropped CASCADE — `pg_dump --clean` emits a plain DROP that errors on
  dependents.
- sqlfluff's Postgres dialect cannot parse `is distinct from` inside a `CASE`.
  Use the `(a is distinct from b)::int` cast form instead — same meaning, and
  it parses. Do NOT switch to `<>`; that silently loses NULL transitions.
- Slim CI must run against the SAME Neon database as prod. `--defer` rewrites
  unchanged `ref()`s to production relations, so a fresh empty Postgres has
  nothing to defer to and deferral silently degrades into a full build.
- CI parses `DATABASE_URL` into `NEON_*` components at runtime. dbt-postgres
  cannot take a connection URL, and five separate secrets would drift out of
  sync with the one the collector already uses.

---

## Definition of done

See `DESIGN.md` §11 — it is now a live checklist with 8 of 11 ticked. The
remaining three: three committed digests (needs time) and two Airflow
screenshots.
