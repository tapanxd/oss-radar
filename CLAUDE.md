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
- **Phase 1 (dbt warehouse) — NOT STARTED.** This is the current work.
- **Phase 2 (Airflow) — NOT STARTED.**
- **Phase 3 (polish/README) — NOT STARTED.**

Before touching anything, run this and don't proceed if it looks wrong:

```sql
select count(*), max(observed_date), min(observed_date)
from raw.repo_observations;
```

---

## Stack (locked — do not substitute without discussion)

dbt Core · Apache Airflow · Docker · PostgreSQL · Neon

- **Two Postgres instances.** Neon = production, holds the observation
  history, collector writes here, never point destructive dbt runs at it
  casually. Local Postgres in Docker = dev, seeded from a Neon dump, plus
  Airflow's metadata DB. **Airflow's metadata DB and the dbt warehouse DB are
  never the same database.**
- **LocalExecutor for Airflow**, not the default Celery/Redis setup in the
  official docker-compose — strip those services out.
- **dbt version 1.8+** (needed for unit tests / `contract: enforced`).
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
  ci.yml            Slim CI — TO BUILD in Phase 1
dbt_project/         TO BUILD
dags/                TO BUILD in Phase 2
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
- Airflow 3.x renamed `Dataset` to `Asset` and changed import paths. Check
  the pinned version before writing asset-triggered DAG code.
- We're on `psycopg` v3 (`psycopg[binary]`), not `psycopg2` — the
  collector's Python is 3.13 and `psycopg2-binary` doesn't reliably have
  wheels for it. Don't reintroduce `psycopg2` imports.

---

## Definition of done

See `DESIGN.md` §11 for the full checklist. Don't mark Phase 1 complete
without: Slim CI actually working (not just configured), every change event
carrying real evidence, and the snapshots-vs-derived-SCD2 decision written
into the eventual README.
