# DESIGN — oss-radar

**Stack (locked):** dbt Core · Apache Airflow · Docker · PostgreSQL · Neon
**Supersedes:** `DESIGN-oss-health.md`, `DESIGN-oss-radar.md`
**Status:** Phases 0, 1 and 2 built and running. Phase 3 partially done (README, dashboard); Airflow screenshots outstanding. Definition of done in §11 tracks the specifics.

---

## 1. Problem

The AI tooling ecosystem ships new repositories constantly — coding agents, MCP servers, skill and plugin collections, eval harnesses. Watching forty of them on GitHub produces four hundred notifications and no signal.

`oss-radar` tracks a categorized set of repositories, detects **material** changes, ranks them, and emits a weekly digest. The question it answers: *what changed this week that I actually need to know about.*

### Why not just GitHub notifications

This must be addressed in the README's first screen, because every reviewer will think it within ten seconds.

- GitHub notifies per-repo. This aggregates across an ecosystem and **ranks by materiality** — a repo being archived and a patch release are not the same event and should not arrive identically.
- GitHub gives you a feed. This gives you **queryable history**. "Which MCP repos changed license last year" and "which projects went stale after a maintainer left" are questions a notification feed cannot answer at all.

If the project can't answer those better than a feed, re-scope rather than ship.

## 2. Non-goals

- Not a security scanner (OpenSSF Scorecard exists).
- Not a package registry mirror.
- No GH Archive firehose — turns this into a volume exercise and destroys the rate-limit narrative.
- No LLM summarization in v1.
- No automated discovery in v1 (see §9).

---

## 3. Architecture

```
GitHub REST API
      │
      ▼
┌─────────────────────────────────────────┐
│  PHASE 0 — collector (BUILT)            │
│  GitHub Actions cron, daily             │
│  collector/collect.py                   │
└─────────────────────────────────────────┘
      │  appends 1 observation per repo per day
      ▼
┌─────────────────────────────────────────┐
│  Neon Postgres — raw.repo_observations  │  ← production warehouse
└─────────────────────────────────────────┘
      │
      ├──────────────────────────────┐
      ▼                              ▼
┌──────────────────┐      ┌────────────────────────┐
│ Airflow (local   │      │ Local Postgres (docker)│
│ docker-compose)  │      │ dev warehouse          │
│ orchestrates dbt │      └────────────────────────┘
└──────────────────┘
      │
      ▼
   dbt Core  →  staging → intermediate → marts
      │
      ▼
   digests/YYYY-WW.md  +  Metabase dashboard
```

**Two Postgres instances, deliberately:**

- **Neon** — production. Holds the accumulating observation history. The collector writes here; it must never stop.
- **Local Postgres in Docker** — development. Airflow metadata DB plus a dev warehouse. Seeded from a Neon dump so you develop against real data without risking production.

Airflow's metadata database is **always separate** from any warehouse database. Pointing dbt at Airflow's metadata DB is a real mistake people make and is miserable to debug.

---

## 4. Phase 0 — the collector (built)

Lives in `collector/`. Runs on GitHub Actions cron, writes to Neon, requires no Airflow and no dbt.

**Why it exists separately from the pipeline:** change history only accumulates in real time. GitHub will not tell you what a repo's license was three weeks ago. The collector starts the clock on day one so that by the time the modelling layer is ready, weeks of genuine change history are waiting.

### Key design decisions

**Append-only observation log, not a current-state table.** One row per repo per day. Volume is ~30 MB/year at 40 repos — irrelevant. The benefit is that history is complete and reconstructable rather than dependent on a downstream process having run.

**Full API payloads stored as `jsonb`.** If a field turns out to be needed in two months, it is already there. Re-collecting past state is impossible, so store everything.

**Upsert on `(repo_full_name, observed_date)`.** Re-running on the same day corrects rather than duplicates. This is the idempotency property the pipeline needs, established at the source.

**Partial success is success.** If the rate budget runs low the collector stops cleanly, records what it skipped in `raw.collection_runs`, and exits zero. Only a total failure alerts.

**`raw.collection_runs` exists so gaps are queryable.** A missed day is a recorded fact, not something discovered months later by noticing absent dates.

### Rate limiting

Authenticated REST allows 5,000 requests/hour, with **secondary limits enforced separately** from that budget — you can have 4,000 remaining and still be throttled for firing too fast. Verify current limits against GitHub's docs; they change. The collector stops at a floor of 100 remaining rather than draining to zero.

---

## 5. Phase 1 — the warehouse

### A decision to make explicitly: snapshots vs. derived SCD2

The earlier design assumed dbt snapshots as the change-detection mechanism. **The observation log makes them largely redundant, and the derived approach is better here.** Document this decision in the README — "I considered dbt snapshots and chose not to use them, because…" is a stronger interview answer than using them by default.

| | dbt snapshot | Derived from observation log |
|---|---|---|
| Missed run | Change lost permanently | Nothing lost — recomputable |
| Backfillable | No | Yes |
| Rerun-safe | Fragile | Fully |
| Storage | Compact | Larger (irrelevant here) |

Build SCD2 yourself: collapse consecutive identical observations into validity ranges using window functions. Same output shape as a snapshot, none of the fragility.

### Layers

**staging (`stg_`, views)**
`stg_repo_observations`, `stg_collection_runs`. Cast, rename, unpack `jsonb` where needed. No logic.

**intermediate (`int_`)**

- `int_repo_state_history` — **the SCD2 spine.** Collapses consecutive identical observations per repo into `valid_from` / `valid_to` ranges over the tracked attributes. Grain: one row per repo per state period.
- `int_metadata_changes` — unpivots state history into one row per repo per changed attribute per change event, with before/after values.
- `int_release_events` — one row per distinct release observed, with parsed semver, bump type, breaking-change flag.
- `int_activity_signals` — per repo: star velocity (7d vs trailing 90d mean), days since last push, staleness flag.
- `int_collection_gaps` — days where a repo has no observation. Feeds the honest "blind spot" reporting.
- `int_change_events` — **the union.** All change types normalized to one shape: `repo`, `category`, `detected_at`, `change_type`, `before_value`, `after_value`, `evidence`, `materiality`.

**marts**

- `dim_repos` — one row per tracked repo, current state.
- `fct_change_events` — one row per detected change. Core fact table.
- `agg_weekly_digest` — one row per repo per week per change, filtered above materiality threshold, ordered for rendering. **The headline model.**
- `agg_category_pulse` — one row per category per week: releases, net star velocity, repos gone stale.
- `agg_repo_timeline` — one row per repo per month.

Apply `contract: enforced` to all mart models. Three lines of YAML, and it is the modern data-contracts practice.

---

## 6. Change types and materiality

| `change_type` | Detected from | Materiality |
|---|---|---|
| `archived` | `is_archived` false → true | **critical** |
| `license_changed` | `license_spdx` diff | **critical** |
| `renamed_or_transferred` | `repo_full_name` diff, or a 404 | **critical** |
| `major_release` | new tag, semver major bump | **high** |
| `breaking_release` | release body matches breaking markers | **high** |
| `minor_release` | new tag, semver minor bump | medium |
| `went_stale` | previously active, no push in 180d | medium |
| `default_branch_changed` | `default_branch` diff | medium |
| `star_spike` | 7d velocity > 3× trailing 90d mean | low |
| `patch_release` | new tag, semver patch bump | low |
| `description_changed` | `description` diff | low |

**Semver parsing must handle reality:** `v` prefixes, prerelease suffixes, date versions (`2024.03.1`), non-semver tags (`release-4`). Tags that don't parse get `release_unclassified` at medium materiality rather than being silently dropped. Test this with pytest against the ugly cases.

**Breaking-change markers** live in a seed file so they're visible and testable: a `BREAKING CHANGE` block, conventional-commit `!` before the colon, headings containing "Breaking" / "Migration" / "Upgrade guide".

**Every change row carries `evidence`** — the actual before/after values or the matched span of the release note. A digest entry the reader can't verify without opening GitHub has no value over a notification. Non-negotiable.

**Ranking** = base materiality by type, +1 level for `priority: high` repos, −1 level for repos with no commits in a year. Weights live in one place. The README states plainly they are judgement, not a validated model.

---

## 7. Phase 2 — Airflow

Two DAGs, connected by an Airflow Dataset/Asset so the digest triggers on data arrival rather than a hopeful cron offset.

> Airflow 3.x renamed the 2.x `Dataset` concept to `Asset` and changed import paths. Pin an exact version and use the matching API.

### `radar_transform_daily`

```
wait_for_fresh_observations   (source freshness check against Neon)
        │
        ▼
dbt_run_staging → dbt_run_intermediate → dbt_run_marts
        │
        ▼
     dbt_test                        [emits asset: change_events]
```

`catchup=False` — GitHub cannot be queried as-of a past date, so backfill is meaningless. The collector plus observation log is the substitute.

### `radar_digest_weekly`

Triggered on the `change_events` asset, gated weekly.

```
build_digest → render_markdown → publish
```

`render_markdown` writes `digests/YYYY-WW.md`. **Commit these into the repo.** A reviewer reading a real generated digest understands the project in eight seconds; no schema diagram achieves that.

### Where the dynamic task mapping goes

Phase 0 removed the per-repo Airflow task, since the collector runs in Actions. To keep the mapped-task story, **migrate the collector into Airflow in Phase 2** as `extract_repo.expand(repo=tracked_repos)` — one task per repo, independently retryable — while leaving the Actions cron running as a fallback. Screenshot the mapped-task grid; thirty parallel repo tasks is the most compelling image in the repo.

Run dbt via `BashOperator` first. Upgrade to `astronomer-cosmos` (each dbt model as its own Airflow task) only after the pipeline works end to end.

---

## 8. Testing and CI

**dbt tests**
- `not_null` / `unique` / `relationships` throughout.
- `accepted_values` on `change_type`, `materiality`, `category` — an unexpected value fails the build rather than reaching the digest.
- Every `fct_change_events` row has non-null `evidence`. Zero exceptions.
- No change event predates the repo's first observation.
- A repo cannot be `archived` and produce a `major_release` in the same week.
- No duplicate `(repo, change_type, week)` in the digest.
- Source freshness on `raw.repo_observations`: warn 36h, error 72h.

**pytest**
- Collector idempotency: replay the same day twice, assert row count unchanged.
- Rate-limit backoff against a mocked 403.
- Semver parsing across the ugly cases.

**CI — Slim CI is the highest-signal thing in this repo.**

Run `dbt build --select state:modified+ --defer --state ./prod-manifest` on every PR, storing the production `manifest.json` as a CI artifact. This builds only changed models and their descendants, deferring unchanged upstream refs to prod. Almost no portfolio project has this, and it's the clearest marker of someone who has worked on a real dbt project. One afternoon of config.

**Neon database branching** — branch the database per CI run so tests execute against a real copy of prod rather than an empty schema. This is a genuine advantage of the Neon choice; use it and write it up.

**Environments** — `dev` and `prod` targets in `profiles.yml`, schema-prefixed per developer. Nobody pushes to `main`; CI gates the merge.

---

## 9. Phasing

Ship in this order. Do not start a phase before the previous one is genuinely done.

- **Phase 0 — collector.** Built. Must be running and accumulating before anything else matters.
- **Phase 1 — warehouse.** dbt models, change detection, tests, Slim CI. Local Postgres for dev, Neon as source.
- **Phase 2 — orchestration.** Airflow, two DAGs, asset trigger, collector migrated to a mapped task.
- **Phase 3 — polish.** README, architecture diagram, committed digests, dashboard, known limitations.
- **v2, only after all of the above.** Automated discovery: search API by topic plus awesome-list parsing, filtered and written to a *review* table, never auto-added. GitHub Trending has no official API — do not design around scraping it.

---

## 10. Failure scenarios (for the README)

**Rate-limit exhaustion mid-run.** Repos 1–18 complete, 19–30 stop cleanly, the run reports partial success, `collection_runs` records the skip, dbt builds on what's present, tomorrow's run fills the gap. Include the actual log output. A pipeline that has never failed is a worse story than one that fails correctly.

**A collection gap.** If the Actions cron is disabled (GitHub disables schedules on repos inactive ~60 days) or Neon is unreachable, days go missing. `int_collection_gaps` surfaces this and the digest reports the blind spot rather than implying continuous coverage. Being explicit about a real limitation reads as considerably more senior than claiming the pipeline is lossless.

---

## 11. Definition of done

- [x] Adding a repo requires editing only `repos.yml` — `radar_collect` reads it at run time
- [ ] Three or more real digests committed to `digests/`, from real accumulated changes — **one so far** (`2026-W37`, partial); needs two more weeks of collection
- [x] Every digest entry carries verifiable evidence — `evidence` is `NOT NULL` at the database level via contract on `fct_change_events`
- [x] Slim CI running on PRs, with the deferred-manifest setup working — proven on PR #1 (2026-09-12): deferred to the manifest from the previous `main` run, built 7 of 13 models into a per-PR schema on Neon, deferred the other 6 to `analytics_*`, full-build fallback skipped, CI schemas dropped afterwards
- [ ] Airflow mapped-task grid screenshot in README — the grid exists (49 green squares at http://localhost:8081), not yet captured
- [~] Asset-triggered digest DAG — built and verified (`asset_triggered__` runs observed); screenshot not yet captured
- [x] Collector idempotency proven by test — `tests/test_collector.py::test_rerunning_the_same_day_upserts_rather_than_duplicating`
- [x] Dashboard: category pulse over time, change feed, repo timeline — Metabase in compose (`make metabase-up`), built from code by `scripts/metabase_setup.py` (`make dashboard`): fourteen native-SQL questions over `agg_category_pulse`, `fct_change_events` and `agg_repo_timeline` in three dashboard sections
- [x] README opens by addressing "why not just GitHub notifications"
- [x] README documents the snapshots-vs-derived-SCD2 decision
- [x] Known limitations names collection gaps and star-velocity noise honestly — seven limitations listed
