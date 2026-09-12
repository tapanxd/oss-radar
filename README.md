# oss-radar

Tracks 49 GitHub repositories across the AI tooling ecosystem — coding agents,
MCP servers, eval harnesses, orchestration frameworks, model serving — and
produces a weekly digest of the changes that actually matter, ranked, with
evidence attached to every line.

**Stack:** dbt Core 1.12 · Apache Airflow 3.3 · PostgreSQL 18 · Neon ·
GitHub Actions · Docker

---

## Why not just use GitHub notifications?

This is the first question anyone should ask, so it goes first.

Watching forty repositories on GitHub produces hundreds of notifications and no
signal. Every event arrives looking identical: a project being **archived** and
a routine patch release land in the same inbox, formatted the same way, at the
same priority.

Two things make this different from a feed:

**1. It ranks by materiality.** A repository going read-only, changing its
licence, or shipping a breaking change is not the same event as a patch bump,
and this does not present them as if they were. Ranking accounts for how much
you care about the repo (`priority` in `repos.yml`) and whether it is still
alive at all.

Here is an actual week of output — two days of collection, no hand-editing:

| # | Repo | Change | Ranked |
|---|------|--------|--------|
| 1 | `openai/codex` | `rust-v0.153.4` → `rust-v0.154.0` | **critical** |
| 2 | `promptfoo/promptfoo` | `code-scan-action-0.2.0` → `0.123.0` | **high** |
| 3 | `Arize-ai/phoenix` | tag namespace changed | medium |
| 4 | `cline/cline` | `desktop-v0.0.24` → `desktop-v0.0.25` | medium |
| 5 | `ollama/ollama` | `v0.33.3` → `v0.34.0` | medium |

Entry 1 outranks entry 2 because both shipped breaking changes, but Codex is
marked high priority in `repos.yml` and promptfoo is not. Entry 4 is only a
patch release, yet it appears at all because Cline is high priority.

A sixth release was detected the same day and is **deliberately absent**:
`crewAIInc/crewAI` `1.15.20` → `1.15.21`, an identical kind of patch bump to
Cline's, on a normal-priority repo. It ranks `low` and falls below the digest
threshold. That pair is the whole idea in one line — same event, different
verdict, because the ranking knows which repos you care about.

**Every entry carries evidence.** Entry 1 is flagged breaking because its
release notes say:

> The deprecated `codex mcp-server` entry point is no longer available. (#42993)

That quote is stored on the row. A digest line the reader cannot verify without
opening GitHub adds nothing over a notification, so `evidence` is `NOT NULL` at
the database level — enforced by a contract, not merely tested.

**2. It has queryable history.** *"Which MCP repos changed licence last year?"*
and *"which projects went quiet after a maintainer left?"* are questions a
notification feed cannot answer at all. Every observation is kept forever, and
state history is reconstructable from it.

That history also surfaces things a feed would never tell you, because they are
states rather than events. Of the 49 tracked repos:

- **3 are archived** — including `RooCodeInc/Roo-Code` (24.3k stars) and
  `huggingface/text-generation-inference` (10.9k stars)
- **5 have been transferred to a different owner** — `block/goose` is now
  `aaif-goose/goose`, `All-Hands-AI/OpenHands` is now `OpenHands/OpenHands`
- **1 has had no commits in 198 days** — and is also one of the transferred ones

None of these produce a change *event*, because they were already true when
tracking began. Reporting them as news would be a lie. They are surfaced in
`dim_repos` instead, flagged `was_archived_before_tracking` and
`is_renamed_from_config`.

---

## Architecture

```
                      GitHub REST API
                            │
                            ▼
        ┌───────────────────────────────────────┐
        │  COLLECTOR  ·  GitHub Actions, daily  │
        │  collector/collect.py                 │
        │  rate-limit aware, partial success OK │
        └───────────────────────────────────────┘
                            │  1 observation per repo per day, append-only
                            ▼
        ┌───────────────────────────────────────┐
        │  Neon Postgres  ·  raw.*              │  production
        └───────────────────────────────────────┘
                            │
             ┌──────────────┴───────────────┐
             ▼                              ▼
   ┌──────────────────┐          ┌────────────────────────┐
   │ local Postgres   │          │  dbt Core              │
   │ (docker) — dev,  │◀─────────│  staging →             │
   │ seeded from prod │  make    │  intermediate →        │
   └──────────────────┘  seed    │  marts                 │
                                 └────────────────────────┘
                                            │
                            ┌───────────────┴───────────────┐
                            ▼                               ▼
                   digests/YYYY-WW.md           Metabase dashboard
                   (Phase 2)                        (Phase 3)
```

### The model layers

```
staging/         cast and rename only, no logic
  stg_repo_observations
  stg_collection_runs

intermediate/    the real logic
  int_repo_state_history    SCD2 spine — everything depends on this
  int_metadata_changes      licence, rename, archive, branch, description
  int_release_events        semver parsing, breaking-change detection
  int_activity_signals      star velocity, staleness
  int_collection_gaps       days we have no data for
  int_change_events         the union, normalised and ranked

marts/           consumable, contract: enforced
  dim_repos                 one row per repo, current state
  fct_change_events         one row per detected change
  agg_weekly_digest         the headline model — renders the digest
  agg_category_pulse        one row per category per week
  agg_repo_timeline         one row per repo per month
```

---

## Design decisions worth defending

### Why not `dbt snapshot`?

dbt ships snapshots specifically for slowly-changing dimensions, and this
project does not use them. That is deliberate.

A snapshot captures change only from the moment it runs. **A missed snapshot run
loses that change permanently, and it cannot be backfilled** — GitHub will not
tell you what a repository's licence was three weeks ago.

Instead the collector writes an append-only observation log, and SCD2 is derived
from it with window functions in `int_repo_state_history`.

| | `dbt snapshot` | Derived from observation log |
|---|---|---|
| Missed run | change lost permanently | nothing lost, recomputable |
| Backfillable | no | yes |
| Rerun-safe | fragile | fully — it is a pure function of the source |
| Storage | compact | larger (~30 MB/year; irrelevant) |

The derived table can be dropped and rebuilt at any time and is identical every
time. That property is the whole argument.

### Identity is the numeric repo ID, not the name

State history is partitioned on GitHub's numeric repository ID, which survives
renames and transfers. Partitioning on the name would make a transfer look like
one repository dying and a different one being born — and five of the tracked
repos have already moved.

### The collector runs outside the pipeline

It is a plain Python script on an Actions cron, not an Airflow task. History
only accumulates in real time, so the clock had to start on day one, long before
the modelling layer existed. Phase 2 migrates it into Airflow as a mapped task
while leaving the cron running as a fallback.

### Volatile counts are excluded from state history

Stars change most days. Including them in the SCD2 fingerprint would open a new
state period daily and turn the history table into a verbose copy of the
observation log. They are handled as velocity in `int_activity_signals` instead.

---

## Running it

```bash
git clone https://github.com/tapanxd/oss-radar && cd oss-radar
cp .env.example .env        # add your Neon URL and a GitHub PAT

make up                     # start local Postgres, wait for healthcheck
make seed                   # pull the raw schema down from Neon (read-only)
make build                  # run and test every model
make digest                 # render digests/YYYY-WNN.md

make airflow-build          # once: build the Airflow image with dbt baked in
make airflow-up             # Airflow UI at http://localhost:8081

make metabase-up            # Metabase at http://localhost:3000
make dashboard              # build the dashboard from scripts/metabase_setup.py
```

`make help` lists everything. `make reset` rebuilds the whole dev environment
from an empty volume; `make ci` runs exactly what CI runs.

Airflow starts with all DAGs paused. Unpausing one runs its most recent missed
interval immediately, so `radar_collect` defaults to dry-run outside
production.

No Postgres client tools are needed — `pg_dump` runs inside the container, which
also guarantees the client version matches the server.

`dbt_project/profiles.yml` lives in the repo rather than `~/.dbt`, so a clone
runs with no hidden machine-local setup. It contains no secrets: the dev
credentials are the throwaway ones from `docker-compose.yml`, and prod reads
Neon connection details from environment variables.

### Adding a repository

Edit `collector/repos.yml`. Nothing else.

### The dashboard

Metabase, reading the marts. Nothing in it is clicked together by hand:
`make dashboard` runs [`scripts/metabase_setup.py`](scripts/metabase_setup.py),
which does first-run setup, connects the warehouse filtered to the marts
schema, and creates or updates fourteen native-SQL questions and the dashboard
that lays them out. Re-running it after a query change updates the existing
questions in place, so the dashboard URL survives.

Three sections, matching the three views the design asked for:

- **Category pulse over time** — material events and stars gained per ISO
  week, stacked by category (`agg_category_pulse`), plus a collection-health
  line that should stay at zero.
- **Change feed** — `fct_change_events` newest first, with the `evidence`
  JSON in the row, and the change-type mix by materiality.
- **Repo timeline** — `agg_repo_timeline`, one row per repo per month, and
  the top repos by stars gained this month.

The SQL is in the script rather than in Metabase's query builder so it is
reviewable in a diff and identical to what a reader would run by hand.

---

## Testing

**172 dbt tests + 13 pytest tests.**

Beyond the usual `not_null` / `unique` / `relationships` / `accepted_values`:

- Every `fct_change_events` row has non-null `evidence` — a database constraint,
  not a test that can be skipped
- No change event predates the repo's first observation
- A repo cannot be archived and ship a major release in the same week
- No duplicate `(repo, change_type, week)` in a digest
- Source freshness on the observation log: warn at 36h, error at 72h

**dbt unit tests** carry most of the weight right now, because the collector has
only been running since 2026-09-09 and the real data contains few changes yet.
They assert behaviour that would otherwise go unverified for months:

- Consecutive identical days collapse; a licence change splits the period
- A `NULL` → value transition is detected (the case a naive `md5(a || b)`
  fingerprint silently loses, because `NULL || 'x'` is `NULL` in Postgres)
- A rename produces two state periods for **one** repo, not two repos
- A repo already stale at its first observation does **not** emit `went_stale`
- Two days of 90× star growth produces **no** spike (see limitations)

**pytest** covers what dbt cannot see, because it is about the collector's
behaviour rather than the data it already wrote:

- Re-running a day upserts rather than duplicates, and the later run wins
- A 403 with budget remaining is a secondary limit → retried with backoff
- A 403 with zero remaining stops the run immediately rather than burning
  retries on calls that cannot succeed until the hourly reset

**Semver parsing is tested against real tags**, taken from a dry run against the
tracked repos rather than invented:

```
v1.12.4          desktop-v0.0.25    @arizeai/phoenix-evals@2.5.0
3.3.1            rust-v0.153.4      langchain-core==1.6.2
v2.0.0-vscode    2026.8.31          2026-07-28
```

`v2.0.0-vscode` is a platform build, not a prerelease. `2026.8.31` is a date, and
parsing it as semver would report **major version 2026** and make every
date-tagged release look like a breaking change — so CalVer is detected and
excluded, degrading to `release_unclassified` rather than being dropped.

### Slim CI

Pull requests run `dbt build --select state:modified+ --defer --state`, which
builds only what changed plus its descendants and points everything else at
production. Proven on the first pull request rather than assumed:
[#1](https://github.com/tapanxd/oss-radar/pull/1) changed one intermediate
model, CI found the manifest from the previous `main` build, built **7 of 13**
models into a per-PR schema on Neon, deferred the other 6, skipped the
full-build fallback, and dropped the schema afterwards.

`--defer` rewrites unchanged `ref()`s to production relations, so those relations
must exist. CI therefore runs against the same Neon database as production, in a
per-PR schema that is dropped afterwards — including on failure. A throwaway
empty Postgres would have nothing to defer to, and deferral would silently
degrade into a full build.

---

## Failure scenarios

A pipeline that has never failed is a worse story than one that fails correctly.

### Rate-limit exhaustion mid-run

The authenticated REST budget is 5,000 requests/hour, and **secondary limits are
enforced separately** — you can have 4,000 remaining and still be throttled for
firing too fast.

The collector stops at a floor of 100 remaining rather than draining to zero.
Repos 1–18 complete, 19–49 stop cleanly, `raw.collection_runs` records what was
skipped, and the process **exits zero**. Partial success is success. dbt builds
on what is present, `int_collection_gaps` marks the missing rows as
`rate_limit_reached`, and tomorrow's run fills them in.

Only a total failure alerts.

### A collection gap

GitHub disables scheduled workflows on repositories inactive for ~60 days, and
it does so **silently**. Neon can also be unreachable. Either way, days go
missing.

Without handling, a week where the collector was dead looks exactly like a week
where nothing happened. `int_collection_gaps` lists every date each repo should
have been observed and was not, and distinguishes:

| `gap_reason` | Means |
|---|---|
| `collector_did_not_run` | infrastructure failure — every repo affected |
| `rate_limit_reached` | designed partial-success path |
| `repo_skipped_or_failed` | per-repo fetch failure |
| `run_died_mid_flight` | process died without recording completion |
| `unexplained` | ran clean, and the row is still missing — investigate |

Those imply different amounts of doubt about the rest of the digest, so they are
not collapsed together. `agg_weekly_digest` carries the week's gap count so the
digest states its blind spot rather than implying full coverage.

---

## Known limitations

Stated plainly, because the alternative is implying the numbers are better than
they are.

**Only 2 days of history.** The collector started on 2026-09-09. Anything needing
a window is correctly refusing to answer — star velocity currently reports
nothing at all, with the caveat `insufficient history: 2 of 7 days`. This is the
guard working, not a bug. It will start producing signal in week two and
trustworthy signal in month four.

**Star velocity is noisy and the threshold is judgement.** A spike is defined as
7-day velocity exceeding 3× the trailing 90-day mean, with a non-zero baseline.
Those numbers are not a validated model. Stars are a poor proxy for importance,
and are gameable. Treat `star_spike` as the weakest signal here — it is ranked
`low` for that reason.

**Materiality weights are opinion.** Base materiality per change type, +1 for a
high-priority repo, −1 for one with no commits in a year. They live in one file
(`macros/materiality.sql`) so the opinion is legible and changeable, but they are
still an opinion.

**Detection date is not event date.** A change is detected on the day the
collector next runs, which can be up to 24 hours after it happened. The digest
reports `detected_at`, and `release_published_at` where GitHub provides it.

**Breaking-change detection is keyword matching.** Seven regex patterns in a seed
file against the release body. It will miss a breaking change described in prose
without any of the marker phrases, and it can fire on a heading that mentions
"migration" in passing. Every match records which rule fired and quotes the
matched line, so a reader can judge it — but it is not semantic analysis, and
there is no LLM summarisation in v1.

**Repos with no releases are quieter than they deserve.** Detection leans on
release events, so a project that ships via commits without tagging releases
produces fewer digest lines than its actual activity warrants.

**Pre-existing state is invisible to the digest.** Anything already true on
2026-09-09 — an archived repo, an old rename — produces no event, by design.
`dim_repos` carries those flags instead.

---

## Status

| Phase | State |
|---|---|
| **0 — Collector** | Running daily since 2026-09-09 |
| **1 — Warehouse** | Complete. 13 models, 174 dbt tests, 13 pytest, Slim CI deferral proven on PR #1 |
| **2 — Airflow** | Running locally. Three DAGs; Asset-triggered digest; collector as 49 mapped tasks |
| **3 — Polish** | README and Metabase dashboard done. Airflow screenshots outstanding |

The first digest is committed: [`digests/2026-W37.md`](digests/2026-W37.md),
rendered from three days of collection and still marked partial until the
week closes. Slim CI's deferral path has run on a real pull request (#1)
and did what it was designed to do.

---

## Non-goals

Not a security scanner — OpenSSF Scorecard exists. Not a package registry
mirror. No GH Archive firehose, which would turn this into a volume exercise and
destroy the rate-limit narrative. No LLM summarisation in v1. No automated repo
discovery in v1; the tracked set is curated by hand in `repos.yml`, and it is
more convincing for it.
