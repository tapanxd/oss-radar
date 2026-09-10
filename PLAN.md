# Portfolio Plan — Three dbt + Airflow Projects

Owner: Tapan Panchal
Status: planning
Audience: this doc plus the three `DESIGN-*.md` files are the brief handed to Claude Code.

---

## 1. What these three projects are

| # | Project | Repo name | Primary skill demonstrated | Build size |
|---|---------|-----------|---------------------------|-----------|
| 1 | SaaS Metrics / MRR Waterfall | `saas-metrics-warehouse` | Analytics engineering — hard SQL, dbt modelling, metric correctness | S–M |
| 2 | Open Source Project Health | `oss-health-monitor` | Data engineering — orchestration at scale, API rate limits, incremental extraction | M |
| 3 | Real Estate Document Intelligence | `re-doc-intelligence` | Unstructured → structured, data quality, branching pipelines | M–L |

They are deliberately different in character. Do not let them converge into three versions of the same "API → warehouse → dashboard" project.

**The differentiator in each:**

- Project 1 — an MRR waterfall that actually reconciles. `new + expansion + contraction + churn + reactivation == net change in MRR`, enforced as a dbt test, at every month, with no fudge factor. Most portfolio churn projects skip straight to an ML model and never build the metric layer correctly.
- Project 2 — one Airflow task per repo via **dynamic task mapping**, not one DAG per repo, plus real rate-limit and incremental-cursor handling. Same pattern as "one task per client / per source" in production.
- Project 3 — documents are messy, extraction is uncertain, and the pipeline must **branch on document type** and **quarantine low-confidence extractions** rather than silently writing garbage into marts.

---

## 2. Build order and why

**Build 1 → 2 → 3.**

1. **SaaS Metrics first.** Data is synthetic and generated locally, so there is zero external dependency and zero flakiness. This lets the shared scaffolding (docker-compose, Airflow, dbt profiles, CI) get debugged against a pipeline that can never fail for reasons outside the repo.
2. **OSS Health second.** Reuses all the scaffolding, adds the first real external API. New difficulty is purely orchestration.
3. **Real Estate third.** Highest effort, most unknowns, most likely to sprawl. Do it last, with the scaffolding already proven.

Each project must be independently runnable and independently presentable. Do not build a shared monorepo or a shared internal library across the three — copy the scaffolding. A reviewer will only ever look at one repo at a time, and a cross-repo dependency makes each repo un-runnable on its own.

---

## 3. Shared stack (identical across all three)

Keep this constant so the second and third builds are fast.

- **Orchestration:** Apache Airflow, run via `docker-compose`.
  - Note for Claude Code: Airflow 3.x renamed the 2.x `Dataset` concept to `Asset` and changed some import paths. Check the installed version in `requirements.txt` and use the matching API rather than assuming. Pin an exact Airflow version in every repo.
- **Warehouse:** PostgreSQL 16 in docker-compose, as a **separate database from Airflow's metadata DB**. Never point dbt at the Airflow metadata database.
  - DuckDB is a tempting alternative but has a single-writer file lock. If DuckDB is used, the extract and the dbt run must be strictly serialized, and parallel mapped tasks writing to it will fail. Postgres avoids this entirely. Default to Postgres.
- **Transformation:** dbt Core with `dbt-postgres`.
- **Extraction:** plain Python in `include/` or `plugins/`, invoked by Airflow. No Spark, no Databricks — these are intentionally small-data projects where the interest is in modelling and orchestration, not volume.
- **dbt ↔ Airflow integration:** run dbt via `BashOperator` calling `dbt build --select ...` in the first build. If time allows, upgrade to `astronomer-cosmos` so each dbt model renders as its own Airflow task — this looks significantly better in a screenshot of the DAG graph. Cosmos is a nice-to-have, not a blocker.
- **CI:** GitHub Actions running `dbt parse`, `sqlfluff lint`, and `pytest` on every PR.
- **Presentation:** `dbt docs generate` output plus either Metabase (docker) or a small Streamlit app. One dashboard per project, three to five tiles. Do not build an elaborate frontend; it is not what is being assessed.

### Non-negotiable conventions

- dbt layers, in every project: `staging/` → `intermediate/` → `marts/`.
  - `staging/` — one model per source table, renaming and casting only, materialized as views, prefix `stg_`.
  - `intermediate/` — the actual logic, prefix `int_`, never exposed to consumers.
  - `marts/` — the consumable models, prefix `dim_` / `fct_` / `agg_`.
- Every model has a `.yml` entry with a description and at least one test. A model with no description does not get merged.
- Every mart model declares its **grain** in the first line of its description. e.g. "One row per customer per month."
- Sources declared in `_sources.yml` with `freshness` blocks where the source has a timestamp.
- All secrets via environment variables and `.env` (gitignored). Never a token in a YAML file, never a token in a DAG.
- Every DAG has `catchup` set explicitly and deliberately, with a comment saying why.
- Every extract task is **idempotent**: re-running it for the same logical date produces the same warehouse state, not duplicated rows.

---

## 4. Milestones per project

Use the same five milestones each time. Each is a PR.

- **M1 — Scaffolding.** docker-compose up, Airflow UI reachable, empty dbt project connects to Postgres, CI green.
- **M2 — Extract + land.** Raw data lands in a `raw` schema. Idempotent. Reruns tested.
- **M3 — Staging + intermediate.** dbt models with tests. This is where the real work is.
- **M4 — Marts + tests.** The signature metric of the project, plus the test that proves it is correct.
- **M5 — Polish.** README with architecture diagram, dbt docs, dashboard, known limitations, one deliberate failure scenario documented.

Do not start M5 of one project before M4 of that project is done. An unfinished project is worth less than a smaller finished one — the existing `github-lakehouse` repo is the cautionary example here: substantial work done, but no README and no dashboard, so nobody can tell.

---

## 5. What "done" means for the portfolio

A reviewer must be able to, in under five minutes:

1. Read the README and understand what the project does and why the design is the way it is.
2. See an architecture diagram.
3. Run `docker compose up` and `make seed && make run` and get a populated warehouse.
4. See a screenshot of the Airflow DAG graph and of the dashboard, in the README, without running anything.

Every README must contain a **"Known limitations"** section and a **"Failure scenario"** section. The failure scenario is the single highest-value thing in the repo for interviews: pick one realistic failure (late-arriving data, API schema drift, a restated record, a rate-limit exhaustion) and document how the pipeline detects and handles it. See each design doc for the assigned scenario.

---

## 6. Explicit non-goals

- No machine learning in projects 1 and 2. Project 3 uses extraction models but is not evaluated on model quality.
- No Kubernetes, no Terraform, no cloud deployment. These run locally via docker-compose. Cloud infra is already covered by the `github-lakehouse` repo and adds cost and flakiness here.
- No real-time / streaming. All three are batch.
- No attempt to make the dashboards beautiful.
- No shared code between the three repos.

---

## 7. Reference

- `DESIGN-saas-metrics.md`
- `DESIGN-oss-health.md`
- `DESIGN-re-doc-intelligence.md`
