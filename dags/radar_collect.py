"""
radar_collect

The collector as a dynamically mapped DAG: one task per tracked repository,
each independently retryable, fanned out from repos.yml at runtime.

    tracked_repos
         |
    extract_repo  [ x49, mapped ]
         |
    record_run

This is the same collector that runs on the GitHub Actions cron, reached by a
different route. It imports collect.py's client, observation builder and upsert
directly rather than re-implementing them, so the two paths cannot drift. Both
write to the same table with the same upsert on (repo, day); if both run on
the same day the later one wins, and they carry the same data.

WHY MAPPED TASKS

A single loop over 49 repos is one task that either finishes or does not. If
repo 31 fails, repos 32-49 are never attempted, and the retry re-fetches 1-30
for nothing. With one task per repo, a failure is isolated to that repo, its
retry re-fetches only that repo, and the other 48 are unaffected. The task
grid in the UI also makes it obvious at a glance WHICH repo is misbehaving.

CONCURRENCY IS DELIBERATELY LOW

max_active_tis_per_dag=4. GitHub's secondary rate limits are enforced
separately from the 5,000/hour budget: you can have 4,000 requests remaining
and still be throttled for firing them too fast. Forty-nine tasks starting at
once is exactly the burst that trips it. Four in flight keeps each run well
under the abuse thresholds; the whole fan-out still completes in under a
minute.

THE ACTIONS CRON KEEPS RUNNING

It is the fallback. History cannot be backfilled, so two independent paths to
the same append-only table is cheap insurance against either one being
silently disabled.

WRITES TO PRODUCTION - BUT NOT BY DEFAULT FROM A DEV ENVIRONMENT.

This DAG upserts into the same Neon table the cron does. The dry_run param
defaults to True unless DBT_TARGET=prod, so a local Airflow fetches and logs
but writes nothing until you either set the target to prod or trigger with
{"dry_run": false} on purpose.

Why: unpausing a DAG makes Airflow run its most recent missed interval
immediately, even with catchup=False. The first time this DAG was unpaused in
a dev environment it did exactly that and wrote 49 rows to Neon before anyone
had chosen to. Harmless - the upsert made them identical to the cron's rows -
but "harmless" is not the same as "intended".
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, task

# The mounted collector package. Imported at task run time, inside the
# functions, so a missing GH_TOKEN breaks a task rather than DAG parsing.
COLLECTOR_DIR = "/opt/airflow/collector"
REPOS_CONFIG = f"{COLLECTOR_DIR}/repos.yml"

DEFAULT_ARGS = {
    "owner": "radar",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _import_collector():
    if COLLECTOR_DIR not in sys.path:
        sys.path.insert(0, COLLECTOR_DIR)
    import collect  # noqa: WPS433 - deliberate late import, see above
    return collect


@dag(
    dag_id="radar_collect",
    description="Collect one observation per tracked repo, one task per repo.",
    # Same slot as the Actions cron. Both paths upsert, so overlap is harmless.
    schedule="15 6 * * *",
    start_date=datetime(2026, 9, 9),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "dry_run": Param(
            # Real writes only when explicitly pointed at production. See the
            # module docstring for the incident that motivated this.
            os.environ.get("DBT_TARGET", "dev") != "prod",
            type="boolean",
            description=(
                "Fetch from GitHub and log, but write nothing to the warehouse. "
                "Defaults to true unless DBT_TARGET=prod."
            ),
        ),
    },
    tags=["radar", "collect"],
    doc_md=__doc__,
)
def radar_collect():

    @task
    def tracked_repos() -> list[dict]:
        """Read repos.yml and hand back one dict per repo for the fan-out.

        Read at run time, not parse time, so editing repos.yml takes effect on
        the next run without touching the DAG. The list is the ONLY thing that
        decides how many extract_repo tasks exist.
        """
        collect = _import_collector()
        repos = collect.load_repos(REPOS_CONFIG)
        print(f"{len(repos)} repos configured")
        return [
            {"full_name": r.full_name, "category": r.category,
             "priority": r.priority, "notes": r.notes}
            for r in repos
        ]

    @task(
        # Four in flight at once. See CONCURRENCY above.
        max_active_tis_per_dag=4,
        # A per-repo failure retries the repo, not the run.
        retries=2,
    )
    def extract_repo(repo: dict, **context) -> dict:
        """Fetch one repo's metadata and latest release, build the observation,
        upsert it. Returns a small result dict for record_run to aggregate."""
        collect = _import_collector()
        dry_run = bool(context["params"].get("dry_run", False))

        tracked = collect.TrackedRepo(
            full_name=repo["full_name"],
            category=repo["category"],
            priority=repo["priority"],
            notes=repo.get("notes"),
        )

        client = collect.GitHubClient(os.environ["GH_TOKEN"])
        observed_at = datetime.now(timezone.utc)

        payload = client.fetch_repo(tracked.full_name)
        if payload is None:
            # A 404 is a real signal, not an error: deleted, made private, or
            # the configured path is wrong. Recorded, not retried.
            print(f"NOT FOUND  {tracked.full_name}")
            return {"repo": tracked.full_name, "status": "not_found",
                    "remaining": client.remaining}

        release = client.fetch_latest_release(tracked.full_name)
        observation = collect.build_observation(tracked, payload, release, observed_at)

        returned_name = payload.get("full_name")
        rename_note = (
            f"  (API returned {returned_name})"
            if returned_name and returned_name != tracked.full_name else ""
        )
        print(
            f"OK  {tracked.full_name}{rename_note}  "
            f"stars={observation['stars']}  "
            f"release={observation['latest_release_tag']}  "
            f"budget={client.remaining}"
        )

        if dry_run:
            return {"repo": tracked.full_name, "status": "dry_run",
                    "remaining": client.remaining}

        import psycopg
        with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=30) as conn:
            collect.write_observations(conn, [observation])
            conn.commit()

        return {"repo": tracked.full_name, "status": "ok",
                "remaining": client.remaining}

    @task(trigger_rule="all_done")
    def record_run(results: list[dict], **context) -> None:
        """Write the run summary to raw.collection_runs so a partial run is a
        queryable fact. trigger_rule=all_done so this runs even if some
        extract_repo tasks failed - that is precisely when the record matters.

        int_collection_gaps reads this table to explain missing observations.
        """
        collect = _import_collector()
        dry_run = bool(context["params"].get("dry_run", False))

        # Mapped-task results arrive as a lazy sequence; failed tasks
        # contribute nothing, so the shortfall against the configured count is
        # the failure count.
        results = list(results)
        configured = len(collect.load_repos(REPOS_CONFIG))
        ok = sum(1 for r in results if r["status"] == "ok")
        not_found = sum(1 for r in results if r["status"] == "not_found")
        dry = sum(1 for r in results if r["status"] == "dry_run")
        failed = configured - len(results)
        remaining = min((r["remaining"] for r in results if r.get("remaining") is not None),
                        default=None)

        print(f"configured={configured} ok={ok} not_found={not_found} "
              f"dry_run={dry} failed={failed} min_budget_remaining={remaining}")

        if dry_run:
            print("dry run: not recording to raw.collection_runs")
            return

        summary = {
            "started_at": context["dag_run"].start_date,
            "finished_at": datetime.now(timezone.utc),
            "collector_version": f"{collect.COLLECTOR_VERSION}+airflow",
            # Key names match write_run_summary's placeholders, not the
            # column names.
            "attempted": configured,
            "succeeded": ok,
            "failed": failed + not_found,
            "skipped": 0,
            "rate_limit_hit": False,
            "rate_limit_remaining": remaining,
            "notes": f"airflow radar_collect; not_found={not_found}",
        }
        import psycopg
        with psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=30) as conn:
            collect.write_run_summary(conn, summary)
            conn.commit()

    record_run(extract_repo.expand(repo=tracked_repos()))


radar_collect()
