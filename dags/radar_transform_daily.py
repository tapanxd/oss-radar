"""
radar_transform_daily

Runs the dbt project layer by layer against the warehouse, then tests it, and
announces fresh change events by updating an Asset. radar_digest_weekly is
scheduled on that Asset, so the digest runs when data actually arrives rather
than on a cron offset that hopes the transform has finished.

    wait_for_fresh_observations
            |
        dbt_seed
            |
      dbt_run_staging -> dbt_run_intermediate -> dbt_run_marts
                                                      |
                                                  dbt_test  [outlet: change_events]

catchup=False, deliberately. GitHub cannot be queried as-of a past date, so a
"backfill" of this DAG would just re-run today's transform under yesterday's
label. The observation log is the substitute: history accumulates there, and
every dbt run recomputes from all of it.

dbt is invoked through BashOperator rather than a Python wrapper. The image has
dbt installed, DBT_PROFILES_DIR and DBT_PROJECT_DIR point at the mounted
project, and the target comes from DBT_TARGET (dev by default, so a local
Airflow builds the local warehouse; set prod to build Neon).
"""

from __future__ import annotations

from datetime import datetime, timedelta

import sys

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

sys.path.insert(0, "/opt/airflow/include")
from radar_assets import CHANGE_EVENTS  # noqa: E402

# Every dbt call starts the same way. Kept as one string so the five tasks
# cannot drift apart in how they invoke dbt.
DBT = "cd $DBT_PROJECT_DIR && dbt --no-use-colors"

DEFAULT_ARGS = {
    "owner": "radar",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="radar_transform_daily",
    description="Build and test the dbt warehouse; emit the change_events asset.",
    # 12:00 UTC. The collector cron is 06:15 UTC, but Actions routinely queues
    # scheduled jobs for hours - the first real run fired at 11:25. Freshness
    # is checked below regardless, so this is a starting point, not a promise.
    schedule="0 12 * * *",
    start_date=datetime(2026, 9, 9),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["radar", "dbt"],
    doc_md=__doc__,
)
def radar_transform_daily():

    # Source freshness is the gate. warn 36h / error 72h are declared on the
    # source in dbt_project/models/staging/_sources.yml, so this task fails -
    # and nothing downstream runs - once two consecutive collections are
    # missing. That is the correct behaviour: building a digest on stale data
    # and publishing it as current would be worse than publishing nothing.
    wait_for_fresh_observations = BashOperator(
        task_id="wait_for_fresh_observations",
        bash_command=f"{DBT} source freshness --target $DBT_TARGET",
    )

    # breaking_change_markers.csv. int_release_events joins to it, so it has
    # to exist before the intermediate layer runs.
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT} seed --target $DBT_TARGET",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT} run --select staging --target $DBT_TARGET",
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"{DBT} run --select intermediate --target $DBT_TARGET",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT} run --select marts --target $DBT_TARGET",
    )

    # All tests, including the unit tests, in one pass at the end. The Asset
    # is only updated when this succeeds, so the digest DAG never sees a
    # warehouse that failed its own checks.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test --target $DBT_TARGET",
        outlets=[CHANGE_EVENTS],
    )

    (
        wait_for_fresh_observations
        >> dbt_seed
        >> dbt_run_staging
        >> dbt_run_intermediate
        >> dbt_run_marts
        >> dbt_test
    )


radar_transform_daily()
