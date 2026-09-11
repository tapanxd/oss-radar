"""
radar_digest_weekly

Renders digests/YYYY-WNN.md from agg_weekly_digest.

    build_digest -> render_markdown -> publish

SCHEDULED ON AN ASSET, NOT A CRON. radar_transform_daily updates the
change_events Asset when dbt_test passes, and this DAG runs in response. There
is no time offset to get wrong: the digest runs when the warehouse says it has
new, tested data, and does not run at all if the transform failed.

"Weekly" describes the output, not the trigger. The renderer produces one file
per ISO week and is idempotent - re-rendering an unchanged week writes nothing
- so it is safe to run every time the Asset updates. An in-progress week is
labelled partial and re-rendered daily until it closes, at which point the
file stops changing. A week that closed with nothing above threshold still
gets a file saying so, because "no digest" and "nothing happened" are
different claims and a reader cannot tell them apart.

build_digest short-circuits when the warehouse has no digest rows at all, which
only happens before the first successful transform.

publish is deliberately modest. It does not commit from inside the container:
git against a Windows-mounted working tree fights line endings and ownership
checks, and a DAG that half-commits is worse than one that does not. It
reports what was written; committing digests/ is a human step, and the files
are meant to be reviewed before they are.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/include")
from radar_assets import CHANGE_EVENTS  # noqa: E402

RENDERER = "/opt/airflow/include/render_digest.py"

DEFAULT_ARGS = {
    "owner": "radar",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _marts_schema() -> str:
    """The schema agg_weekly_digest lives in depends on the dbt target.

    dev builds into <profile schema>_marts, prod into analytics_marts. Derived
    here rather than hard-coded in the renderer so the same DAG works against
    either warehouse.
    """
    target = os.environ.get("DBT_TARGET", "dev")
    if target == "prod":
        return "analytics_marts"
    return "dbt_tapan_marts"


def _warehouse_dsn() -> str:
    target = os.environ.get("DBT_TARGET", "dev")
    if target == "prod":
        return (
            f"postgresql://{os.environ['NEON_USER']}:{os.environ['NEON_PASSWORD']}"
            f"@{os.environ['NEON_HOST']}:{os.environ.get('NEON_PORT', '5432')}"
            f"/{os.environ['NEON_DB']}?sslmode=require"
        )
    host = os.environ.get("RADAR_PG_HOST", "postgres")
    port = os.environ.get("RADAR_PG_PORT", "5432")
    return f"postgresql://radar:radar@{host}:{port}/warehouse"


@dag(
    dag_id="radar_digest_weekly",
    description="Render the weekly digest when fresh change events arrive.",
    schedule=[CHANGE_EVENTS],
    start_date=datetime(2026, 9, 9),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["radar", "digest"],
    doc_md=__doc__,
)
def radar_digest_weekly():

    @task.short_circuit
    def build_digest() -> bool:
        """Confirm there is something to render. Returns False - skipping the
        rest of the DAG - only when agg_weekly_digest is empty."""
        import psycopg

        schema = _marts_schema()
        with psycopg.connect(_warehouse_dsn(), connect_timeout=30) as conn:
            weeks = conn.execute(
                f"select detected_iso_year, detected_iso_week, count(*) "
                f"from {schema}.agg_weekly_digest "
                f"group by 1, 2 order by 1, 2"
            ).fetchall()

        if not weeks:
            print("agg_weekly_digest is empty; nothing to render yet")
            return False

        for year, week, n in weeks:
            print(f"{year}-W{week:02d}: {n} entries above threshold")
        return True

    @task
    def render_markdown() -> list[str]:
        """Run the renderer. It writes only files whose content changed and
        prints a JSON summary on its last line, which is returned via XCom."""
        env = {
            **os.environ,
            "RADAR_WAREHOUSE_DSN": _warehouse_dsn(),
            "RADAR_MARTS_SCHEMA": _marts_schema(),
            "DIGESTS_DIR": os.environ.get("DIGESTS_DIR", "/opt/airflow/digests"),
        }
        result = subprocess.run(
            [sys.executable, RENDERER],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        summary = json.loads(result.stdout.strip().splitlines()[-1])
        return summary["written"]

    @task
    def publish(written: list[str]) -> None:
        """Report what changed. See the module docstring for why this does not
        commit."""
        digests_dir = Path(os.environ.get("DIGESTS_DIR", "/opt/airflow/digests"))
        existing = sorted(p.name for p in digests_dir.glob("*.md"))

        if not written:
            print(f"no digest changed this run; {len(existing)} on disk: {existing}")
            return

        print(f"{len(written)} digest file(s) written or updated:")
        for path in written:
            print(f"  {path}")
        print()
        print("To publish, commit them from the repository root:")
        print("  git add digests/ && git commit -m 'Digest update'")

    gate = build_digest()
    rendered = render_markdown()
    gate >> rendered
    publish(rendered)


radar_digest_weekly()
