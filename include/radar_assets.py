"""Assets shared between DAGs.

Lives in include/, NOT dags/. A DAG file that imports another DAG file executes
it, and the dag-processor then discovers that DAG twice - once from its own
file and once via the import - and attributes it to whichever it parsed last.
Keeping shared objects outside the DAGs folder avoids that entirely.
"""

from airflow.sdk import Asset

# Updated by radar_transform_daily when dbt_test passes. radar_digest_weekly is
# scheduled on it. A URI-shaped name, per the 3.x convention.
CHANGE_EVENTS = Asset("radar://warehouse/change_events")
