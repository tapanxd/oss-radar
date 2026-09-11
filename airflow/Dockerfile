# Airflow image with dbt and the collector's dependencies installed.
#
# dbt goes INTO the Airflow image rather than into a separate container, so
# BashOperator can call it directly. A separate dbt container means solving
# networking and volume mounts between the two for no benefit at this scale.
#
# Pinned to an exact Airflow version. 3.x renamed Dataset to Asset and moved
# the DAG-authoring API to airflow.sdk; a floating tag would let a minor
# upgrade break every DAG silently.
FROM apache/airflow:3.3.1

# The image's Python is 3.13, the same as the collector runs on in Actions, so
# the collector's pinned requirements install unchanged.
COPY collector/requirements.txt /tmp/collector-requirements.txt
COPY requirements-dev.txt        /tmp/requirements-dev.txt

# dbt-postgres from the dev pins, so the DAG runs the same dbt version that
# `make build` and CI run. pytest and sqlfluff are not needed at runtime and
# are filtered out to keep the image small.
RUN pip install --no-cache-dir \
      -r /tmp/collector-requirements.txt \
      $(grep -E '^(dbt-core|dbt-postgres|psycopg)' /tmp/requirements-dev.txt)
