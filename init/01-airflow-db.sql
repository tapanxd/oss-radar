-- Airflow's metadata database. Separate from the `warehouse` database that dbt
-- builds into. Phase 2 points AIRFLOW__DATABASE__SQL_ALCHEMY_CONN here.
create database airflow;
