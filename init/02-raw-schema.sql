-- The dev warehouse mirrors production's `raw` schema, which is where the
-- collector lands observations on Neon. Creating it empty here means
-- `dbt debug` and `dbt parse` work before the Neon seed has been pulled down.
--
-- The seed (scripts/seed_dev.sh) drops and recreates this schema from a Neon
-- dump, so anything added here is not durable. Structure only.
create schema if not exists raw;
