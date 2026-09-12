-- Metabase's application database: its users, saved questions and dashboards.
-- Separate from `warehouse` (what the dashboard reads) and from `airflow`, for
-- the same reason those two are separate. Phase 3 points MB_DB_* here.
--
-- Runs only on first init of an empty volume; `make metabase-up` also creates
-- it if missing, so an existing volume does not need `make nuke`.
create database metabase;
