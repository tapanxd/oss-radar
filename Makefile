# oss-radar
#
# Everything below runs through bash, not cmd.exe, so the same targets work on
# Windows (Git Bash), macOS and Linux.
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

# .env is NOT pulled in with `include .env`, deliberately.
#
# Make's include does not strip quotes, so RADAR_PG_PORT="5433" arrives as the
# six characters "5433" and docker compose rejects it with
# `invalid hostPort: "5433"`. The quotes cannot simply be dropped from .env
# either: DATABASE_URL contains an & and an unquoted value breaks `source`.
#
# Instead every recipe that needs the variables sources .env through bash,
# which strips quotes correctly. docker compose reads .env natively and needs
# no help. dbt would find .env on its own via python-dotenv, but that is an
# implementation detail rather than a contract, so it is made explicit here.
LOAD_ENV := set -a; [ -f .env ] && . ./.env; set +a;

DBT := ../.venv/Scripts/dbt.exe
DBT_DIR := dbt_project

.PHONY: help up down nuke seed debug build test docs collect collect-dry fresh reset check lint fix pytest ci digest airflow-up airflow-down airflow-logs airflow-build metabase-up metabase-down dashboard

help:  ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) \
	  | sed -E 's/^([a-zA-Z_-]+):.*## /\1|/' \
	  | sort \
	  | awk -F'|' '{printf "  %-14s %s\n", $$1, $$2}'

up:  ## Start the local dev Postgres and wait for it to be healthy
	docker compose up -d
	@printf "waiting for postgres"
	@for i in $$(seq 1 60); do \
	  s=$$(docker inspect --format '{{.State.Health.Status}}' radar-postgres 2>/dev/null || echo none); \
	  if [ "$$s" = "healthy" ]; then echo " ready"; exit 0; fi; \
	  printf "."; sleep 1; \
	done; echo " TIMED OUT"; docker logs --tail 30 radar-postgres; exit 1

# Every profile is named so Airflow and Metabase come down with Postgres. A
# plain `docker compose down` only sees the default profile and would leave
# them running against a database that no longer exists.
down:  ## Stop the stack (Postgres, Airflow, Metabase), keeping data
	docker compose --profile airflow --profile dashboard down

nuke:  ## Stop the stack and DELETE the local volume (dev data only; prod untouched)
	docker compose --profile airflow --profile dashboard down -v

seed:  ## Reload the dev warehouse from the Neon raw schema (read-only against prod)
	bash scripts/seed_dev.sh

debug:  ## Check dbt can reach the dev warehouse
	@$(LOAD_ENV) cd $(DBT_DIR) && $(DBT) debug

build:  ## Run and test every dbt model
	@$(LOAD_ENV) cd $(DBT_DIR) && $(DBT) build

test:  ## Run dbt tests only
	@$(LOAD_ENV) cd $(DBT_DIR) && $(DBT) test

fresh:  ## Check source freshness against the collector's output
	@$(LOAD_ENV) cd $(DBT_DIR) && $(DBT) source freshness

docs:  ## Generate and serve dbt docs
	@$(LOAD_ENV) cd $(DBT_DIR) && $(DBT) docs generate && $(DBT) docs serve

reset: nuke up seed build  ## Rebuild the dev environment from scratch

lint:  ## Lint every dbt model with sqlfluff
	@$(LOAD_ENV) .venv/Scripts/sqlfluff.exe lint dbt_project/models --processes 2

fix:  ## Auto-fix sqlfluff formatting violations
	@$(LOAD_ENV) .venv/Scripts/sqlfluff.exe fix dbt_project/models --processes 2 --force

pytest:  ## Run the collector test suite
	@$(LOAD_ENV) .venv/Scripts/python.exe -m pytest

check: fresh build  ## What CI runs: freshness, then a full build with tests

ci: lint pytest check  ## Everything CI runs, locally, before opening a PR

digest:  ## Render digests/YYYY-WNN.md from the dev warehouse
	@$(LOAD_ENV) .venv/Scripts/python.exe include/render_digest.py

airflow-build:  ## Build the Airflow image (dbt + collector deps baked in)
	docker compose --profile airflow build

airflow-up:  ## Start Airflow (api-server, scheduler, dag-processor) on :8081
	docker compose --profile airflow up -d
	@printf "waiting for airflow api-server"
	@for i in $$(seq 1 90); do 	  s=$$(docker inspect --format '{{.State.Health.Status}}' radar-airflow-apiserver 2>/dev/null || echo none); 	  if [ "$$s" = "healthy" ]; then echo " ready -> http://localhost:$${RADAR_AIRFLOW_PORT:-8081}"; exit 0; fi; 	  printf "."; sleep 2; 	done; echo " TIMED OUT"; docker compose --profile airflow logs --tail 30 airflow-apiserver; exit 1

airflow-down:  ## Stop Airflow, keep Postgres running
	docker compose --profile airflow stop airflow-apiserver airflow-scheduler airflow-dag-processor
	docker compose --profile airflow rm -f airflow-apiserver airflow-scheduler airflow-dag-processor airflow-init

airflow-logs:  ## Tail Airflow scheduler and dag-processor logs
	docker compose --profile airflow logs -f --tail 50 airflow-scheduler airflow-dag-processor

metabase-up:  ## Start Metabase on :3000 (creates its app DB if the volume predates it)
	@$(LOAD_ENV) docker compose up -d postgres >/dev/null
	@docker exec radar-postgres psql -U radar -d warehouse -tAc 	  "select 1 from pg_database where datname = 'metabase'" | grep -q 1 	  || docker exec radar-postgres psql -U radar -d warehouse -c "create database metabase"
	docker compose --profile dashboard up -d metabase
	@printf "waiting for metabase"
	@for i in $$(seq 1 120); do 	  s=$$(docker inspect --format '{{.State.Health.Status}}' radar-metabase 2>/dev/null || echo none); 	  if [ "$$s" = "healthy" ]; then echo " ready -> http://localhost:$${RADAR_METABASE_PORT:-3000}"; exit 0; fi; 	  printf "."; sleep 2; 	done; echo " TIMED OUT"; docker logs --tail 30 radar-metabase; exit 1

metabase-down:  ## Stop Metabase, keep Postgres running
	docker compose --profile dashboard stop metabase
	docker compose --profile dashboard rm -f metabase

dashboard:  ## Build or update the oss-radar dashboard in Metabase from scripts/metabase_setup.py
	@$(LOAD_ENV) .venv/Scripts/python.exe scripts/metabase_setup.py

collect-dry:  ## Run the collector against GitHub without writing anything
	@$(LOAD_ENV) .venv/Scripts/python.exe collector/collect.py --config collector/repos.yml --dry-run

collect:  ## Run the collector for real. WRITES TO PRODUCTION (Neon).
	@echo "This writes to Neon production. Ctrl-C within 5s to abort."
	@sleep 5
	@$(LOAD_ENV) .venv/Scripts/python.exe collector/collect.py --config collector/repos.yml
