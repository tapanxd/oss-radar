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

.PHONY: help up down nuke seed debug build test docs collect collect-dry fresh reset check

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

down:  ## Stop the stack, keeping data
	docker compose down

nuke:  ## Stop the stack and DELETE the local volume (dev data only; prod untouched)
	docker compose down -v

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

check: fresh build  ## What CI runs: freshness, then a full build with tests

collect-dry:  ## Run the collector against GitHub without writing anything
	@$(LOAD_ENV) .venv/Scripts/python.exe collector/collect.py --config collector/repos.yml --dry-run

collect:  ## Run the collector for real. WRITES TO PRODUCTION (Neon).
	@echo "This writes to Neon production. Ctrl-C within 5s to abort."
	@sleep 5
	@$(LOAD_ENV) .venv/Scripts/python.exe collector/collect.py --config collector/repos.yml
