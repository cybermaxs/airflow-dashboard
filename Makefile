# Airflow 3 monitoring with DuckDB + dbt + dbt Charts. Everything runs on your laptop.
#
#   make demo       a made-up Airflow, no database needed: generate it, dbt build, then the dashboards
#   make live       your Airflow 3: load what's new, then the dashboards
#   make refresh    your Airflow 3: load what's new (then reload the page)
#
# live reads the Airflow 3 metadata DB (Postgres, read-only) through DuckDB's postgres extension.
# Settings: copy .env.example to .env.

-include .env

MODE ?= live
PORT ?= 8080
KEYCHAIN_SERVICE ?= airflow-pg

ifeq ($(MODE),demo)
DUCKDB_PATH := demo.duckdb
AIRFLOW_DAG_TEAMS :=                        # the made-up company's teams: seeds/dag_teams.csv
AIRFLOW_UI_URL := https://airflow.example.com# links show, but lead nowhere
FULL := --full-refresh                      # the made-up history ends now: rebuild it whole
else
DUCKDB_PATH ?= dashboard.duckdb
# Postgres password: PGPASSWORD from your environment, else macOS Keychain (see .env.example)
PGPASSWORD ?= $(shell security find-generic-password -s $(KEYCHAIN_SERVICE) -a $(AIRFLOW_PG_USER) -w 2>/dev/null)
endif

export DBT_TARGET := $(MODE)
export DUCKDB_PATH PGPASSWORD AIRFLOW_PG_HOST AIRFLOW_PG_PORT AIRFLOW_PG_DB AIRFLOW_PG_USER AIRFLOW_DAG_TEAMS AIRFLOW_UI_URL

.PHONY: demo live fake tools check refresh full-refresh test serve stop open shell

demo:              ## made-up Airflow, no database needed -> dashboards on http://localhost:$(PORT)
	@$(MAKE) --no-print-directory MODE=demo tools fake refresh serve open

live:              ## your Airflow 3: load what's new -> dashboards on http://localhost:$(PORT). make refresh + reload for newer data
	@test "$(MODE)" = live || { echo "make live reads your Airflow; make demo is the made-up one"; exit 1; }
	@$(MAKE) --no-print-directory check refresh serve open

fake:              ## a made-up Airflow 3 metadata DB whose history ends now (demo/fake_airflow.sql)
	@duckdb -bail -list -noheader demo/fake_airflow.duckdb < demo/fake_airflow.sql

tools:             ## are duckdb, dbt (Fusion) and dct installed?
	@command -v duckdb >/dev/null || { echo "missing duckdb: brew install duckdb  (https://duckdb.org/install)"; exit 1; }
	@command -v dbt    >/dev/null || { echo "missing dbt:    curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update"; exit 1; }
	@command -v dct    >/dev/null || { echo "missing dct:    uv tool install dbt-charts"; exit 1; }
	@echo "$$(duckdb --version | cut -d' ' -f1) duckdb · $$(dbt --version | head -1) · dct $$(dct --version 2>/dev/null | cut -d' ' -f2) · $(MODE) -> $(DUCKDB_PATH)"

check: tools       ## ... and is the Airflow connection set?
	@test -n "$(AIRFLOW_PG_HOST)" || { echo "no Airflow connection: cp .env.example .env and fill it in"; exit 1; }
	@test -n "$(PGPASSWORD)" || { echo "no password: export PGPASSWORD=... or store it in Keychain (see .env.example)"; exit 1; }
	@echo "reading $(AIRFLOW_PG_DB)@$(AIRFLOW_PG_HOST)"

refresh:           ## load what's new from Airflow (incremental; always full in demo) + marts + tests
	dbt build $(FULL)

full-refresh:      ## rebuild everything (live: loses the rows Airflow already purged!)
	dbt build --full-refresh

test:
	dbt test

serve: stop        ## dashboards on http://localhost:$(PORT) (restarted: dct would reuse a server reading the other mode's file)
	dct serve --detach --no-cache --port $(PORT)

open:
	@open http://localhost:$(PORT)/now/ 2>/dev/null || echo "open http://localhost:$(PORT)/now/"

stop:
	@dct serve --stop --port $(PORT) 2>/dev/null || true

shell:             ## DuckDB shell on the local copy
	duckdb $(DUCKDB_PATH)
