SHELL := /usr/bin/env bash

CONFIG_FILE ?= ./config.env
GOMODCACHE ?= $(CURDIR)/.gomodcache
GOCACHE ?= $(CURDIR)/.gocache
RUN_ENV = CONFIG_FILE="$(CONFIG_FILE)" GOMODCACHE="$(GOMODCACHE)" GOCACHE="$(GOCACHE)"

.PHONY: help bootstrap-config check-config list-accounts sync sync-forecasts eval-forecasts sync-all serve report run

help:
	@echo "Targets:"
	@echo "  make bootstrap-config  - create config.env from example (if absent)"
	@echo "  make check-config      - validate config.env"
	@echo "  make list-accounts     - list API accounts"
	@echo "  make sync              - sync operations + rebuild deals into SQLite"
	@echo "  make sync-forecasts    - sync analyst forecasts snapshots into SQLite"
	@echo "  make eval-forecasts    - evaluate matured forecast snapshots"
	@echo "  make sync-all          - sync operations + forecasts + evaluation"
	@echo "  make serve             - run web UI"
	@echo "  make report            - legacy direct report mode"
	@echo "  make run               - sync + serve + open browser"

bootstrap-config:
	@if [[ -f "$(CONFIG_FILE)" ]]; then \
		echo "$(CONFIG_FILE) already exists"; \
	else \
		cp ./config.env.example "$(CONFIG_FILE)"; \
		echo "Created $(CONFIG_FILE). Fill TINVEST_TOKEN and TINVEST_ACCOUNT_ID"; \
	fi

check-config:
	@$(RUN_ENV) ./run.sh check-config

list-accounts:
	@$(RUN_ENV) ./run.sh list-accounts

sync:
	@$(RUN_ENV) ./run.sh sync

sync-forecasts:
	@$(RUN_ENV) ./run.sh sync-forecasts

eval-forecasts:
	@$(RUN_ENV) ./run.sh eval-forecasts

sync-all:
	@$(RUN_ENV) ./run.sh sync-all

serve:
	@$(RUN_ENV) ./run.sh serve

report:
	@$(RUN_ENV) ./run.sh report

run:
	@$(RUN_ENV) ./run.sh run
