# Phase 1: rebuild the LendingClub accepted-loans star schema.
UV ?= uv

.PHONY: help download build build-sample test check clean

help:
	@echo "Phase 1 targets"
	@echo "  make download       Download accepted loans from Kaggle into data/raw (needs .env)"
	@echo "  make build          Rebuild data/warehouse.duckdb from the raw CSV"
	@echo "  make build-sample   Rebuild data/sample_warehouse.duckdb from the synthetic fixture"
	@echo "  make test           Run the test suite on the synthetic fixture"
	@echo "  make check          Rebuild the sample warehouse and print quality-check status"
	@echo "  make clean          Delete local DuckDB files (does not delete data/raw)"

download:
	$(UV) run python -m credit_risk_agent download

build:
	$(UV) run python -m credit_risk_agent build $(if $(SOURCE),--source $(SOURCE),) $(if $(WAREHOUSE),--warehouse $(WAREHOUSE),)

build-sample:
	$(UV) run python -m credit_risk_agent build \
		--source tests/fixtures/accepted_sample.csv \
		--warehouse data/sample_warehouse.duckdb \
		--no-download

test:
	$(UV) run pytest

check: build-sample
	$(UV) run python -m credit_risk_agent check --warehouse data/sample_warehouse.duckdb

clean:
	rm -f data/*.duckdb data/*.duckdb.wal data/*.duckdb.building data/*.duckdb.building.wal
