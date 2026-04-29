.PHONY: help install up down seed run-pipeline test test-integration dbt-run dbt-test ge-validate lint clean

PYTHON := python3.11
VENV := .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python

help:
	@echo "FinSight - common targets"
	@echo "  install           install python deps into $(VENV)"
	@echo "  up                docker compose up -d (MinIO, fake-gcs, MSSQL, Airflow)"
	@echo "  down              tear down local infra"
	@echo "  seed              create buckets and load sample data"
	@echo "  run-pipeline      trigger master DAG"
	@echo "  test              unit tests"
	@echo "  test-integration  integration tests against local stack"
	@echo "  dbt-run           dbt run on the duckdb mock target"
	@echo "  dbt-test          dbt test"
	@echo "  ge-validate       run Great Expectations checkpoints"
	@echo "  lint              ruff + mypy"
	@echo "  clean             remove caches and build artifacts"

install:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down -v

seed:
	$(PY) -m src.utils.seed_local_infra

run-pipeline:
	$(PY) -m src.cli run-pipeline --dag master_finsight_dag

test:
	$(PY) -m pytest tests/unit -v

test-integration:
	$(PY) -m pytest tests/integration -v

dbt-run:
	cd dbt && dbt run --target duckdb

dbt-test:
	cd dbt && dbt test --target duckdb

ge-validate:
	$(PY) -m src.quality.data_quality_checks --suite all

lint:
	$(VENV)/bin/ruff check src tests
	$(VENV)/bin/mypy src

clean:
	rm -rf $(VENV) .pytest_cache .mypy_cache .ruff_cache dbt/target dbt/logs
	find . -type d -name __pycache__ -exec rm -rf {} +
