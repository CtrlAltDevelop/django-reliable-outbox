# Everyday tasks. Each target is one command you could type yourself.

PG_IMAGE ?= postgres:16
PG_PORT ?= 55435
export DATABASE_URL ?= postgres://postgres:postgres@localhost:$(PG_PORT)/outbox

.PHONY: install lint format typecheck test check build pg-up pg-down bench clean

install:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy

test:
	uv run pytest

check: lint typecheck test

build:
	uv build

pg-up:
	docker run -d --name outbox-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=outbox \
		-p $(PG_PORT):5432 $(PG_IMAGE) -c max_connections=300

pg-down:
	docker rm -f outbox-pg

bench:
	uv run python bench/run.py

clean:
	rm -rf dist .mypy_cache .pytest_cache .ruff_cache
