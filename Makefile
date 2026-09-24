.PHONY: install up down migrate api worker seed test lint

install:
	uv sync

up:
	docker compose up -d db redis

down:
	docker compose down

migrate:
	uv run alembic upgrade head

api:
	uv run uvicorn notify_queue.api.main:app --reload --port 8000

worker:
	uv run python -m notify_queue.worker

seed:
	uv run python seed.py

test:
	uv run pytest -q

lint:
	uv run ruff check src tests
