.PHONY: help kafka-topics test lint format

help:
	@echo "StreamForge Development Commands:"
	@echo "  make kafka-topics  - Idempotently create default Kafka topics (orders.events)"
	@echo "  make test          - Run all unit and integration tests"
	@echo "  make lint          - Run ruff check and mypy"
	@echo "  make format        - Run ruff check --fix and format"

kafka-topics:
	uv run python -m streamforge.kafka.admin create-topics

test:
	uv run pytest tests/unit tests/integration

lint:
	uv run ruff check src tests
	uv run mypy src tests

format:
	uv run ruff check --fix src tests
	uv run ruff format src tests
