## Context

StreamForge is architected as an event-driven platform composed of distributed services: Order, Payment, Inventory, Fraud, and Notification. Sprint 1 focuses exclusively on establishing the engineering foundation: packaging, code quality gates, containerized persistent store (PostgreSQL), configuration management, and baseline health/readiness endpoints.

## Goals / Non-Goals

**Goals:**
- Provide a clean multi-service Python 3.13 project layout.
- Fast dependency resolution and lockfile reproducibility using `uv`.
- Zero-tolerance code quality checks with `ruff` and strict `mypy`.
- Isolated PostgreSQL 17 database via Docker Compose with health checks.
- Resilient configuration system using Pydantic Settings with `.env` override capability.
- FastAPI-powered liveness probe (`GET /health`) and database readiness probe (`GET /ready`).
- Automated tests covering settings, health endpoints, and database connection.

**Non-Goals:**
- Message broker (Kafka) integration (deferred to Sprint 2).
- Full domain models and business logic implementations for order, payment, inventory, fraud, or notifications (deferred to subsequent sprints).
- Authentication and authorization layers (deferred).
- Complex event schema registries (deferred to Sprint 2).

## Decisions

- **Package Manager: `uv`**:
  - *Rationale*: Sub-second dependency resolution, unified Python toolchain management, built-in virtual environment and lockfile support.
  - *Alternative Considered*: `poetry` / `pipenv` / standard `pip` + `venv`. `uv` is significantly faster and standardizes modern Python workflow.
- **Web Framework: `FastAPI`**:
  - *Rationale*: Native async support, high performance, automatic OpenAPI documentation, tight integration with Pydantic v2.
- **Database Driver: `SQLAlchemy 2.0` with `psycopg3` (async)**:
  - *Rationale*: Standard in modern async Python; `psycopg` is the official next-gen PostgreSQL driver supporting async/await natively.
- **Service Layering**:
  - *Layout*: Each service under `src/services/<name>` has `api/`, `application/`, `domain/`, `infrastructure/`, and `main.py`.
  - *Shared Packages*: Cross-cutting concerns placed under `src/shared/` (`config`, `logging`, `events`, `observability`).

## Risks / Trade-offs

- [Risk: Local PostgreSQL port conflict on 5432] → Configurable via `POSTGRES_PORT` in `.env`.
- [Risk: Async database connection timeout during startup] → Health endpoint `/health` is decoupled from DB; `/ready` handles connection exceptions gracefully and reports detailed status.
