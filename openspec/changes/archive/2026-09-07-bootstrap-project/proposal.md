## Why

StreamForge requires a robust foundation for an event-driven platform handling orders, payments, inventory, fraud analytics, and notifications. Setting up a standardized project structure, dependency management, code quality tooling, Dockerized PostgreSQL, and health check endpoints establishes reliable baseline engineering practices before building business services.

## What Changes

- Initialize Python 3.13 project with `uv` dependency management and lockfile.
- Configure code quality tooling: `ruff` (linting & formatting), `mypy` (strict static typing), and `pytest` (asynchronous unit and integration testing).
- Establish a multi-service clean architecture skeleton under `src/services/` (`order`, `payment`, `inventory`, `fraud`, `notification`) and shared packages under `src/shared/`.
- Provide PostgreSQL 17 database environment using Docker Compose.
- Implement centralized environment configuration using Pydantic Settings.
- Implement FastAPI application entrypoint in Order Service with `GET /health` and `GET /ready` (database connectivity probe).
- Establish test hierarchy (`unit`, `integration`, `contract`, `e2e`) and write initial automated tests.
- Set up project documentation and Git tracking.

## Capabilities

### New Capabilities
- `project-foundation`: Project structure, dependency management with `uv`, linting with `ruff`, type checking with `mypy`, and testing framework with `pytest`.
- `service-runtime`: Shared environment configuration, FastAPI application bootstrap, `/health` liveness probe, and `/ready` database connectivity probe.

### Modified Capabilities
<!-- None: Greenfield project -->

## Impact

- Repository filesystem layout created.
- New dependencies: `fastapi`, `uvicorn`, `pydantic`, `pydantic-settings`, `sqlalchemy`, `alembic`, `psycopg[binary]`.
- Dev dependencies: `pytest`, `pytest-asyncio`, `httpx`, `ruff`, `mypy`.
- PostgreSQL 17 service running via Docker on port 5432.
