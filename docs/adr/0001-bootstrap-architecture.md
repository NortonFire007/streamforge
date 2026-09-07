# ADR 0001: Project Bootstrap Architecture and Toolchain

## Status
Accepted

## Context
StreamForge requires a performant, maintainable, and type-safe foundation for an event-driven microservice system. Decisions made at the bootstrap phase determine developer velocity, test reliability, and production operational stability.

## Decisions
1. **Python 3.13+ & uv**: We adopt Python 3.13 and Astral's `uv` for package and virtual environment management. `uv` ensures reproducible, lockfile-backed builds and fast resolution.
2. **FastAPI & Pydantic v2**: High-performance asynchronous API framework with native OpenAPI schema generation and strong request validation.
3. **SQLAlchemy 2.0 & psycopg3**: Async connection pooling and type-annotated ORM querying against PostgreSQL 17.
4. **Spec-Driven Development (OpenSpec)**: All major features, architecture shifts, and cross-cutting requirements are defined and validated via OpenSpec changes before implementation.
5. **Code Quality Gates**: Zero tolerance for formatting drift (`ruff`), lint violations (`ruff check`), or type inconsistencies (`mypy --strict`).

## Consequences
- **Positive**: Strict typing and fast local development cycles; automated CI gates prevent regressions; reproducible environments via Docker and lockfiles.
- **Trade-offs**: Requires disciplined adherence to type annotations and async conventions across all modules.
