# StreamForge

Event-driven order, payment, and real-time fraud analytics platform.

## Overview

StreamForge is built as a modular event-driven distributed system designed for resilient order processing, inventory synchronization, transactional payment handling, and sub-second fraud detection.

## Tech Stack (Sprint 1 Foundation)

- **Language & Runtime:** Python 3.13+
- **Packaging & Environment:** [uv](https://docs.astral.sh/uv/)
- **Web Framework:** [FastAPI](https://fastapi.tiangolo.com/)
- **Database ORM:** [SQLAlchemy 2.0](https://www.sqlalchemy.org/) (Async) with [psycopg3](https://www.psycopg.org/psycopg3/)
- **Configuration & Validation:** [Pydantic v2](https://docs.pydantic.dev/) & Pydantic Settings
- **Infrastructure:** Docker Compose (PostgreSQL 17)
- **Quality Gates:** Ruff, Mypy (Strict), Pytest (Asyncio)

## Quick Start

### 1. Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- Docker & Docker Compose
- Node.js 20.19+ (for OpenSpec CLI)

### 2. Environment Setup

```bash
# Copy environment variables
cp .env.example .env

# Install dependencies and sync virtual environment
uv sync
```

### 3. Start Infrastructure

```bash
docker compose up -d
```

### 4. Run Service

```bash
uv run uvicorn services.order.main:app --reload --port 8000
```

### 5. Health Checks

- Liveness: `curl http://localhost:8000/health` -> `{"status":"ok"}`
- Readiness: `curl http://localhost:8000/ready` -> `{"status":"ready","database":"connected"}`

### 6. Run Quality Gates

```bash
uv run pytest
uv run ruff check .
uv run mypy src tests
openspec validate
```
