# service-runtime Specification

## Purpose
TBD - created by archiving change bootstrap-project. Update Purpose after archive.
## Requirements
### Requirement: Centralized environment configuration
The system SHALL provide a typed configuration loader utilizing Pydantic Settings supporting `.env` and environment variable overrides.

#### Scenario: Loading default settings
- **WHEN** application initializes settings without environment overrides
- **THEN** settings resolve to default values for PostgreSQL host, port, database, and application name

### Requirement: Liveness health probe
The system SHALL expose a `GET /health` HTTP endpoint on the API service to indicate process liveness.

#### Scenario: Health check request
- **WHEN** client sends a GET request to `/health`
- **THEN** service responds with HTTP 200 and JSON body `{"status": "ok"}`

### Requirement: Readiness health probe
The system SHALL expose a `GET /ready` HTTP endpoint on the API service verifying backend dependencies including PostgreSQL connectivity.

#### Scenario: Database reachable
- **WHEN** PostgreSQL database is connected and operational
- **THEN** service responds with HTTP 200 and indicates database status is connected

#### Scenario: Database unreachable
- **WHEN** PostgreSQL database is offline or unreachable
- **THEN** service responds with HTTP 503 and indicates database connection error

