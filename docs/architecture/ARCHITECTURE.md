# StreamForge Architecture

## System Purpose

StreamForge is an event-driven platform handling orders, payments, inventory, fraud analytics, and notifications with high throughput, strict consistency where needed, and low latency.

## Architecture Blueprint

```mermaid
graph TD
    Client[Client / Gateway] --> OrderService[Order Service]
    Client --> PaymentService[Payment Service]
    
    subgraph "Core Microservices"
        OrderService
        PaymentService
        InventoryService[Inventory Service]
        FraudService[Fraud Service]
        NotificationService[Notification Service]
    end
    
    subgraph "Infrastructure"
        PostgreSQL[(PostgreSQL 17)]
        Kafka[Event Bus - Kafka (Sprint 2)]
    end
    
```

### Interactive Architecture Map

An interactive, self-contained HTML diagram generated with [Archify](https://github.com/tt-a1i/archify) is available at:
- **Interactive Map:** [streamforge.architecture.html](file:///e:/Programming/pythonProj/2026/streamforge/docs/architecture/streamforge.architecture.html)
- **Specification Source:** [streamforge.architecture.json](file:///e:/Programming/pythonProj/2026/streamforge/docs/architecture/streamforge.architecture.json)

Features include path focus views (Order/Payment Flow vs. Event-Driven Mesh), node inspection, dark/light themes, and export to PNG/SVG/Share Cards.


## Service Layout

Each service under `src/services/<service_name>/` follows clean architecture layering:
- `api/`: Transport layer, HTTP routes, controllers, request/response models.
- `application/`: Use cases, orchestrators, command/query handlers.
- `domain/`: Business entities, domain invariants, domain events.
- `infrastructure/`: Database adapters, external clients, messaging producers/consumers.
- `main.py`: ASGI application lifecycle and router registration.

## Shared Packages

Cross-cutting capabilities under `src/shared/`:
- `config/`: Pydantic settings loading typed configuration from `.env` and environment variables.
- `logging/`: Structured JSON logging and correlation IDs.
- `events/`: Base event contracts and schema serialization.
- `observability/`: Tracing, metrics, and health indicators.
