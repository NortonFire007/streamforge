# StreamForge Event Flow

## High-Level Event Lifecycle

StreamForge relies on asynchronous event choreographies and sagas across microservices.

```mermaid
sequenceDiagram
    autonumber
    actor Customer
    participant Order as Order Service
    participant Bus as Kafka Broker (Sprint 2)
    participant Fraud as Fraud Service
    participant Payment as Payment Service
    participant Inventory as Inventory Service
    participant Notify as Notification Service

    Customer->>Order: Create Order
    Order->>Bus: Event: OrderCreated
    par Fraud Analysis
        Bus->>Fraud: Consume OrderCreated
        Fraud->>Bus: Event: FraudCheckPassed / Rejected
    and Payment Processing
        Bus->>Payment: Consume OrderCreated
        Payment->>Bus: Event: PaymentCompleted / Failed
    and Inventory Reservation
        Bus->>Inventory: Consume OrderCreated
        Inventory->>Bus: Event: InventoryReserved / Failed
    end
    Bus->>Notify: Consume Outcome Events
    Notify->>Customer: Order Status Notification
```

## Sprint Progression
- **Sprint 1 (Current):** Foundation, packaging, code quality gates, PostgreSQL 17, and health/readiness endpoints.
- **Sprint 2:** Kafka cluster configuration, event schema definitions, base publisher/consumer abstractions.
- **Sprint 3+:** Saga execution, transactional outbox pattern, real-time analytics.
