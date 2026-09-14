"""Application use-case orchestrators for the Order Service."""

from decimal import Decimal
from uuid import UUID, uuid4

from streamforge.services.order.domain.models import Order, OrderStatus
from streamforge.services.order.infrastructure.publisher import OrderEventPublisher
from streamforge.services.order.infrastructure.repository import OrderRepository


class OrderService:
    """Use-case orchestrator for Order lifecycle operations.

    Orchestrates pure domain models and infrastructure adapters without
    depending on web or framework-specific constructs.

    Dual-Write Architectural Limitation (Sprint 2):
    ------------------------------------------------
    Persistence in PostgreSQL and Kafka publication are non-atomic. The repository
    commits PostgreSQL first; then the publisher produces to Kafka. If a process crash
    occurs between these operations, state may diverge. This intentional limitation
    will be resolved in Sprint 5 via Transactional Outbox + Debezium (CDC).
    """

    def __init__(
        self,
        repository: OrderRepository,
        publisher: OrderEventPublisher,
    ) -> None:
        self._repository = repository
        self._publisher = publisher

    async def create_order(
        self,
        customer_id: UUID,
        total_amount: Decimal,
        order_id: UUID | None = None,
    ) -> Order:
        """Create a new order, commit it to PostgreSQL, and publish OrderCreated to Kafka.

        Args:
            customer_id: Identifier of the customer placing the order.
            total_amount: Exact monetary value of the order.
            order_id: Optional client-specified order ID; generated if None.

        Returns:
            The created and persisted domain Order entity.
        """
        # 1. Instantiate pure domain entity
        order = Order(
            order_id=order_id or uuid4(),
            customer_id=customer_id,
            total_amount=total_amount,
            status=OrderStatus.CREATED,
        )

        # 2. Persist to PostgreSQL (explicit commit boundary in repository)
        persisted_order = await self._repository.create(order)

        # 3. Publish OrderCreated event to Kafka
        self._publisher.publish_order_created(persisted_order)

        return persisted_order
