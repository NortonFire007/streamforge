"""Pure domain models for the Order Service."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid4


class OrderStatus(StrEnum):
    """Lifecycle status of an order."""

    CREATED = "created"
    PROCESSING = "processing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass
class Order:
    """Pure domain entity representing a customer order.

    Contains no database, web framework, or serialization dependencies.
    Uses UUID for unique identifiers and Decimal for monetary amounts to
    avoid floating-point precision issues.
    """

    customer_id: UUID
    total_amount: Decimal
    order_id: UUID = field(default_factory=uuid4)
    status: OrderStatus = OrderStatus.CREATED
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
