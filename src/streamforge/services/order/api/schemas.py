"""HTTP API Data Transfer Objects (DTOs) for the Order Service."""

from decimal import Decimal
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from streamforge.services.order.domain.models import Order


class OrderCreateRequest(BaseModel):
    """Request DTO payload for creating a new order."""

    model_config = ConfigDict(extra="forbid")

    customer_id: UUID = Field(..., description="Unique customer identifier")
    total_amount: Decimal = Field(
        ...,
        gt=Decimal("0.00"),
        description="Total monetary purchase amount",
    )
    order_id: UUID | None = Field(
        default=None,
        description="Optional client-specified order ID",
    )


class OrderResponse(BaseModel):
    """Response DTO payload representing order state."""

    model_config = ConfigDict(extra="forbid")

    order_id: UUID = Field(..., description="Unique order identifier")
    customer_id: UUID = Field(..., description="Customer identifier")
    total_amount: Decimal = Field(..., description="Total order amount")
    status: str = Field(..., description="Order status")

    @classmethod
    def from_domain(cls, order: Order) -> Self:
        """Construct an OrderResponse DTO from a domain Order entity."""
        return cls(
            order_id=order.order_id,
            customer_id=order.customer_id,
            total_amount=order.total_amount,
            status=order.status.value,
        )


# Backward compatibility alias
OrderCreate = OrderCreateRequest
