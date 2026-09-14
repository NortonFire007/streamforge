"""HTTP API route handlers for the Order Service."""

from fastapi import APIRouter, Depends, HTTPException, status

from streamforge.services.order.api.dependencies import get_order_service
from streamforge.services.order.api.schemas import OrderCreateRequest, OrderResponse
from streamforge.services.order.application.service import OrderService
from streamforge.services.order.infrastructure.database import check_database_connection

router = APIRouter()


@router.get("/health", status_code=status.HTTP_200_OK, tags=["Health"])
async def health_check() -> dict[str, str]:
    """Liveness probe returning basic service operational status."""
    return {"status": "ok"}


@router.get("/ready", status_code=status.HTTP_200_OK, tags=["Health"])
async def readiness_check() -> dict[str, str]:
    """Readiness probe checking database connectivity."""
    db_connected = await check_database_connection()
    if not db_connected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "not_ready", "database": "unreachable"},
        )
    return {"status": "ready", "database": "connected"}


@router.post(
    "/orders",
    status_code=status.HTTP_201_CREATED,
    response_model=OrderResponse,
    tags=["Orders"],
)
async def create_order(
    payload: OrderCreateRequest,
    service: OrderService = Depends(get_order_service),
) -> OrderResponse:
    """Create a new order and publish the corresponding event via OrderService."""
    order = await service.create_order(
        customer_id=payload.customer_id,
        total_amount=payload.total_amount,
        order_id=payload.order_id,
    )
    return OrderResponse.from_domain(order)
