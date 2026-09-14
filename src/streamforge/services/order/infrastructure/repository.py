"""PostgreSQL persistence repository adapter for the Order Service."""

from sqlalchemy.ext.asyncio import AsyncSession

from streamforge.services.order.domain.models import Order
from streamforge.services.order.infrastructure.models import OrderModel


class OrderRepository:
    """PostgreSQL adapter handling Order persistence.

    Enforces the transaction boundary by committing persisted state,
    and maps between pure domain entities and SQLAlchemy ORM models.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, order: Order) -> Order:
        """Persist a new domain Order to PostgreSQL and commit the transaction.

        Args:
            order: Pure domain Order entity to persist.

        Returns:
            The persisted domain Order entity.
        """
        model = OrderModel(
            id=str(order.order_id),
            customer_id=str(order.customer_id),
            total_amount=order.total_amount,
            status=order.status.value,
            created_at=order.created_at,
        )
        self._session.add(model)
        await self._session.commit()
        return order
