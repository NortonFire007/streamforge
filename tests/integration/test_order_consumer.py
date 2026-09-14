"""Integration test verifying standalone OrderEventConsumer against live Kafka."""

import asyncio
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from streamforge.services.order.consumer import OrderEventConsumer
from streamforge.services.order.infrastructure.database import get_engine
from streamforge.services.order.infrastructure.models import Base
from streamforge.services.order.main import app, lifespan
from streamforge.shared.config.settings import Settings


@pytest.mark.asyncio
async def test_order_consumer_reads_event_produced_by_post_orders() -> None:
    # Ensure tables exist in PostgreSQL
    async with get_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    customer_id = str(uuid.uuid4())
    total_amount = "129.50"
    created_order_id: str | None = None

    # Step 1: Produce an order event via POST /orders
    async with lifespan(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/orders",
                json={"customer_id": customer_id, "total_amount": total_amount},
            )
            assert response.status_code == 201
            data = response.json()
            created_order_id = data["order_id"]

    assert created_order_id is not None

    # Step 2: Use OrderEventConsumer with a unique group to read from the beginning
    test_settings = Settings(
        kafka_consumer_group_id=f"test-verify-group-{uuid.uuid4()}",
        kafka_consumer_client_id="test-verify-consumer",
        kafka_consumer_auto_offset_reset="earliest",
    )
    consumer = OrderEventConsumer(settings=test_settings, topic="orders.events")
    consumer.consumer.subscribe(
        [consumer.topic],
        on_assign=consumer.on_assign,
        on_revoke=consumer.on_revoke,
    )

    # Step 3: Poll until our created order event is found, processed, and committed
    found_event = None
    max_retries = 20
    try:
        for _ in range(max_retries):
            msg = consumer.consumer.poll(timeout=1.0)
            if msg is None:
                await asyncio.sleep(0.2)
                continue
            if msg.error():
                continue

            event_data = consumer.process_message(msg)
            if event_data and event_data.get("order_id") == created_order_id:
                consumer.consumer.commit(asynchronous=False)
                found_event = event_data
                break
    finally:
        consumer.close()

    assert found_event is not None
    assert found_event["customer_id"] == customer_id
    assert found_event["event_type"] == "order.created"
