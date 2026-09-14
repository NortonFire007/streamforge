import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from streamforge.services.order.api.routes import router
from streamforge.services.order.infrastructure.database import close_database, init_database
from streamforge.shared.config import get_settings
from streamforge.shared.kafka import build_producer


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage Order Service application lifecycle.

    Educational Simplification Notice (Sprint 2):
    ---------------------------------------------
    Synchronous `confluent_kafka.Producer` operations inside FastAPI handlers and lifespan
    are an intentional educational simplification for Sprint 2 to study Kafka client
    mechanics (`produce`, `poll`, `flush`, `acks`, `retries`) without wrapping them in
    asynchronous background broker abstractions. This is not the final production async
    integration pattern.
    """
    init_database()

    # Initialize Kafka Producer singleton in application state
    settings = get_settings()
    app.state.producer = build_producer(settings)

    yield

    # Offload Producer flush to an executor thread to avoid blocking the event loop
    producer = getattr(app.state, "producer", None)
    if producer is not None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, producer.flush, 10)

    await close_database()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="StreamForge Order Service",
        description="Event-driven order management service",
        version="0.1.0",
        debug=settings.app_env == "development",
        lifespan=lifespan,
    )
    app.include_router(router)
    return app


app = create_app()
