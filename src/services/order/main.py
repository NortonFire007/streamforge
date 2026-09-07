from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from services.order.api.routes import router as health_router
from shared.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Startup lifecycle hooks can be placed here
    yield
    # Cleanup / shutdown logic (e.g. engine disposal) can be placed here
    from services.order.infrastructure.database import _engine

    if _engine is not None:
        await _engine.dispose()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="StreamForge Order Service",
        description="Event-driven order management service",
        version="0.1.0",
        debug=settings.app_env == "development",
        lifespan=lifespan,
    )
    app.include_router(health_router)
    return app


app = create_app()
