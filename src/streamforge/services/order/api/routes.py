from fastapi import APIRouter, HTTPException, status

from streamforge.services.order.infrastructure.database import check_database_connection

router = APIRouter(tags=["Health"])


@router.get("/health", status_code=status.HTTP_200_OK)
async def health_check() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready", status_code=status.HTTP_200_OK)
async def readiness_check() -> dict[str, str]:
    db_connected = await check_database_connection()
    if not db_connected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "not_ready", "database": "unreachable"},
        )
    return {"status": "ready", "database": "connected"}
