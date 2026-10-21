import asyncio
import logging
from typing import Annotated, Literal

from asyncpg import PostgresError
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from payments_service.db.session import get_session

logger = logging.getLogger(__name__)
router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse()


@router.get("/health/ready", response_model=HealthResponse)
async def readiness(session: Annotated[AsyncSession, Depends(get_session)]) -> HealthResponse:
    try:
        async with asyncio.timeout(3):
            await session.execute(text("SELECT 1"))
    except (SQLAlchemyError, PostgresError, OSError, TimeoutError) as exc:
        logger.warning("Database readiness check failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="Database is unavailable",
        ) from exc
    return HealthResponse()
