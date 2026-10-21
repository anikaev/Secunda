import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from payments_service.api.auth import require_api_key
from payments_service.api.health import router as health_router
from payments_service.api.payments import router as payments_router
from payments_service.config import Settings, get_settings
from payments_service.db.session import Database


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logging.basicConfig(level=settings.log_level)
        database = Database(settings)
        app.state.database = database
        try:
            yield
        finally:
            await database.close()

    app = FastAPI(
        title="Async Payment Service",
        version="0.1.0",
        lifespan=lifespan,
        dependencies=[Depends(require_api_key)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = settings
    app.include_router(health_router)
    app.include_router(payments_router)
    return app
