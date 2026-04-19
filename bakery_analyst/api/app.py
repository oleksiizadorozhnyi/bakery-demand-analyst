"""FastAPI application factory for the Bakery Demand Analytics API."""

from fastapi import FastAPI

from bakery_analyst.api.middleware import FailureMiddleware
from bakery_analyst.api.routes import router


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns:
        A fully configured FastAPI instance with middleware and routes registered.
    """
    app = FastAPI(
        title="Bakery Demand Analytics API",
        description=(
            "REST API for querying bakery demand forecasts. "
            "Supports optional failure injection for resilience testing."
        ),
        version="1.0.0",
    )

    app.add_middleware(FailureMiddleware)
    app.include_router(router)

    return app


app: FastAPI = create_app()
