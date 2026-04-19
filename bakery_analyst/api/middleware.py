"""Failure-injection middleware for the Bakery Demand Analytics API."""

import asyncio
import random

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from bakery_analyst.config import settings


class FailureMiddleware(BaseHTTPMiddleware):
    """Middleware that optionally injects artificial failures into responses.

    When ``settings.failure_enabled`` is ``True`` the middleware evaluates two
    independent failure modes for every incoming request, in order:

    1. **HTTP 500** — returns a simulated server-error response with probability
       ``settings.error_500_probability``.
    2. **Artificial delay** — sleeps for ``settings.delay_seconds`` with
       probability ``settings.delay_probability``.

    If neither condition triggers, the request passes through to the next
    handler unchanged.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        """Process the request, potentially injecting failures before forwarding.

        Args:
            request: The incoming HTTP request.
            call_next: Callable that forwards the request to the next handler.

        Returns:
            Either a simulated 500 ``JSONResponse`` or the actual application
            response, optionally after an artificial delay.
        """
        if settings.failure_enabled:
            if random.random() < settings.error_500_probability:
                return JSONResponse(
                    status_code=500,
                    content={"detail": "Simulated server error"},
                )

            if random.random() < settings.delay_probability:
                await asyncio.sleep(settings.delay_seconds)

        return await call_next(request)
