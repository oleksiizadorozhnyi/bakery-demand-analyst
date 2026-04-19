"""Entry-point script for running the Bakery Demand Analytics API server."""

import uvicorn

from bakery_analyst.api.app import app
from bakery_analyst.config import settings

if __name__ == "__main__":
    uvicorn.run(app, host=settings.api_host, port=settings.api_port, reload=False)
