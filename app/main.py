# MongoDB Fleet Healthchecker — Main Entry Point
# ==============================================
# FastAPI lifespan: db connect → scheduler start → uvicorn

from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.db import connect, disconnect
from app.api import router
from app.scheduler import get_scheduler, setup_signal_handlers
from pathlib import Path

# ── Logging setup ───────────────────────────────────────────────


def setup_logging() -> None:
    """Structured logging (JSON format opsiyonel)."""
    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    if settings.log_json:
        import json_log_formatter

        formatter = json_log_formatter.JSONFormatter()
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(handler)
        root.setLevel(level)
    else:
        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

    # Silence third-party loggers
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("motor").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


# ── FastAPI app ─────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Uygulama lifecycle."""
    setup_logging()
    logger = logging.getLogger(__name__)

    # Startup
    logger.info("Starting MongoDB Fleet Healthchecker...")
    await connect()

    scheduler = await get_scheduler()
    await scheduler.start()

    loop = asyncio.get_running_loop()
    setup_signal_handlers(loop)

    logger.info("Ready — http://%s:%s", settings.api_host, settings.api_port)

    yield

    # Shutdown
    logger.info("Shutting down...")
    await scheduler.stop()
    await disconnect()


app = FastAPI(
    title="MongoDB Fleet Healthchecker",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)

# Static files (HTMX, CSS)
_static_dir = Path(__file__).parent / "static"
_static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


# ── CLI entry ───────────────────────────────────────────────────


def main() -> None:
    """CLI entry point."""
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_reload,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
