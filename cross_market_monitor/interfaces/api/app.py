from __future__ import annotations

from contextlib import asynccontextmanager
from functools import lru_cache
import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from cross_market_monitor.application.service import MonitorRuntime, MonitorService
from cross_market_monitor.interfaces.api.routes_control import build_control_router
from cross_market_monitor.interfaces.api.routes_market import build_market_router
from cross_market_monitor.interfaces.api.routes_ops import build_ops_router

DASHBOARD_DIR = Path(__file__).resolve().parents[1] / "dashboard"
LOGGER = logging.getLogger("cross_market_monitor")


@lru_cache(maxsize=1)
def _dashboard_template() -> str:
    return (DASHBOARD_DIR / "index.html").read_text(encoding="utf-8")


def render_dashboard_html(title: str) -> str:
    return _dashboard_template().replace("__APP_TITLE__", title)


def create_app(
    service: MonitorService,
    *,
    run_runtime: bool = True,
    serve_dashboard: bool = True,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if not run_runtime:
            yield
            return
        runtime = MonitorRuntime(service)
        initial_delay_sec = 0.0
        try:
            await service.poll_once(pairs=service.context.dashboard_pairs)
            initial_delay_sec = service.config.app.poll_interval_sec
        except Exception:  # pragma: no cover - startup guard
            LOGGER.exception("Initial poll during API startup failed")
        await runtime.start(background_startup=True, initial_delay_sec=initial_delay_sec)
        yield
        await runtime.stop()

    app = FastAPI(title=service.config.app.name, lifespan=lifespan)
    app.include_router(build_market_router(service))
    app.include_router(build_ops_router(service))
    app.include_router(build_control_router(service))
    if serve_dashboard:
        app.mount("/dashboard", StaticFiles(directory=DASHBOARD_DIR), name="dashboard")

        @app.get("/", response_class=HTMLResponse)
        async def index() -> HTMLResponse:
            return HTMLResponse(render_dashboard_html(service.config.app.name))

    return app
