from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from cross_market_monitor.application.common import build_worker_runtime_state
from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.application.history.history_service import HistoryService
from cross_market_monitor.application.history.retention_service import RetentionService
from cross_market_monitor.application.monitor.poll_cycle import PollCycleService

LOGGER = logging.getLogger("cross_market_monitor")


class RuntimeService:
    def __init__(
        self,
        context: ServiceContext,
        history: HistoryService,
        retention: RetentionService,
        poll_cycle: PollCycleService,
    ) -> None:
        self.context = context
        self.history = history
        self.retention = retention
        self.poll_cycle = poll_cycle

    async def run_forever(self) -> None:
        while not self.context.stop_event.is_set():
            try:
                await self.poll_cycle.poll_once()
            except Exception:  # pragma: no cover - background task guard
                LOGGER.exception("Polling cycle failed")
            try:
                await asyncio.to_thread(self.retention.maybe_run)
            except Exception:  # pragma: no cover - background task guard
                LOGGER.exception("Retention cycle failed")
            try:
                await asyncio.wait_for(
                    self.context.stop_event.wait(),
                    timeout=self.context.config.app.poll_interval_sec,
                )
            except TimeoutError:
                continue

    async def _finish_startup(self) -> None:
        try:
            await self.history.maybe_backfill_startup_history()
            await self.history.maybe_backfill_tqsdk_shadow_history()
            self.history.start_tqsdk_shadow_collector()
            await asyncio.to_thread(self.retention.maybe_run, force=True)
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            raise
        except Exception:  # pragma: no cover - background task guard
            LOGGER.exception("Startup background tasks failed")
        finally:
            self.context.repository.upsert_runtime_state(build_worker_runtime_state(self.context))

    async def startup(self, *, background_history: bool = False) -> None:
        if self.context.startup_completed:
            return
        self.context.startup_completed = True
        self.context.repository.upsert_runtime_state(build_worker_runtime_state(self.context))
        if background_history:
            if self.context.startup_task is None or self.context.startup_task.done():
                self.context.startup_task = asyncio.create_task(self._finish_startup())
            return
        await self._finish_startup()

    async def shutdown(self) -> None:
        self.context.stop_event.set()
        self.context.shadow_stop_event.set()
        if self.context.startup_task is not None:
            self.context.startup_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.context.startup_task
            self.context.startup_task = None
        if self.context.shadow_thread is not None:
            await asyncio.to_thread(self.context.shadow_thread.join, 5.0)
            self.context.shadow_thread = None
        self.context.repository.upsert_runtime_state(build_worker_runtime_state(self.context))


class MonitorRuntime:
    def __init__(self, service) -> None:
        self.service = service
        self.task: asyncio.Task | None = None

    async def start(self, *, background_startup: bool = False) -> None:
        if self.task is None:
            await self.service.startup(background_history=background_startup)
            self.task = asyncio.create_task(self.service.run_forever())

    async def stop(self) -> None:
        await self.service.shutdown()
        if self.task is not None:
            with suppress(asyncio.CancelledError):
                await self.task
            self.task = None
