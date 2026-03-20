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

    async def startup(self) -> None:
        if self.context.startup_completed:
            return
        await self.history.maybe_backfill_tqsdk_shadow_history()
        self.history.start_tqsdk_shadow_collector()
        await asyncio.to_thread(self.retention.maybe_run, force=True)
        self.context.repository.upsert_runtime_state(build_worker_runtime_state(self.context))
        self.context.startup_completed = True

    async def shutdown(self) -> None:
        self.context.stop_event.set()
        self.context.shadow_stop_event.set()
        if self.context.shadow_thread is not None:
            await asyncio.to_thread(self.context.shadow_thread.join, 5.0)
            self.context.shadow_thread = None
        self.context.repository.upsert_runtime_state(build_worker_runtime_state(self.context))


class MonitorRuntime:
    def __init__(self, service) -> None:
        self.service = service
        self.task: asyncio.Task | None = None

    async def start(self) -> None:
        if self.task is None:
            await self.service.startup()
            self.task = asyncio.create_task(self.service.run_forever())

    async def stop(self) -> None:
        await self.service.shutdown()
        if self.task is not None:
            with suppress(asyncio.CancelledError):
                await self.task
            self.task = None
