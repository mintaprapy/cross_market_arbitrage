from __future__ import annotations

import asyncio
import logging

from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.application.common import FXContext, build_worker_runtime_state, utc_now
from cross_market_monitor.application.monitor.fx_service import FXService
from cross_market_monitor.application.monitor.snapshot_builder import SnapshotBuilder
from cross_market_monitor.domain.models import SpreadSnapshot

LOGGER = logging.getLogger("cross_market_monitor")


class PollCycleService:
    def __init__(
        self,
        context: ServiceContext,
        fx_service: FXService,
        snapshot_builder: SnapshotBuilder,
    ) -> None:
        self.context = context
        self.fx_service = fx_service
        self.snapshot_builder = snapshot_builder

    async def poll_once(self, pairs=None) -> list[SpreadSnapshot]:
        if self.context.is_polling:
            return list(self.context.latest_snapshots.values())

        self.context.is_polling = True
        self.context.last_poll_started_at = utc_now()
        self.context.repository.upsert_runtime_state(build_worker_runtime_state(self.context))
        self.snapshot_builder.quote_router.reset_cycle_cache()
        try:
            target_pairs = pairs or [
                pair
                for pair in self.context.config.pairs
                if pair.enabled
            ]
            now_local = self.context.last_poll_started_at.astimezone(self.context.local_tz)
            should_collect_session_data = any(
                self.snapshot_builder.is_domestic_session_open(pair, now_local)
                for pair in target_pairs
            )
            if should_collect_session_data:
                fx_context = await self.fx_service.fetch_fx_context()
                self.context.latest_fx_quote = fx_context.quote
                self.context.latest_fx_jump_pct = fx_context.jump_pct
                if fx_context.quote and fx_context.fetched:
                    self.context.repository.insert_fx_rate(
                        fx_context.quote,
                        timezone_name=self.context.config.app.timezone,
                    )
            else:
                fx_context = FXContext(
                    quote=self.context.latest_fx_quote,
                    jump_pct=None,
                    previous_rate=self.context.fx_window.last(),
                    is_live=self.context.latest_fx_is_live,
                    fetched=False,
                )
            tasks = [
                asyncio.create_task(self.snapshot_builder.build_snapshot(pair, fx_context))
                for pair in target_pairs
            ]
            timeout_sec = max(int(self.context.config.app.poll_timeout_sec), 1)
            try:
                snapshots = await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout_sec)
            except TimeoutError:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                LOGGER.warning("Polling cycle timed out after %ss", timeout_sec)
                return list(self.context.latest_snapshots.values())
            self.context.last_poll_finished_at = utc_now()
            self.context.total_cycles += 1
            self.context.repository.upsert_runtime_state(build_worker_runtime_state(self.context))
            return snapshots
        finally:
            self.snapshot_builder.quote_router.reset_cycle_cache()
            self.context.is_polling = False
            self.context.repository.upsert_runtime_state(build_worker_runtime_state(self.context))
