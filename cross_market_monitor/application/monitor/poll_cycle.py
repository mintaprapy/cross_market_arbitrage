from __future__ import annotations

import asyncio

from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.application.common import build_worker_runtime_state, utc_now
from cross_market_monitor.application.monitor.fx_service import FXService
from cross_market_monitor.application.monitor.snapshot_builder import SnapshotBuilder
from cross_market_monitor.domain.models import SpreadSnapshot


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
            fx_context = await self.fx_service.fetch_fx_context()
            self.context.latest_fx_quote = fx_context.quote
            self.context.latest_fx_jump_pct = fx_context.jump_pct
            if fx_context.quote and fx_context.fetched:
                self.context.repository.insert_fx_rate(
                    fx_context.quote,
                    timezone_name=self.context.config.app.timezone,
                )

            target_pairs = pairs or [
                pair
                for pair in self.context.config.pairs
                if pair.enabled
            ]
            tasks = [
                self.snapshot_builder.build_snapshot(pair, fx_context)
                for pair in target_pairs
            ]
            snapshots = await asyncio.gather(*tasks)
            self.context.last_poll_finished_at = utc_now()
            self.context.total_cycles += 1
            self.context.repository.upsert_runtime_state(build_worker_runtime_state(self.context))
            return snapshots
        finally:
            self.snapshot_builder.quote_router.reset_cycle_cache()
            self.context.is_polling = False
            self.context.repository.upsert_runtime_state(build_worker_runtime_state(self.context))
