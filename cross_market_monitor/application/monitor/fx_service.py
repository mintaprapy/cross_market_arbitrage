from __future__ import annotations

import asyncio
import logging
from time import perf_counter

from cross_market_monitor.application.common import FX_ALIGNMENT_TOLERANCE_SEC, FXContext, age_seconds
from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.application.monitor.source_health import SourceHealthRecorder
from cross_market_monitor.domain.models import FXQuote, MarketQuote

LOGGER = logging.getLogger("cross_market_monitor")


class FXService:
    def __init__(self, context: ServiceContext, health: SourceHealthRecorder) -> None:
        self.context = context
        self.health = health

    async def fetch_fx_context(self) -> FXContext:
        previous_rate = self.context.fx_window.last()
        cached_quote = self.context.latest_fx_quote
        if cached_quote is not None and not self.should_refresh_fx(cached_quote):
            self.context.latest_fx_is_live = True
            if self.context.latest_fx_last_live_at is None:
                self.context.latest_fx_last_live_at = cached_quote.ts
            self.context.latest_fx_frozen_since = None
            return FXContext(
                quote=cached_quote,
                jump_pct=None,
                previous_rate=previous_rate,
                is_live=True,
                fetched=False,
            )

        for source_name in self.fx_source_names():
            adapter = self.context.adapters[source_name]
            started = perf_counter()
            try:
                quote = await asyncio.to_thread(adapter.fetch_rate, "USD", "CNY")
                latency_ms = (perf_counter() - started) * 1000
                self.health.record_success(source_name, "USD/CNY", latency_ms)
                jump_pct = None
                if previous_rate not in (None, 0):
                    jump_pct = (quote.rate - previous_rate) / previous_rate
                self.context.fx_window.append(quote.rate)
                self.context.latest_fx_is_live = True
                self.context.latest_fx_last_live_at = quote.ts
                self.context.latest_fx_frozen_since = None
                return FXContext(
                    quote=quote,
                    jump_pct=jump_pct,
                    previous_rate=previous_rate,
                    is_live=True,
                    fetched=True,
                )
            except Exception as exc:
                latency_ms = (perf_counter() - started) * 1000
                self.health.record_failure(source_name, "USD/CNY", latency_ms, str(exc))
                LOGGER.warning("FX fetch failed from %s: %s", source_name, exc)

        self.context.latest_fx_is_live = False
        if cached_quote is not None:
            if self.context.latest_fx_frozen_since is None:
                self.context.latest_fx_frozen_since = self.context.latest_fx_last_live_at or cached_quote.ts
            return FXContext(
                quote=cached_quote,
                jump_pct=None,
                previous_rate=previous_rate,
                is_live=False,
                fetched=False,
            )
        self.context.latest_fx_frozen_since = None
        return FXContext(
            quote=None,
            jump_pct=None,
            previous_rate=previous_rate,
            is_live=False,
            fetched=False,
        )

    def should_refresh_fx(self, quote: FXQuote) -> bool:
        last_live_at = self.context.latest_fx_last_live_at or quote.ts
        return age_seconds(last_live_at) >= self.fx_poll_interval_sec()

    def fx_poll_interval_sec(self) -> int:
        return max(int(self.context.config.app.fx_poll_interval_sec), 1)

    def fx_source_names(self) -> list[str]:
        ordered: list[str] = []
        for source_name in [self.context.config.app.fx_source, *self.context.config.app.fx_backup_sources]:
            if source_name and source_name in self.context.adapters and source_name not in ordered:
                ordered.append(source_name)
        return ordered

    def effective_fx_for_domestic_quote(
        self,
        domestic_quote: MarketQuote | None,
        fx_context: FXContext,
    ) -> tuple[FXQuote | None, float | None]:
        effective_quote = self.effective_fx_for_quote(domestic_quote, fx_context)
        if effective_quote is None:
            return None, None
        live_quote = fx_context.quote
        if live_quote is not None and live_quote.ts == effective_quote.ts and live_quote.rate == effective_quote.rate:
            return effective_quote, fx_context.jump_pct
        return effective_quote, None

    def effective_fx_for_quote(
        self,
        domestic_quote: MarketQuote | None,
        fx_context: FXContext,
    ) -> FXQuote | None:
        live_quote = fx_context.quote
        if domestic_quote is None:
            return live_quote
        frozen_quote = self.context.repository.load_latest_fx_rate_before_any(
            self.fx_source_names(),
            domestic_quote.ts,
        )
        if frozen_quote is not None:
            return frozen_quote
        if live_quote is not None and live_quote.ts <= domestic_quote.ts:
            return live_quote
        if live_quote is not None and abs((live_quote.ts - domestic_quote.ts).total_seconds()) <= FX_ALIGNMENT_TOLERANCE_SEC:
            return live_quote
        aligned_quote = self.context.repository.load_nearest_fx_rate_any(
            self.fx_source_names(),
            domestic_quote.ts,
            max_delta_sec=FX_ALIGNMENT_TOLERANCE_SEC,
        )
        return aligned_quote or live_quote
