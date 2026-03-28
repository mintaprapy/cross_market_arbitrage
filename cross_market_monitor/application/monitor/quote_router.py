from __future__ import annotations

import asyncio
from time import perf_counter

from cross_market_monitor.application.common import RouteFetchResult, active_trading_session_window, utc_now
from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.application.monitor.source_health import SourceHealthRecorder
from cross_market_monitor.domain.models import MarketQuote, PairConfig, QuoteRouteConfig

TQSDK_FETCH_LEAD_SEC = 300
TQSDK_FETCH_GRACE_SEC = 300


class QuoteRouter:
    def __init__(self, context: ServiceContext, health: SourceHealthRecorder) -> None:
        self.context = context
        self.health = health
        self._candidate_tasks: dict[tuple[str, str, str], asyncio.Task[RouteFetchResult]] = {}

    def reset_cycle_cache(self) -> None:
        self._candidate_tasks.clear()

    async def fetch_leg_quote(
        self,
        group_name: str,
        leg_type: str,
        candidates: list[QuoteRouteConfig],
    ) -> tuple[MarketQuote | None, list[MarketQuote], list[str], dict]:
        pair = self.context.pair_map.get(group_name)
        enabled_candidates = [candidate for candidate in candidates if candidate.enabled]
        tasks = [self.fetch_candidate(group_name, leg_type, candidate, pair) for candidate in enabled_candidates]
        results = await asyncio.gather(*tasks)

        errors: list[str] = []
        attempts: list[dict] = []
        selected: MarketQuote | None = None
        successful_quotes: list[MarketQuote] = []
        for result in results:
            attempts.append(
                {
                    "source": result.candidate.source,
                    "symbol": result.candidate.symbol,
                    "label": result.candidate.label,
                    "success": result.quote is not None,
                    "latency_ms": round(result.latency_ms, 2),
                    "error": result.error,
                    "persisted": result.persist,
                    "skipped": result.skipped,
                    "skip_reason": result.skip_reason,
                }
            )
            if result.quote is not None:
                if result.persist:
                    self.context.repository.insert_raw_quote(
                        group_name,
                        leg_type,
                        result.quote,
                        timezone_name=self.context.config.app.timezone,
                    )
                successful_quotes.append(result.quote)
                if selected is None:
                    selected = result.quote
            elif result.error:
                errors.append(f"{leg_type}:{result.candidate.source}:{result.error}")

        selected_detail = None
        if selected is not None:
            selected_detail = {
                "source": selected.source_name,
                "symbol": selected.symbol,
                "label": selected.label,
            }
        return selected, successful_quotes, errors, {"attempts": attempts, "selected": selected_detail}

    async def fetch_candidate(
        self,
        group_name: str,
        leg_type: str,
        candidate: QuoteRouteConfig,
        pair: PairConfig | None,
    ) -> RouteFetchResult:
        cached_result = self._load_cached_quote_if_fetch_window_closed(group_name, leg_type, candidate, pair)
        if cached_result is not None:
            return cached_result

        key = (candidate.source, candidate.symbol, candidate.label)
        cached_task = self._candidate_tasks.get(key)
        if cached_task is None:
            cached_task = asyncio.create_task(self._fetch_candidate_uncached(candidate))
            self._candidate_tasks[key] = cached_task
        return await cached_task

    def _load_cached_quote_if_fetch_window_closed(
        self,
        group_name: str,
        leg_type: str,
        candidate: QuoteRouteConfig,
        pair: PairConfig | None,
    ) -> RouteFetchResult | None:
        if leg_type != "domestic" or pair is None or not pair.trading_sessions_local:
            return None

        source = self.context.config.sources.get(candidate.source)
        if source is None or source.kind != "tqsdk_main":
            return None

        now_utc = utc_now()
        now_local = now_utc.astimezone(self.context.local_tz)
        fetch_window = active_trading_session_window(
            now_local,
            pair.trading_sessions_local,
            lead_sec=TQSDK_FETCH_LEAD_SEC,
            grace_sec=TQSDK_FETCH_GRACE_SEC,
            non_trading_dates=self.context.config.app.domestic_non_trading_dates_local,
            weekends_closed=self.context.config.app.domestic_weekends_closed,
        )
        if fetch_window is not None:
            return None

        cached_quote = self.context.repository.load_latest_raw_quote_before(
            group_name,
            leg_type,
            candidate.symbol,
            now_utc,
        )
        return RouteFetchResult(
            candidate=candidate,
            quote=cached_quote,
            error=None,
            latency_ms=0.0,
            persist=False,
            skipped=True,
            skip_reason="outside_tqsdk_fetch_window",
        )

    async def _fetch_candidate_uncached(self, candidate: QuoteRouteConfig) -> RouteFetchResult:
        started = perf_counter()
        try:
            adapter = self.context.adapters[candidate.source]
            quote = await asyncio.to_thread(adapter.fetch_quote, candidate.symbol, candidate.label)
            latency_ms = (perf_counter() - started) * 1000
            self.health.record_success(candidate.source, candidate.symbol, latency_ms)
            return RouteFetchResult(candidate=candidate, quote=quote, error=None, latency_ms=latency_ms)
        except Exception as exc:
            latency_ms = (perf_counter() - started) * 1000
            self.health.record_failure(candidate.source, candidate.symbol, latency_ms, str(exc))
            return RouteFetchResult(candidate=candidate, quote=None, error=str(exc), latency_ms=latency_ms)
