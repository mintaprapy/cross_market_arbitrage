from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from cross_market_monitor.application.common import default_overseas_symbol, utc_now
from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.application.control.route_preference_service import RoutePreferenceService
from cross_market_monitor.application.history.history_service import HistoryService
from cross_market_monitor.application.history.retention_service import RetentionService
from cross_market_monitor.application.monitor.alert_service import AlertService
from cross_market_monitor.application.monitor.fx_service import FXService
from cross_market_monitor.application.monitor.poll_cycle import PollCycleService
from cross_market_monitor.application.monitor.quote_router import QuoteRouter
from cross_market_monitor.application.monitor.runtime import MonitorRuntime, RuntimeService
from cross_market_monitor.application.monitor.snapshot_builder import SnapshotBuilder
from cross_market_monitor.application.monitor.source_health import SourceHealthRecorder
from cross_market_monitor.application.monitor.summary_cache import SummaryCacheService
from cross_market_monitor.application.monitor.telegram_command_service import TelegramCommandService
from cross_market_monitor.application.query.query_service import QueryService
from cross_market_monitor.application.replay import ReplayAnalyzer
from cross_market_monitor.domain.models import FXQuote, MarketQuote, MonitorConfig, SourceConfig, SourceHealth
from cross_market_monitor.domain.stats import RollingWindow
from cross_market_monitor.infrastructure.http_client import HttpClient
from cross_market_monitor.infrastructure.marketdata.binance import BinanceFuturesAdapter
from cross_market_monitor.infrastructure.marketdata.cme import CmeReferenceAdapter
from cross_market_monitor.infrastructure.marketdata.frankfurter import FrankfurterFxAdapter
from cross_market_monitor.infrastructure.marketdata.gate import GateFuturesAdapter
from cross_market_monitor.infrastructure.marketdata.gate_tradfi import GateTradFiAdapter
from cross_market_monitor.infrastructure.marketdata.hyperliquid import HyperliquidAdapter
from cross_market_monitor.infrastructure.marketdata.open_er_api import OpenErApiFxAdapter
from cross_market_monitor.infrastructure.marketdata.okx import OkxSwapAdapter
from cross_market_monitor.infrastructure.marketdata.shfe import ShfeDelayMarketAdapter
from cross_market_monitor.infrastructure.marketdata.sina import SinaFuturesAdapter, SinaFxAdapter
from cross_market_monitor.infrastructure.marketdata.tqsdk import TqSdkMainAdapter
from cross_market_monitor.infrastructure.notifiers import build_notifier
from cross_market_monitor.infrastructure.repository import SQLiteRepository


class MockQuoteAdapter:
    def __init__(self, source_name: str) -> None:
        self.source_name = source_name

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        now = utc_now()
        base = 100.0 + (hash(symbol) % 1000) / 10
        return MarketQuote(
            source_name=self.source_name,
            symbol=symbol,
            label=label,
            ts=now,
            last=base,
            bid=base - 0.1,
            ask=base + 0.1,
            raw_payload="mock",
        )


class MockFxAdapter:
    def __init__(self, source_name: str, rate: float = 6.9) -> None:
        self.source_name = source_name
        self.rate = rate

    def fetch_rate(self, base: str, quote: str) -> FXQuote:
        return FXQuote(
            source_name=self.source_name,
            pair=f"{base}/{quote}",
            ts=utc_now(),
            rate=self.rate,
            raw_payload="mock",
        )


def _build_adapter(source_name: str, source_config: SourceConfig, timeout_sec: int):
    http_client = HttpClient(
        timeout_sec=timeout_sec,
        verify_ssl=source_config.verify_ssl,
    )
    if source_config.kind == "sina_futures":
        return SinaFuturesAdapter(source_name, source_config, http_client)
    if source_config.kind == "sina_fx":
        return SinaFxAdapter(source_name, source_config, http_client)
    if source_config.kind == "shfe_delaymarket":
        return ShfeDelayMarketAdapter(source_name, source_config, http_client)
    if source_config.kind == "tqsdk_main":
        return TqSdkMainAdapter(source_name, source_config, http_client)
    if source_config.kind == "okx_swap":
        return OkxSwapAdapter(source_name, source_config, http_client)
    if source_config.kind == "binance_futures":
        return BinanceFuturesAdapter(source_name, source_config, http_client)
    if source_config.kind == "gate_futures":
        return GateFuturesAdapter(source_name, source_config, http_client)
    if source_config.kind == "gate_tradfi":
        return GateTradFiAdapter(source_name, source_config, http_client)
    if source_config.kind == "hyperliquid":
        return HyperliquidAdapter(source_name, source_config, http_client)
    if source_config.kind == "cme_reference":
        return CmeReferenceAdapter(source_name, source_config, http_client)
    if source_config.kind == "frankfurter_fx":
        return FrankfurterFxAdapter(source_name, source_config, http_client)
    if source_config.kind == "open_er_api_fx":
        return OpenErApiFxAdapter(source_name, source_config, http_client)
    if source_config.kind == "mock_quote":
        return MockQuoteAdapter(source_name)
    if source_config.kind == "mock_fx":
        return MockFxAdapter(source_name, rate=source_config.fallback_rate or 6.9)
    raise ValueError(f"Unsupported source kind: {source_config.kind}")


class MonitorService:
    def __init__(
        self,
        config: MonitorConfig,
        repository: SQLiteRepository,
        *,
        preload_spread_windows: bool = True,
    ) -> None:
        self.config = config
        self.repository = repository
        self._local_tz = self._resolve_timezone(config.app.timezone)
        self._pair_map = {pair.group_name: pair for pair in config.pairs}
        self._enabled_pairs = [pair for pair in config.pairs if pair.enabled]
        self._dashboard_pairs = [pair for pair in self._enabled_pairs if pair.dashboard_enabled]
        self._preferred_domestic_symbols: dict[str, str] = {
            pair.group_name: pair.domestic_symbol for pair in self._enabled_pairs
        }
        self._preferred_overseas_symbols: dict[str, str] = {
            pair.group_name: default_overseas_symbol(pair) for pair in self._enabled_pairs
        }

        self.adapters = {
            source_name: _build_adapter(source_name, source_config, config.app.http_timeout_sec)
            for source_name, source_config in config.sources.items()
        }
        self.notifiers = [
            build_notifier(notifier, timezone_name=config.app.timezone)
            for notifier in config.notifiers
            if notifier.enabled
        ]
        zscore_max_age = (
            timedelta(days=config.app.zscore_window_days)
            if config.app.zscore_window_days > 0
            else None
        )
        self.windows = {
            pair.group_name: RollingWindow(
                None,
                max_age=zscore_max_age,
                bucket_size=timedelta(minutes=15),
            )
            for pair in self._enabled_pairs
        }
        self.fx_window = RollingWindow(
            config.app.fx_window_size,
            seed=repository.load_recent_fx_rates(config.app.fx_source, config.app.fx_window_size),
        )
        self.source_health = {
            source_name: SourceHealth(source_name=source_name, kind=source_config.kind)
            for source_name, source_config in config.sources.items()
        }
        self.replay = ReplayAnalyzer(
            repository,
            self._enabled_pairs,
            target_daily_vol_pct=config.app.replay_target_daily_vol_pct,
            bucket_minutes=config.app.replay_bucket_minutes,
            timezone_name=config.app.timezone,
            domestic_non_trading_dates_local=config.app.domestic_non_trading_dates_local,
            domestic_weekends_closed=config.app.domestic_weekends_closed,
        )

        self.context = ServiceContext(
            config=config,
            repository=repository,
            adapters=self.adapters,
            notifiers=self.notifiers,
            windows=self.windows,
            fx_window=self.fx_window,
            source_health=self.source_health,
            replay=self.replay,
            local_tz=self._local_tz,
            pair_map=self._pair_map,
            enabled_pairs=self._enabled_pairs,
            dashboard_pairs=self._dashboard_pairs,
            preferred_domestic_symbols=self._preferred_domestic_symbols,
            preferred_overseas_symbols=self._preferred_overseas_symbols,
        )
        self.latest_snapshots = self.context.latest_snapshots

        self.health_recorder = SourceHealthRecorder(self.context)
        self.route_preferences = RoutePreferenceService(self.context)
        self.history = HistoryService(self.context, self.route_preferences, self.health_recorder)
        self.retention = RetentionService(self.context)
        self.fx_service = FXService(self.context, self.health_recorder)
        self.quote_router = QuoteRouter(self.context, self.health_recorder)
        self.alert_service = AlertService(self.context, self.history)
        self.snapshot_builder = SnapshotBuilder(
            self.context,
            self.route_preferences,
            self.quote_router,
            self.fx_service,
            self.alert_service,
        )
        self.poll_cycle = PollCycleService(self.context, self.fx_service, self.snapshot_builder)
        self.query = QueryService(self.context, self.route_preferences, self.history)
        self.summary_cache = SummaryCacheService(self.query, self.config.app.export_dir)
        self.telegram_commands = TelegramCommandService(self.context, self.query)
        self.runtime = RuntimeService(
            self.context,
            self.history,
            self.retention,
            self.poll_cycle,
            self.telegram_commands,
            self.summary_cache,
        )

        self._preload_cached_state()
        self.route_preferences.load_persisted_preferences()
        if preload_spread_windows:
            self.history.refresh_spread_windows_from_local_history()

    @property
    def started_at(self) -> datetime:
        return self.context.started_at

    @property
    def last_poll_started_at(self) -> datetime | None:
        return self.context.last_poll_started_at

    @property
    def last_poll_finished_at(self) -> datetime | None:
        return self.context.last_poll_finished_at

    @property
    def is_polling(self) -> bool:
        return self.context.is_polling

    @property
    def total_cycles(self) -> int:
        return self.context.total_cycles

    @property
    def latest_fx_quote(self) -> FXQuote | None:
        return self.context.latest_fx_quote

    @property
    def latest_fx_jump_pct(self) -> float | None:
        return self.context.latest_fx_jump_pct

    async def run_forever(self, *, initial_delay_sec: float = 0.0) -> None:
        await self.runtime.run_forever(initial_delay_sec=initial_delay_sec)

    async def startup(self, *, background_history: bool = False) -> None:
        await self.runtime.startup(background_history=background_history)

    async def shutdown(self) -> None:
        await self.runtime.shutdown()

    async def poll_once(self, pairs=None):
        return await self.poll_cycle.poll_once(pairs=pairs)

    def get_health(self) -> dict:
        return self.query.get_health()

    def get_snapshot(self, *, include_cards: bool = False) -> dict:
        return self.query.get_snapshot(include_cards=include_cards)

    def get_snapshot_summary(self) -> dict:
        return self.query.get_snapshot_summary()

    def get_card_view(
        self,
        group_name: str,
        range_key: str | None = None,
        *,
        include_replay: bool = False,
    ) -> dict:
        return self.query.get_card_view(
            group_name,
            range_key=range_key,
            include_replay=include_replay,
        )

    def get_history(
        self,
        group_name: str,
        limit: int = 300,
        *,
        range_key: str | None = None,
    ) -> list[dict]:
        return self.history.get_history(group_name, limit=limit, range_key=range_key)

    def get_alerts(self, limit: int = 100) -> list[dict]:
        return self.query.get_alerts(limit)

    def get_notification_deliveries(self, limit: int = 100) -> list[dict]:
        return self.query.get_notification_deliveries(limit)

    def get_job_runs(self) -> list[dict]:
        return self.query.get_job_runs()

    def get_source_health(self) -> list[dict]:
        return self.query.get_source_health()

    def get_domestic_route_options(self, group_name: str, *, refresh_dynamic: bool = True) -> dict:
        return self.route_preferences.get_domestic_route_options(group_name, refresh_dynamic=refresh_dynamic)

    def set_domestic_route_preference(self, group_name: str, symbol: str | None) -> dict:
        result = self.route_preferences.set_domestic_route_preference(group_name, symbol)
        self.history.refresh_spread_windows_from_local_history(
            self.route_preferences.linked_variant_groups(group_name)
        )
        return result

    def get_overseas_route_options(self, group_name: str) -> dict:
        return self.route_preferences.get_overseas_route_options(group_name)

    def set_overseas_route_preference(self, group_name: str, symbol: str | None) -> dict:
        result = self.route_preferences.set_overseas_route_preference(group_name, symbol)
        self.history.refresh_spread_windows_from_local_history(
            self.route_preferences.linked_variant_groups(group_name)
        )
        return result

    def backfill_domestic_history(
        self,
        group_name: str,
        *,
        interval: str = "15m",
        range_key: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> dict:
        return self.history.backfill_domestic_history(
            group_name,
            interval=interval,
            range_key=range_key,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    def backfill_overseas_history(
        self,
        group_name: str,
        *,
        interval: str = "15m",
        range_key: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> dict:
        return self.history.backfill_overseas_history(
            group_name,
            interval=interval,
            range_key=range_key,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    def replay_summary(
        self,
        group_name: str,
        *,
        limit: int = 1000,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> dict:
        return self.query.replay_summary(group_name, limit=limit, start_ts=start_ts, end_ts=end_ts)

    def get_shadow_comparison(self, group_name: str, *, limit: int = 240) -> dict | None:
        return self.history.get_shadow_comparison(group_name, limit=limit)

    def ensure_overseas_history(
        self,
        group_name: str,
        *,
        range_key: str,
        start_ts: str | None,
        end_ts: str | None,
    ) -> None:
        pair = self._pair_map[group_name]
        self.history.ensure_overseas_history(pair, range_key=range_key, start_ts=start_ts, end_ts=end_ts)

    def _preload_cached_state(self) -> None:
        enabled_group_names = {pair.group_name for pair in self._enabled_pairs}
        latest_snapshots = self.repository.load_latest_snapshots()
        if latest_snapshots:
            self.context.latest_snapshots = {
                snapshot.group_name: snapshot
                for snapshot in latest_snapshots
                if snapshot.group_name in enabled_group_names
            }
            self.latest_snapshots = self.context.latest_snapshots
            if self.context.latest_snapshots:
                newest_snapshot = max(self.context.latest_snapshots.values(), key=lambda item: item.ts)
                self.context.last_poll_finished_at = newest_snapshot.ts
                self.context.latest_fx_jump_pct = newest_snapshot.fx_jump_pct
        runtime_state = self.repository.load_runtime_state("worker")
        if runtime_state is not None:
            self.context.total_cycles = runtime_state.total_cycles
            self.context.last_poll_started_at = runtime_state.last_poll_started_at
            self.context.last_poll_finished_at = runtime_state.last_poll_finished_at or self.context.last_poll_finished_at
            self.context.latest_fx_jump_pct = runtime_state.latest_fx_jump_pct
            self.context.latest_fx_is_live = runtime_state.fx_is_live
            self.context.latest_fx_last_live_at = runtime_state.fx_last_live_at
            self.context.latest_fx_frozen_since = runtime_state.fx_frozen_since
        for persisted in self.repository.load_source_health_state():
            if persisted.source_name in self.context.source_health:
                self.context.source_health[persisted.source_name] = persisted
        latest_fx_quote = self.repository.load_latest_fx_rate_any(self.fx_source_names())
        if latest_fx_quote is not None:
            self.context.latest_fx_quote = latest_fx_quote
            if self.context.latest_fx_last_live_at is None:
                self.context.latest_fx_last_live_at = latest_fx_quote.ts

    def fx_source_names(self) -> list[str]:
        ordered: list[str] = []
        for source_name in [self.config.app.fx_source, *self.config.app.fx_backup_sources]:
            if source_name and source_name in self.config.sources and source_name not in ordered:
                ordered.append(source_name)
        return ordered

    async def _maybe_backfill_tqsdk_shadow_history(self) -> None:
        await self.history.maybe_backfill_tqsdk_shadow_history()

    def _start_tqsdk_shadow_collector(self) -> None:
        self.history.start_tqsdk_shadow_collector()

    @staticmethod
    def _resolve_timezone(name: str):
        try:
            return ZoneInfo(name)
        except Exception:
            return UTC
