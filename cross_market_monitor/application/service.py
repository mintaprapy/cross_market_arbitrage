from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import UTC, datetime

from cross_market_monitor.domain.formulas import (
    compute_executable_spreads,
    compute_spread,
    normalize_domestic_quote,
)
from cross_market_monitor.domain.models import (
    AlertEvent,
    FXQuote,
    MarketQuote,
    MonitorConfig,
    NotificationDelivery,
    PairConfig,
    RuntimeHealth,
    SourceConfig,
    SpreadSnapshot,
)
from cross_market_monitor.domain.stats import RollingWindow
from cross_market_monitor.infrastructure.http_client import HttpClient
from cross_market_monitor.infrastructure.marketdata.frankfurter import FrankfurterFxAdapter
from cross_market_monitor.infrastructure.marketdata.okx import OkxSwapAdapter
from cross_market_monitor.infrastructure.marketdata.sina import SinaFuturesAdapter
from cross_market_monitor.infrastructure.notifiers import build_notifier
from cross_market_monitor.infrastructure.repository import SQLiteRepository
from cross_market_monitor.application.replay import ReplayAnalyzer

LOGGER = logging.getLogger("cross_market_monitor")


def utc_now() -> datetime:
    return datetime.now(UTC)


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


def _build_adapter(source_name: str, source_config: SourceConfig, http_client: HttpClient):
    if source_config.kind == "sina_futures":
        return SinaFuturesAdapter(source_name, source_config, http_client)
    if source_config.kind == "okx_swap":
        return OkxSwapAdapter(source_name, source_config, http_client)
    if source_config.kind == "frankfurter_fx":
        return FrankfurterFxAdapter(source_name, source_config, http_client)
    if source_config.kind == "mock_quote":
        return MockQuoteAdapter(source_name)
    if source_config.kind == "mock_fx":
        return MockFxAdapter(source_name, rate=source_config.fallback_rate or 6.9)
    raise ValueError(f"Unsupported source kind: {source_config.kind}")


class MonitorService:
    def __init__(self, config: MonitorConfig, repository: SQLiteRepository) -> None:
        self.config = config
        self.repository = repository
        self.started_at = utc_now()
        self.last_poll_started_at: datetime | None = None
        self.last_poll_finished_at: datetime | None = None
        self.is_polling = False
        self.total_cycles = 0
        self.latest_fx_quote: FXQuote | None = None
        self.latest_snapshots: dict[str, SpreadSnapshot] = {}
        self._stop_event = asyncio.Event()
        self._cooldowns: dict[tuple[str, str], datetime] = {}

        http_client = HttpClient(timeout_sec=config.app.http_timeout_sec)
        self.adapters = {
            source_name: _build_adapter(source_name, source_config, http_client)
            for source_name, source_config in config.sources.items()
        }
        self.notifiers = [build_notifier(notifier) for notifier in config.notifiers if notifier.enabled]
        self.windows = {
            pair.group_name: RollingWindow(
                config.app.rolling_window_size,
                seed=repository.load_recent_spreads(pair.group_name, config.app.rolling_window_size),
            )
            for pair in config.pairs
            if pair.enabled
        }
        self.replay = ReplayAnalyzer(repository, [pair for pair in config.pairs if pair.enabled])

    async def run_forever(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.poll_once()
            except Exception:  # pragma: no cover - background task guard
                LOGGER.exception("Polling cycle failed")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.config.app.poll_interval_sec)
            except TimeoutError:
                continue

    async def shutdown(self) -> None:
        self._stop_event.set()

    async def poll_once(self) -> list[SpreadSnapshot]:
        if self.is_polling:
            return list(self.latest_snapshots.values())

        self.is_polling = True
        self.last_poll_started_at = utc_now()

        try:
            fx_quote = await self._fetch_fx_quote()
            self.latest_fx_quote = fx_quote
            if fx_quote:
                self.repository.insert_fx_rate(fx_quote)

            tasks = [self._build_snapshot(pair, fx_quote) for pair in self.config.pairs if pair.enabled]
            snapshots = await asyncio.gather(*tasks)
            self.last_poll_finished_at = utc_now()
            self.total_cycles += 1
            return snapshots
        finally:
            self.is_polling = False

    def get_health(self) -> dict:
        health = RuntimeHealth(
            started_at=self.started_at,
            last_poll_started_at=self.last_poll_started_at,
            last_poll_finished_at=self.last_poll_finished_at,
            poll_interval_sec=self.config.app.poll_interval_sec,
            rolling_window_size=self.config.app.rolling_window_size,
            history_limit=self.config.app.history_limit,
            is_polling=self.is_polling,
            total_cycles=self.total_cycles,
            latest_fx_rate=self.latest_fx_quote.rate if self.latest_fx_quote else None,
        ).model_dump(mode="json")

        health["pairs"] = [
            {
                "group_name": pair.group_name,
                "status": self.latest_snapshots.get(pair.group_name).status if pair.group_name in self.latest_snapshots else "waiting",
            }
            for pair in self.config.pairs
            if pair.enabled
        ]
        return health

    def get_snapshot(self) -> dict:
        return {
            "as_of": self.last_poll_finished_at.isoformat() if self.last_poll_finished_at else None,
            "health": self.get_health(),
            "snapshots": [
                snapshot.model_dump(mode="json")
                for snapshot in sorted(self.latest_snapshots.values(), key=lambda item: item.group_name)
            ],
        }

    def get_history(self, group_name: str, limit: int = 300) -> list[dict]:
        return self.repository.fetch_history(group_name, limit)

    def get_alerts(self, limit: int = 100) -> list[dict]:
        return self.repository.fetch_alerts(limit)

    def get_notification_deliveries(self, limit: int = 100) -> list[dict]:
        return self.repository.fetch_notification_deliveries(limit)

    def replay_summary(
        self,
        group_name: str,
        *,
        limit: int = 1000,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> dict:
        return self.replay.analyze(group_name, limit=limit, start_ts=start_ts, end_ts=end_ts)

    async def _fetch_fx_quote(self) -> FXQuote | None:
        source_name = self.config.app.fx_source
        source_config = self.config.sources[source_name]
        adapter = self.adapters[source_name]

        try:
            return await asyncio.to_thread(adapter.fetch_rate, "USD", "CNY")
        except Exception as exc:
            LOGGER.warning("FX fetch failed from %s: %s", source_name, exc)
            if source_config.fallback_rate:
                return FXQuote(
                    source_name=source_name,
                    pair="USD/CNY",
                    ts=utc_now(),
                    rate=source_config.fallback_rate,
                    raw_payload="fallback_rate",
                )
            return None

    async def _build_snapshot(self, pair: PairConfig, fx_quote: FXQuote | None) -> SpreadSnapshot:
        domestic_task = asyncio.to_thread(
            self.adapters[pair.domestic_source].fetch_quote, pair.domestic_symbol, pair.domestic_label
        )
        overseas_task = asyncio.to_thread(
            self.adapters[pair.overseas_source].fetch_quote, pair.overseas_symbol, pair.overseas_label
        )
        domestic_result, overseas_result = await asyncio.gather(domestic_task, overseas_task, return_exceptions=True)

        domestic_quote = domestic_result if isinstance(domestic_result, MarketQuote) else None
        overseas_quote = overseas_result if isinstance(overseas_result, MarketQuote) else None

        errors: list[str] = []
        if isinstance(domestic_result, Exception):
            errors.append(f"domestic: {domestic_result}")
        if isinstance(overseas_result, Exception):
            errors.append(f"overseas: {overseas_result}")
        if fx_quote is None:
            errors.append("fx: unavailable")

        if domestic_quote:
            self.repository.insert_raw_quote(pair.group_name, "domestic", domestic_quote)
        if overseas_quote:
            self.repository.insert_raw_quote(pair.group_name, "overseas", overseas_quote)

        normalized_quote = normalize_domestic_quote(
            pair,
            fx_quote.rate if fx_quote else None,
            domestic_quote.last if domestic_quote else None,
            domestic_quote.bid if domestic_quote else None,
            domestic_quote.ask if domestic_quote else None,
        )

        spread, spread_pct = compute_spread(
            overseas_quote.last if overseas_quote else None,
            normalized_quote.last,
        )
        exec_buy_domestic, exec_buy_overseas = compute_executable_spreads(
            normalized_quote.bid,
            normalized_quote.ask,
            overseas_quote.bid if overseas_quote else None,
            overseas_quote.ask if overseas_quote else None,
        )

        domestic_age = _age_seconds(domestic_quote.ts) if domestic_quote else None
        overseas_age = _age_seconds(overseas_quote.ts) if overseas_quote else None
        fx_age = _age_seconds(fx_quote.ts) if fx_quote else None
        max_skew = _max_skew_seconds(domestic_quote, overseas_quote, fx_quote)

        status = "error"
        if domestic_quote or overseas_quote or fx_quote:
            status = "partial"
        if domestic_quote and overseas_quote and fx_quote and spread is not None:
            status = "ok"

        if status == "ok":
            if any(
                value is not None and value > pair.thresholds.stale_seconds
                for value in (domestic_age, overseas_age, fx_age)
            ):
                status = "stale"
                errors.append("data_quality: one or more quotes are stale")
            if max_skew is not None and max_skew > pair.thresholds.max_skew_seconds:
                status = "stale"
                errors.append("data_quality: quote timestamps are too far apart")

        window = self.windows[pair.group_name]
        rolling_mean = rolling_std = zscore = delta_spread = None
        if spread is not None:
            rolling_mean, rolling_std, zscore, delta_spread = window.summary(spread)
            if status == "ok":
                window.append(spread)

        snapshot = SpreadSnapshot(
            ts=utc_now(),
            group_name=pair.group_name,
            domestic_symbol=pair.domestic_symbol,
            overseas_symbol=pair.overseas_symbol,
            fx_source=self.config.app.fx_source,
            fx_rate=fx_quote.rate if fx_quote else None,
            formula=pair.formula,
            formula_version=pair.formula_version,
            tax_mode=pair.tax_mode,
            target_unit=pair.target_unit,
            status=status,
            errors=errors,
            domestic_last_raw=domestic_quote.last if domestic_quote else None,
            domestic_bid_raw=domestic_quote.bid if domestic_quote else None,
            domestic_ask_raw=domestic_quote.ask if domestic_quote else None,
            overseas_last=overseas_quote.last if overseas_quote else None,
            overseas_bid=overseas_quote.bid if overseas_quote else None,
            overseas_ask=overseas_quote.ask if overseas_quote else None,
            normalized_last=normalized_quote.last,
            normalized_bid=normalized_quote.bid,
            normalized_ask=normalized_quote.ask,
            spread=spread,
            spread_pct=spread_pct,
            rolling_mean=rolling_mean,
            rolling_std=rolling_std,
            zscore=zscore,
            delta_spread=delta_spread,
            executable_buy_domestic_sell_overseas=exec_buy_domestic,
            executable_buy_overseas_sell_domestic=exec_buy_overseas,
            domestic_age_sec=domestic_age,
            overseas_age_sec=overseas_age,
            fx_age_sec=fx_age,
            max_skew_sec=max_skew,
        )
        self.repository.insert_snapshot(snapshot)
        self.latest_snapshots[pair.group_name] = snapshot

        alerts = self._evaluate_alerts(pair, snapshot)
        for alert in alerts:
            self.repository.insert_alert(alert)
        await self._dispatch_alerts(alerts)

        return snapshot

    def _evaluate_alerts(self, pair: PairConfig, snapshot: SpreadSnapshot) -> list[AlertEvent]:
        alerts: list[AlertEvent] = []
        now = snapshot.ts

        if snapshot.status in {"partial", "stale", "error"}:
            alerts.append(
                self._make_alert(
                    now,
                    pair.group_name,
                    "data_quality",
                    "critical" if snapshot.status == "error" else "warning",
                    f"{pair.group_name} data status is {snapshot.status}",
                    {
                        "errors": snapshot.errors,
                        "domestic_age_sec": snapshot.domestic_age_sec,
                        "overseas_age_sec": snapshot.overseas_age_sec,
                        "fx_age_sec": snapshot.fx_age_sec,
                        "max_skew_sec": snapshot.max_skew_sec,
                    },
                )
            )

        if snapshot.spread_pct is not None and abs(snapshot.spread_pct) >= pair.thresholds.spread_pct_abs:
            alerts.append(
                self._make_alert(
                    now,
                    pair.group_name,
                    "spread_pct",
                    "warning",
                    f"{pair.group_name} spread_pct reached {snapshot.spread_pct:.2%}",
                    {"spread_pct": snapshot.spread_pct, "spread": snapshot.spread},
                )
            )

        if snapshot.zscore is not None and abs(snapshot.zscore) >= pair.thresholds.zscore_abs:
            alerts.append(
                self._make_alert(
                    now,
                    pair.group_name,
                    "zscore",
                    "warning",
                    f"{pair.group_name} zscore reached {snapshot.zscore:.2f}",
                    {"zscore": snapshot.zscore, "spread": snapshot.spread},
                )
            )

        if snapshot.fx_rate is None:
            alerts.append(
                self._make_alert(
                    now,
                    pair.group_name,
                    "fx",
                    "critical",
                    f"{pair.group_name} FX rate is unavailable",
                    {},
                )
            )

        return [alert for alert in alerts if alert is not None]

    def _make_alert(
        self,
        ts: datetime,
        group_name: str,
        category: str,
        severity: str,
        message: str,
        metadata: dict,
    ) -> AlertEvent | None:
        key = (group_name, category)
        previous = self._cooldowns.get(key)
        cooldown = next(
            (
                pair.thresholds.alert_cooldown_seconds
                for pair in self.config.pairs
                if pair.group_name == group_name
            ),
            300,
        )
        if previous and (ts - previous).total_seconds() < cooldown:
            return None

        self._cooldowns[key] = ts
        return AlertEvent(
            ts=ts,
            group_name=group_name,
            category=category,  # type: ignore[arg-type]
            severity=severity,  # type: ignore[arg-type]
            message=message,
            metadata=metadata,
        )

    async def _dispatch_alerts(self, alerts: list[AlertEvent]) -> None:
        if not alerts or not self.notifiers:
            return

        deliveries = await asyncio.gather(
            *(self._deliver_alert(alert) for alert in alerts),
            return_exceptions=False,
        )
        for delivery_batch in deliveries:
            for delivery in delivery_batch:
                self.repository.insert_notification_delivery(delivery)

    async def _deliver_alert(self, alert: AlertEvent) -> list[NotificationDelivery]:
        deliveries: list[NotificationDelivery] = []
        for notifier in self.notifiers:
            if not notifier.should_send(alert.severity):
                continue
            try:
                result = await asyncio.to_thread(notifier.send, alert)
                deliveries.append(
                    NotificationDelivery(
                        ts=alert.ts,
                        notifier_name=result.notifier_name,
                        group_name=alert.group_name,
                        category=alert.category,
                        severity=alert.severity,
                        success=result.success,
                        response_message=result.response_message,
                        payload=result.payload,
                    )
                )
            except Exception as exc:
                deliveries.append(
                    NotificationDelivery(
                        ts=alert.ts,
                        notifier_name=getattr(notifier, "config", None).name if getattr(notifier, "config", None) else "unknown",
                        group_name=alert.group_name,
                        category=alert.category,
                        severity=alert.severity,
                        success=False,
                        response_message=str(exc),
                        payload={
                            "group_name": alert.group_name,
                            "category": alert.category,
                            "severity": alert.severity,
                            "message": alert.message,
                        },
                    )
                )
                LOGGER.warning("Notifier delivery failed: %s", exc)
        return deliveries


def _age_seconds(timestamp: datetime) -> float:
    return max((utc_now() - timestamp.astimezone(UTC)).total_seconds(), 0.0)


def _max_skew_seconds(domestic: MarketQuote | None, overseas: MarketQuote | None, fx: FXQuote | None) -> float | None:
    timestamps = [
        item.ts.astimezone(UTC)
        for item in (domestic, overseas, fx)
        if item is not None
    ]
    if len(timestamps) < 2:
        return None
    seconds = [ts.timestamp() for ts in timestamps]
    return max(seconds) - min(seconds)


class MonitorRuntime:
    def __init__(self, service: MonitorService) -> None:
        self.service = service
        self.task: asyncio.Task | None = None

    async def start(self) -> None:
        if self.task is None:
            self.task = asyncio.create_task(self.service.run_forever())

    async def stop(self) -> None:
        await self.service.shutdown()
        if self.task is not None:
            with suppress(asyncio.CancelledError):
                await self.task
            self.task = None
