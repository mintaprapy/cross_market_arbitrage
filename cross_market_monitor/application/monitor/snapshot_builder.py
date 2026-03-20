from __future__ import annotations

from cross_market_monitor.application.common import (
    age_seconds,
    is_within_trading_sessions,
    latest_session_end_before,
    max_skew_seconds,
    utc_now,
)
from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.application.control.route_preference_service import RoutePreferenceService
from cross_market_monitor.application.monitor.alert_service import AlertService
from cross_market_monitor.application.monitor.fx_service import FXService
from cross_market_monitor.application.monitor.quote_router import QuoteRouter
from cross_market_monitor.domain.formulas import compute_spread, normalize_domestic_quote
from cross_market_monitor.domain.models import FXQuote, MarketQuote, PairConfig, SpreadSnapshot


class SnapshotBuilder:
    def __init__(
        self,
        context: ServiceContext,
        route_preferences: RoutePreferenceService,
        quote_router: QuoteRouter,
        fx_service: FXService,
        alert_service: AlertService,
    ) -> None:
        self.context = context
        self.route_preferences = route_preferences
        self.quote_router = quote_router
        self.fx_service = fx_service
        self.alert_service = alert_service

    async def build_snapshot(self, pair: PairConfig, fx_context) -> SpreadSnapshot:
        domestic_quote, domestic_quotes, domestic_errors, domestic_detail = await self.quote_router.fetch_leg_quote(
            pair.group_name,
            "domestic",
            self.route_preferences.domestic_candidates(pair),
        )
        overseas_quote, _, overseas_errors, overseas_detail = await self.quote_router.fetch_leg_quote(
            pair.group_name,
            "overseas",
            self.route_preferences.overseas_candidates(pair),
        )
        domestic_quote, domestic_quotes = self.freeze_domestic_quotes_if_closed(pair, domestic_quote, domestic_quotes)

        errors = [*domestic_errors, *overseas_errors]
        fx_quote, effective_fx_jump_pct = self.fx_service.effective_fx_for_domestic_quote(domestic_quote, fx_context)
        if fx_quote is None:
            errors.append("fx: unavailable")
        else:
            errors.extend(self.fx_quality_errors(fx_quote))
        errors.extend(self.quote_quality_errors("domestic", domestic_quote))
        errors.extend(self.quote_quality_errors("overseas", overseas_quote))

        normalized_quote = normalize_domestic_quote(
            pair,
            fx_quote.rate if fx_quote else None,
            domestic_quote.last if domestic_quote else None,
            domestic_quote.bid if domestic_quote else None,
            domestic_quote.ask if domestic_quote else None,
        )
        if fx_quote is not None:
            for quote in domestic_quotes:
                quote_fx = self.fx_service.effective_fx_for_quote(quote, fx_context)
                if quote_fx is None:
                    continue
                normalized_domestic = normalize_domestic_quote(
                    pair,
                    quote_fx.rate,
                    quote.last,
                    quote.bid,
                    quote.ask,
                )
                self.context.repository.insert_normalized_domestic_quote(
                    pair.group_name,
                    quote,
                    fx_source=quote_fx.source_name,
                    fx_rate=quote_fx.rate,
                    formula=pair.formula,
                    formula_version=pair.formula_version,
                    tax_mode=pair.tax_mode,
                    target_unit=pair.target_unit,
                    normalized_last=normalized_domestic.last,
                    normalized_bid=normalized_domestic.bid,
                    normalized_ask=normalized_domestic.ask,
                    timezone_name=self.context.config.app.timezone,
                )
        spread, spread_pct = compute_spread(
            normalized_quote.last,
            overseas_quote.last if overseas_quote else None,
        )

        domestic_age = age_seconds(domestic_quote.ts) if domestic_quote else None
        overseas_age = age_seconds(overseas_quote.ts) if overseas_quote else None
        fx_age = age_seconds(fx_quote.ts) if fx_quote else None
        max_skew = max_skew_seconds(domestic_quote, overseas_quote, None)

        quality_error = any(
            error.startswith("data_quality:") or error.startswith("fx: non-positive")
            for error in errors
        )

        status = "error"
        if domestic_quote or overseas_quote or fx_quote:
            status = "partial"
        if domestic_quote and overseas_quote and fx_quote and spread is not None:
            status = "ok"
        if quality_error:
            status = "error"

        signal_state = "active"
        pause_reason = None
        if status == "ok" and pair.thresholds.pause_on_fx_jump and effective_fx_jump_pct is not None:
            if abs(effective_fx_jump_pct) >= pair.thresholds.fx_jump_abs_pct:
                status = "paused"
                signal_state = "paused"
                pause_reason = (
                    f"FX jump {effective_fx_jump_pct:.2%} exceeded threshold {pair.thresholds.fx_jump_abs_pct:.2%}"
                )
                errors.append(f"fx: {pause_reason}")

        if status == "ok":
            fx_stale_seconds = self.fx_stale_seconds(pair)
            if any(
                value is not None and value > pair.thresholds.stale_seconds
                for value in (domestic_age, overseas_age)
            ) or (fx_age is not None and fx_age > fx_stale_seconds):
                status = "stale"
                errors.append("data_quality: one or more quotes are stale")
            if max_skew is not None and max_skew > pair.thresholds.max_skew_seconds:
                status = "stale"
                errors.append("data_quality: quote timestamps are too far apart")

        window = self.context.windows[pair.group_name]
        rolling_mean = rolling_std = zscore = delta_spread = None
        if spread is not None:
            rolling_mean, rolling_std, zscore, delta_spread = window.summary(spread)
            if status == "ok":
                window.append(spread)

        snapshot_ts = utc_now()
        snapshot = SpreadSnapshot(
            ts=snapshot_ts,
            ts_local=snapshot_ts.astimezone(self.context.local_tz),
            group_name=pair.group_name,
            domestic_symbol=domestic_quote.symbol if domestic_quote else pair.domestic_symbol,
            overseas_symbol=overseas_quote.symbol if overseas_quote else pair.overseas_symbol,
            domestic_source=domestic_quote.source_name if domestic_quote else None,
            overseas_source=overseas_quote.source_name if overseas_quote else None,
            domestic_label=domestic_quote.label if domestic_quote else pair.domestic_label,
            overseas_label=overseas_quote.label if overseas_quote else pair.overseas_label,
            fx_source=fx_quote.source_name if fx_quote else self.context.config.app.fx_source,
            fx_rate=fx_quote.rate if fx_quote else None,
            formula=pair.formula,
            formula_version=pair.formula_version,
            tax_mode=pair.tax_mode,
            target_unit=pair.target_unit,
            status=status,  # type: ignore[arg-type]
            errors=errors,
            signal_state=signal_state,  # type: ignore[arg-type]
            pause_reason=pause_reason,
            fx_jump_pct=effective_fx_jump_pct,
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
            executable_buy_domestic_sell_overseas=None,
            executable_buy_overseas_sell_domestic=None,
            domestic_age_sec=domestic_age,
            overseas_age_sec=overseas_age,
            fx_age_sec=fx_age,
            max_skew_sec=max_skew,
            route_detail={
                "domestic": domestic_detail,
                "overseas": overseas_detail,
                "preferred_domestic_symbol": self.context.preferred_domestic_symbols.get(pair.group_name),
                "preferred_overseas_symbol": self.context.preferred_overseas_symbols.get(pair.group_name),
                "fx_previous_rate": fx_context.previous_rate,
                "fx_jump_pct": effective_fx_jump_pct,
                "fx_live_rate": fx_context.quote.rate if fx_context.quote else None,
                "fx_live_ts": fx_context.quote.ts.isoformat() if fx_context.quote else None,
                "fx_live_source": fx_context.quote.source_name if fx_context.quote else None,
                "fx_is_live": fx_context.is_live,
                "fx_is_frozen": bool(fx_quote is not None and not fx_context.is_live),
                "effective_fx_ts": fx_quote.ts.isoformat() if fx_quote else None,
                "effective_fx_source": fx_quote.source_name if fx_quote else None,
            },
        )
        self.context.repository.insert_snapshot(snapshot, timezone_name=self.context.config.app.timezone)
        self.context.latest_snapshots[pair.group_name] = snapshot

        alerts = self.alert_service.evaluate_alerts(pair, snapshot)
        for alert in alerts:
            self.context.repository.insert_alert(alert, timezone_name=self.context.config.app.timezone)
        await self.alert_service.dispatch_alerts(alerts)
        return snapshot

    def freeze_domestic_quotes_if_closed(
        self,
        pair: PairConfig,
        domestic_quote: MarketQuote | None,
        domestic_quotes: list[MarketQuote],
    ) -> tuple[MarketQuote | None, list[MarketQuote]]:
        if domestic_quote is None or not pair.trading_sessions_local:
            return domestic_quote, domestic_quotes

        now_local = utc_now().astimezone(self.context.local_tz)
        if is_within_trading_sessions(
            now_local,
            pair.trading_sessions_local,
            non_trading_dates=self.context.config.app.domestic_non_trading_dates_local,
            weekends_closed=self.context.config.app.domestic_weekends_closed,
        ):
            return domestic_quote, domestic_quotes

        session_end_local = latest_session_end_before(
            now_local,
            pair.trading_sessions_local,
            non_trading_dates=self.context.config.app.domestic_non_trading_dates_local,
            weekends_closed=self.context.config.app.domestic_weekends_closed,
        )
        if session_end_local is None:
            return domestic_quote, domestic_quotes

        frozen_quote = self.context.repository.load_latest_raw_quote_before(
            pair.group_name,
            "domestic",
            domestic_quote.symbol,
            session_end_local,
        )
        if frozen_quote is None:
            return domestic_quote, domestic_quotes
        return frozen_quote, [frozen_quote]

    def fx_stale_seconds(self, pair: PairConfig) -> int:
        return max(
            pair.thresholds.stale_seconds,
            int(self.context.config.app.fx_poll_interval_sec) + int(self.context.config.app.poll_interval_sec),
        )

    @staticmethod
    def quote_quality_errors(leg_type: str, quote: MarketQuote | None) -> list[str]:
        if quote is None:
            return []

        values = {
            "last": quote.last,
            "bid": quote.bid,
            "ask": quote.ask,
        }
        positive_values = [value for value in values.values() if value is not None and value > 0]
        if not positive_values:
            return [f"data_quality: {leg_type} quote has no positive price fields"]

        invalid_fields = [
            field_name
            for field_name, value in values.items()
            if value is not None and value <= 0
        ]
        if invalid_fields:
            return [f"data_quality: {leg_type} quote has non-positive fields {', '.join(invalid_fields)}"]
        return []

    @staticmethod
    def fx_quality_errors(quote: FXQuote) -> list[str]:
        if quote.rate <= 0:
            return ["fx: non-positive rate"]
        return []
