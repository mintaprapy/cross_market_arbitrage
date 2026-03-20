from __future__ import annotations

import json

from cross_market_monitor.domain.models import AlertEvent, FXQuote, MarketQuote, NotificationDelivery, SpreadSnapshot


class SQLiteWriterMixin:
    def insert_raw_quote(
        self,
        group_name: str,
        leg_type: str,
        quote: MarketQuote,
        *,
        timezone_name: str = "UTC",
    ) -> None:
        ts, ts_utc, ts_local = self._timestamp_fields(quote.ts, timezone_name)
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO raw_quotes (
                    ts, ts_utc, ts_local, group_name, leg_type, source_name, symbol, label, last_px, bid_px, ask_px, raw_payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    ts_utc,
                    ts_local,
                    group_name,
                    leg_type,
                    quote.source_name,
                    quote.symbol,
                    quote.label,
                    quote.last,
                    quote.bid,
                    quote.ask,
                    quote.raw_payload,
                ),
            )

    def insert_raw_quote_if_missing(
        self,
        group_name: str,
        leg_type: str,
        quote: MarketQuote,
        *,
        timezone_name: str = "UTC",
    ) -> bool:
        ts, ts_utc, ts_local = self._timestamp_fields(quote.ts, timezone_name)
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO raw_quotes (
                    ts, ts_utc, ts_local, group_name, leg_type, source_name, symbol, label, last_px, bid_px, ask_px, raw_payload
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM raw_quotes
                    WHERE group_name = ?
                      AND leg_type = ?
                      AND source_name = ?
                      AND symbol = ?
                      AND ts = ?
                )
                """,
                (
                    ts,
                    ts_utc,
                    ts_local,
                    group_name,
                    leg_type,
                    quote.source_name,
                    quote.symbol,
                    quote.label,
                    quote.last,
                    quote.bid,
                    quote.ask,
                    quote.raw_payload,
                    group_name,
                    leg_type,
                    quote.source_name,
                    quote.symbol,
                    ts,
                ),
            )
        return cursor.rowcount > 0

    def insert_fx_rate(self, quote: FXQuote, *, timezone_name: str = "UTC") -> None:
        ts, ts_utc, ts_local = self._timestamp_fields(quote.ts, timezone_name)
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO fx_rates (ts, ts_utc, ts_local, source_name, pair, rate, raw_payload)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, ts_utc, ts_local, quote.source_name, quote.pair, quote.rate, quote.raw_payload),
            )

    def insert_fx_rate_if_missing(self, quote: FXQuote, *, timezone_name: str = "UTC") -> bool:
        ts, ts_utc, ts_local = self._timestamp_fields(quote.ts, timezone_name)
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO fx_rates (ts, ts_utc, ts_local, source_name, pair, rate, raw_payload)
                SELECT ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM fx_rates
                    WHERE source_name = ?
                      AND pair = ?
                      AND ts = ?
                )
                """,
                (
                    ts,
                    ts_utc,
                    ts_local,
                    quote.source_name,
                    quote.pair,
                    quote.rate,
                    quote.raw_payload,
                    quote.source_name,
                    quote.pair,
                    ts,
                ),
            )
        return cursor.rowcount > 0

    def insert_normalized_domestic_quote(
        self,
        group_name: str,
        quote: MarketQuote,
        *,
        fx_source: str,
        fx_rate: float,
        formula: str,
        formula_version: str,
        tax_mode: str,
        target_unit: str,
        normalized_last: float | None,
        normalized_bid: float | None,
        normalized_ask: float | None,
        timezone_name: str = "UTC",
    ) -> None:
        ts, ts_utc, ts_local = self._timestamp_fields(quote.ts, timezone_name)
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO normalized_domestic_quotes (
                    ts, ts_utc, ts_local, group_name, source_name, symbol, label,
                    fx_source, fx_rate, formula, formula_version, tax_mode, target_unit,
                    raw_last, raw_bid, raw_ask, normalized_last, normalized_bid, normalized_ask
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    ts_utc,
                    ts_local,
                    group_name,
                    quote.source_name,
                    quote.symbol,
                    quote.label,
                    fx_source,
                    fx_rate,
                    formula,
                    formula_version,
                    tax_mode,
                    target_unit,
                    quote.last,
                    quote.bid,
                    quote.ask,
                    normalized_last,
                    normalized_bid,
                    normalized_ask,
                ),
            )

    def insert_normalized_domestic_quote_if_missing(
        self,
        group_name: str,
        quote: MarketQuote,
        *,
        fx_source: str,
        fx_rate: float,
        formula: str,
        formula_version: str,
        tax_mode: str,
        target_unit: str,
        normalized_last: float | None,
        normalized_bid: float | None,
        normalized_ask: float | None,
        timezone_name: str = "UTC",
    ) -> bool:
        ts, ts_utc, ts_local = self._timestamp_fields(quote.ts, timezone_name)
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO normalized_domestic_quotes (
                    ts, ts_utc, ts_local, group_name, source_name, symbol, label,
                    fx_source, fx_rate, formula, formula_version, tax_mode, target_unit,
                    raw_last, raw_bid, raw_ask, normalized_last, normalized_bid, normalized_ask
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM normalized_domestic_quotes
                    WHERE group_name = ?
                      AND source_name = ?
                      AND symbol = ?
                      AND ts = ?
                )
                """,
                (
                    ts,
                    ts_utc,
                    ts_local,
                    group_name,
                    quote.source_name,
                    quote.symbol,
                    quote.label,
                    fx_source,
                    fx_rate,
                    formula,
                    formula_version,
                    tax_mode,
                    target_unit,
                    quote.last,
                    quote.bid,
                    quote.ask,
                    normalized_last,
                    normalized_bid,
                    normalized_ask,
                    group_name,
                    quote.source_name,
                    quote.symbol,
                    ts,
                ),
            )
        return cursor.rowcount > 0

    def insert_snapshot(self, snapshot: SpreadSnapshot, *, timezone_name: str = "UTC") -> None:
        ts, ts_utc, ts_local = self._timestamp_fields(snapshot.ts, timezone_name)
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO spread_snapshots (
                    ts, ts_utc, ts_local, group_name, domestic_symbol, overseas_symbol, domestic_source,
                    overseas_source, domestic_label, overseas_label, fx_source, fx_rate, fx_jump_pct,
                    formula, formula_version, tax_mode, target_unit, status, signal_state, pause_reason,
                    errors, route_detail, domestic_last_raw, domestic_bid_raw, domestic_ask_raw, overseas_last,
                    overseas_bid, overseas_ask, normalized_last, normalized_bid, normalized_ask, spread,
                    spread_pct, rolling_mean, rolling_std, zscore, delta_spread,
                    executable_buy_domestic_sell_overseas, executable_buy_overseas_sell_domestic,
                    domestic_age_sec, overseas_age_sec, fx_age_sec, max_skew_sec
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    ts_utc,
                    ts_local,
                    snapshot.group_name,
                    snapshot.domestic_symbol,
                    snapshot.overseas_symbol,
                    snapshot.domestic_source,
                    snapshot.overseas_source,
                    snapshot.domestic_label,
                    snapshot.overseas_label,
                    snapshot.fx_source,
                    snapshot.fx_rate,
                    snapshot.fx_jump_pct,
                    snapshot.formula,
                    snapshot.formula_version,
                    snapshot.tax_mode,
                    snapshot.target_unit,
                    snapshot.status,
                    snapshot.signal_state,
                    snapshot.pause_reason,
                    json.dumps(snapshot.errors, ensure_ascii=False),
                    json.dumps(snapshot.route_detail, ensure_ascii=False),
                    snapshot.domestic_last_raw,
                    snapshot.domestic_bid_raw,
                    snapshot.domestic_ask_raw,
                    snapshot.overseas_last,
                    snapshot.overseas_bid,
                    snapshot.overseas_ask,
                    snapshot.normalized_last,
                    snapshot.normalized_bid,
                    snapshot.normalized_ask,
                    snapshot.spread,
                    snapshot.spread_pct,
                    snapshot.rolling_mean,
                    snapshot.rolling_std,
                    snapshot.zscore,
                    snapshot.delta_spread,
                    snapshot.executable_buy_domestic_sell_overseas,
                    snapshot.executable_buy_overseas_sell_domestic,
                    snapshot.domestic_age_sec,
                    snapshot.overseas_age_sec,
                    snapshot.fx_age_sec,
                    snapshot.max_skew_sec,
                ),
            )
            snapshot_id = int(cursor.lastrowid)
            connection.execute(
                """
                INSERT INTO latest_snapshots (group_name, snapshot_id, ts)
                VALUES (?, ?, ?)
                ON CONFLICT(group_name) DO UPDATE SET
                    snapshot_id = excluded.snapshot_id,
                    ts = excluded.ts
                """,
                (
                    snapshot.group_name,
                    snapshot_id,
                    ts,
                ),
            )

    def insert_alert(self, alert: AlertEvent, *, timezone_name: str = "UTC") -> None:
        ts, ts_utc, ts_local = self._timestamp_fields(alert.ts, timezone_name)
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO alert_events (ts, ts_utc, ts_local, group_name, category, severity, message, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    ts_utc,
                    ts_local,
                    alert.group_name,
                    alert.category,
                    alert.severity,
                    alert.message,
                    json.dumps(alert.metadata, ensure_ascii=False),
                ),
            )

    def insert_notification_delivery(
        self,
        delivery: NotificationDelivery,
        *,
        timezone_name: str = "UTC",
    ) -> None:
        ts, ts_utc, ts_local = self._timestamp_fields(delivery.ts, timezone_name)
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO notification_deliveries (
                    ts, ts_utc, ts_local, notifier_name, group_name, category, severity, success, response_message, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    ts_utc,
                    ts_local,
                    delivery.notifier_name,
                    delivery.group_name,
                    delivery.category,
                    delivery.severity,
                    int(delivery.success),
                    delivery.response_message,
                    json.dumps(delivery.payload, ensure_ascii=False),
                ),
            )
