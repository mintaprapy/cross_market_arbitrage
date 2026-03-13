from __future__ import annotations

import csv
import json
import sqlite3
import threading
from pathlib import Path

from cross_market_monitor.domain.models import AlertEvent, FXQuote, MarketQuote, NotificationDelivery, SpreadSnapshot


class SQLiteRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._lock, self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS raw_quotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    leg_type TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    label TEXT NOT NULL,
                    last_px REAL,
                    bid_px REAL,
                    ask_px REAL,
                    raw_payload TEXT
                );

                CREATE TABLE IF NOT EXISTS fx_rates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    rate REAL NOT NULL,
                    raw_payload TEXT
                );

                CREATE TABLE IF NOT EXISTS spread_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    domestic_symbol TEXT NOT NULL,
                    overseas_symbol TEXT NOT NULL,
                    fx_source TEXT NOT NULL,
                    fx_rate REAL,
                    formula TEXT NOT NULL,
                    formula_version TEXT NOT NULL,
                    tax_mode TEXT NOT NULL,
                    target_unit TEXT NOT NULL,
                    status TEXT NOT NULL,
                    errors TEXT NOT NULL,
                    domestic_last_raw REAL,
                    domestic_bid_raw REAL,
                    domestic_ask_raw REAL,
                    overseas_last REAL,
                    overseas_bid REAL,
                    overseas_ask REAL,
                    normalized_last REAL,
                    normalized_bid REAL,
                    normalized_ask REAL,
                    spread REAL,
                    spread_pct REAL,
                    rolling_mean REAL,
                    rolling_std REAL,
                    zscore REAL,
                    delta_spread REAL,
                    executable_buy_domestic_sell_overseas REAL,
                    executable_buy_overseas_sell_domestic REAL,
                    domestic_age_sec REAL,
                    overseas_age_sec REAL,
                    fx_age_sec REAL,
                    max_skew_sec REAL
                );

                CREATE TABLE IF NOT EXISTS alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    message TEXT NOT NULL,
                    metadata TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS notification_deliveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    notifier_name TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    response_message TEXT NOT NULL,
                    payload TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_raw_quotes_group_ts ON raw_quotes(group_name, ts);
                CREATE INDEX IF NOT EXISTS idx_spread_snapshots_group_ts ON spread_snapshots(group_name, ts);
                CREATE INDEX IF NOT EXISTS idx_alert_events_group_ts ON alert_events(group_name, ts);
                CREATE INDEX IF NOT EXISTS idx_notification_deliveries_group_ts ON notification_deliveries(group_name, ts);
                """
            )

    def insert_raw_quote(self, group_name: str, leg_type: str, quote: MarketQuote) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO raw_quotes (
                    ts, group_name, leg_type, source_name, symbol, label, last_px, bid_px, ask_px, raw_payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    quote.ts.isoformat(),
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

    def insert_fx_rate(self, quote: FXQuote) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO fx_rates (ts, source_name, pair, rate, raw_payload)
                VALUES (?, ?, ?, ?, ?)
                """,
                (quote.ts.isoformat(), quote.source_name, quote.pair, quote.rate, quote.raw_payload),
            )

    def insert_snapshot(self, snapshot: SpreadSnapshot) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO spread_snapshots (
                    ts, group_name, domestic_symbol, overseas_symbol, fx_source, fx_rate, formula,
                    formula_version, tax_mode, target_unit, status, errors, domestic_last_raw,
                    domestic_bid_raw, domestic_ask_raw, overseas_last, overseas_bid, overseas_ask,
                    normalized_last, normalized_bid, normalized_ask, spread, spread_pct, rolling_mean,
                    rolling_std, zscore, delta_spread, executable_buy_domestic_sell_overseas,
                    executable_buy_overseas_sell_domestic, domestic_age_sec, overseas_age_sec,
                    fx_age_sec, max_skew_sec
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.ts.isoformat(),
                    snapshot.group_name,
                    snapshot.domestic_symbol,
                    snapshot.overseas_symbol,
                    snapshot.fx_source,
                    snapshot.fx_rate,
                    snapshot.formula,
                    snapshot.formula_version,
                    snapshot.tax_mode,
                    snapshot.target_unit,
                    snapshot.status,
                    json.dumps(snapshot.errors, ensure_ascii=False),
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

    def insert_alert(self, alert: AlertEvent) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO alert_events (ts, group_name, category, severity, message, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    alert.ts.isoformat(),
                    alert.group_name,
                    alert.category,
                    alert.severity,
                    alert.message,
                    json.dumps(alert.metadata, ensure_ascii=False),
                ),
            )

    def insert_notification_delivery(self, delivery: NotificationDelivery) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO notification_deliveries (
                    ts, notifier_name, group_name, category, severity, success, response_message, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    delivery.ts.isoformat(),
                    delivery.notifier_name,
                    delivery.group_name,
                    delivery.category,
                    delivery.severity,
                    int(delivery.success),
                    delivery.response_message,
                    json.dumps(delivery.payload, ensure_ascii=False),
                ),
            )

    def load_recent_spreads(self, group_name: str, limit: int) -> list[float]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT spread
                FROM spread_snapshots
                WHERE group_name = ? AND spread IS NOT NULL
                ORDER BY ts DESC
                LIMIT ?
                """,
                (group_name, limit),
            ).fetchall()
        return [row["spread"] for row in reversed(rows) if row["spread"] is not None]

    def fetch_history(self, group_name: str, limit: int) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT ts, spread, spread_pct, zscore, normalized_last, overseas_last, status
                FROM spread_snapshots
                WHERE group_name = ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (group_name, limit),
            ).fetchall()
        return [dict(row) for row in reversed(rows)]

    def fetch_alerts(self, limit: int = 100) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT ts, group_name, category, severity, message, metadata
                FROM alert_events
                ORDER BY ts DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        results = [dict(row) for row in rows]
        for row in results:
            row["metadata"] = json.loads(row["metadata"])
        return results

    def fetch_notification_deliveries(self, limit: int = 100) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT ts, notifier_name, group_name, category, severity, success, response_message, payload
                FROM notification_deliveries
                ORDER BY ts DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        results = [dict(row) for row in rows]
        for row in results:
            row["payload"] = json.loads(row["payload"])
            row["success"] = bool(row["success"])
        return results

    def fetch_snapshots(
        self,
        group_name: str | None = None,
        limit: int = 300,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> list[dict]:
        where, params = self._build_filters(group_name=group_name, start_ts=start_ts, end_ts=end_ts)
        query = f"""
            SELECT *
            FROM spread_snapshots
            {where}
            ORDER BY ts DESC
            LIMIT ?
        """
        params.append(limit)
        with self._lock, self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        results = [dict(row) for row in reversed(rows)]
        for row in results:
            row["errors"] = json.loads(row["errors"])
        return results

    def export_dataset_to_csv(
        self,
        dataset: str,
        output_path: str,
        *,
        group_name: str | None = None,
        limit: int = 5000,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> int:
        rows = self._fetch_dataset_rows(
            dataset,
            group_name=group_name,
            limit=limit,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("w", newline="", encoding="utf-8") as handle:
            if not rows:
                handle.write("")
                return 0
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        return len(rows)

    def _fetch_dataset_rows(
        self,
        dataset: str,
        *,
        group_name: str | None = None,
        limit: int = 5000,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> list[dict]:
        table = {
            "snapshots": "spread_snapshots",
            "alerts": "alert_events",
            "raw_quotes": "raw_quotes",
            "fx_rates": "fx_rates",
            "notification_deliveries": "notification_deliveries",
        }.get(dataset)
        if table is None:
            raise ValueError(f"Unsupported dataset: {dataset}")

        where, params = self._build_filters(
            group_name=group_name if table != "fx_rates" else None,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        query = f"SELECT * FROM {table} {where} ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self._lock, self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [dict(row) for row in reversed(rows)]

    def _build_filters(
        self,
        *,
        group_name: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> tuple[str, list]:
        conditions: list[str] = []
        params: list = []
        if group_name:
            conditions.append("group_name = ?")
            params.append(group_name)
        if start_ts:
            conditions.append("ts >= ?")
            params.append(start_ts)
        if end_ts:
            conditions.append("ts <= ?")
            params.append(end_ts)
        if not conditions:
            return "", params
        return "WHERE " + " AND ".join(conditions), params
