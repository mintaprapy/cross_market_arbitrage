from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from cross_market_monitor.domain.models import FXQuote, MarketQuote, SpreadSnapshot


class SQLiteRepositoryBase:
    _INITIALIZE_RETRIES = 5
    _INITIALIZE_RETRY_DELAY_SEC = 0.2

    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout=30000")
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        last_error: sqlite3.OperationalError | None = None
        for attempt in range(self._INITIALIZE_RETRIES):
            try:
                with self._lock, self._connect() as connection:
                    connection.executescript(
                        """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS raw_quotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    ts_utc TEXT NOT NULL,
                    ts_local TEXT NOT NULL,
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
                    ts_utc TEXT NOT NULL,
                    ts_local TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    pair TEXT NOT NULL,
                    rate REAL NOT NULL,
                    raw_payload TEXT
                );

                CREATE TABLE IF NOT EXISTS normalized_domestic_quotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    ts_utc TEXT NOT NULL,
                    ts_local TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    label TEXT NOT NULL,
                    fx_source TEXT NOT NULL,
                    fx_rate REAL NOT NULL,
                    formula TEXT NOT NULL,
                    formula_version TEXT NOT NULL,
                    tax_mode TEXT NOT NULL,
                    target_unit TEXT NOT NULL,
                    raw_last REAL,
                    raw_bid REAL,
                    raw_ask REAL,
                    normalized_last REAL,
                    normalized_bid REAL,
                    normalized_ask REAL
                );

                CREATE TABLE IF NOT EXISTS spread_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    ts_utc TEXT NOT NULL,
                    ts_local TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    domestic_symbol TEXT NOT NULL,
                    overseas_symbol TEXT NOT NULL,
                    domestic_source TEXT,
                    overseas_source TEXT,
                    domestic_label TEXT,
                    overseas_label TEXT,
                    fx_source TEXT NOT NULL,
                    fx_rate REAL,
                    fx_jump_pct REAL,
                    formula TEXT NOT NULL,
                    formula_version TEXT NOT NULL,
                    tax_mode TEXT NOT NULL,
                    target_unit TEXT NOT NULL,
                    status TEXT NOT NULL,
                    signal_state TEXT NOT NULL,
                    pause_reason TEXT,
                    errors TEXT NOT NULL,
                    route_detail TEXT NOT NULL,
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

                CREATE TABLE IF NOT EXISTS latest_snapshots (
                    group_name TEXT PRIMARY KEY,
                    snapshot_id INTEGER NOT NULL,
                    ts TEXT NOT NULL,
                    FOREIGN KEY(snapshot_id) REFERENCES spread_snapshots(id)
                );

                CREATE TABLE IF NOT EXISTS alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    ts_utc TEXT NOT NULL,
                    ts_local TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    message TEXT NOT NULL,
                    metadata TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS notification_deliveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    ts_utc TEXT NOT NULL,
                    ts_local TEXT NOT NULL,
                    notifier_name TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    response_message TEXT NOT NULL,
                    payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS route_preferences (
                    group_name TEXT NOT NULL,
                    leg_type TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (group_name, leg_type)
                );

                CREATE TABLE IF NOT EXISTS runtime_state (
                    state_name TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS source_health_state (
                    source_name TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    success_count INTEGER NOT NULL,
                    failure_count INTEGER NOT NULL,
                    last_success_at TEXT,
                    last_failure_at TEXT,
                    last_error TEXT,
                    last_symbol TEXT,
                    last_latency_ms REAL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS job_runs (
                    job_name TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    details TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_raw_quotes_group_ts ON raw_quotes(group_name, ts);
                CREATE INDEX IF NOT EXISTS idx_raw_quotes_lookup
                    ON raw_quotes(group_name, leg_type, source_name, symbol, ts);
                CREATE INDEX IF NOT EXISTS idx_fx_rates_source_ts ON fx_rates(source_name, ts);
                CREATE INDEX IF NOT EXISTS idx_normalized_domestic_quotes_group_symbol_ts
                    ON normalized_domestic_quotes(group_name, symbol, ts);
                CREATE INDEX IF NOT EXISTS idx_spread_snapshots_group_ts ON spread_snapshots(group_name, ts);
                CREATE INDEX IF NOT EXISTS idx_latest_snapshots_ts ON latest_snapshots(ts);
                CREATE INDEX IF NOT EXISTS idx_alert_events_group_ts ON alert_events(group_name, ts);
                CREATE INDEX IF NOT EXISTS idx_notification_deliveries_group_ts ON notification_deliveries(group_name, ts);
                CREATE INDEX IF NOT EXISTS idx_source_health_updated_at ON source_health_state(updated_at);
                CREATE INDEX IF NOT EXISTS idx_job_runs_updated_at ON job_runs(updated_at);
                """
                    )
                    self._ensure_columns(
                        connection,
                        "raw_quotes",
                        {
                            "ts_utc": "TEXT",
                            "ts_local": "TEXT",
                        },
                    )
                    self._ensure_columns(
                        connection,
                        "fx_rates",
                        {
                            "ts_utc": "TEXT",
                            "ts_local": "TEXT",
                        },
                    )
                    self._ensure_columns(
                        connection,
                        "spread_snapshots",
                        {
                            "ts_utc": "TEXT",
                            "ts_local": "TEXT",
                            "domestic_source": "TEXT",
                            "overseas_source": "TEXT",
                            "domestic_label": "TEXT",
                            "overseas_label": "TEXT",
                            "fx_jump_pct": "REAL",
                            "signal_state": "TEXT DEFAULT 'active'",
                            "pause_reason": "TEXT",
                            "route_detail": "TEXT DEFAULT '{}'",
                        },
                    )
                    self._ensure_columns(
                        connection,
                        "alert_events",
                        {
                            "ts_utc": "TEXT",
                            "ts_local": "TEXT",
                        },
                    )
                    self._ensure_columns(
                        connection,
                        "notification_deliveries",
                        {
                            "ts_utc": "TEXT",
                            "ts_local": "TEXT",
                        },
                    )
                    self._record_migration(connection, 1)
                return
            except sqlite3.OperationalError as exc:
                if not self._is_retryable_lock_error(exc):
                    raise
                last_error = exc
                if attempt == self._INITIALIZE_RETRIES - 1:
                    break
                time.sleep(self._INITIALIZE_RETRY_DELAY_SEC * (attempt + 1))
        if last_error is not None:
            raise last_error

    def _ensure_columns(self, connection: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
        existing = {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for column_name, column_type in columns.items():
            if column_name not in existing:
                connection.execute(
                    f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type}"
                )

    def _record_migration(self, connection: sqlite3.Connection, version: int) -> None:
        connection.execute(
            """
            INSERT OR IGNORE INTO schema_migrations (version, applied_at)
            VALUES (?, ?)
            """,
            (version, self._timestamp_fields_string()[0]),
        )

    @staticmethod
    def _is_retryable_lock_error(exc: sqlite3.OperationalError) -> bool:
        message = str(exc).lower()
        return "database is locked" in message or "database schema is locked" in message

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)

    def _row_to_fx_quote(self, row: sqlite3.Row | None) -> FXQuote | None:
        if row is None:
            return None
        return FXQuote(
            source_name=row["source_name"],
            pair=row["pair"],
            ts=self._parse_timestamp(row["ts"]),
            rate=row["rate"],
            raw_payload=row["raw_payload"],
        )

    def _row_to_market_quote(self, row: sqlite3.Row | None) -> MarketQuote | None:
        if row is None:
            return None
        return MarketQuote(
            source_name=row["source_name"],
            symbol=row["symbol"],
            label=row["label"],
            ts=self._parse_timestamp(row["ts"]),
            last=row["last_px"],
            bid=row["bid_px"],
            ask=row["ask_px"],
            raw_payload=row["raw_payload"],
        )

    def _timestamp_fields(self, ts: datetime, timezone_name: str) -> tuple[str, str, str]:
        ts_utc = ts.astimezone(ZoneInfo("UTC"))
        ts_local = ts.astimezone(ZoneInfo(timezone_name))
        iso_utc = ts_utc.isoformat()
        return iso_utc, iso_utc, ts_local.isoformat()

    def _timestamp_fields_string(self) -> tuple[str, str, str]:
        now = datetime.now(ZoneInfo("UTC"))
        iso = now.isoformat()
        return iso, iso, iso
