from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


TaxMode = Literal["gross", "net"]
DomesticUnit = Literal["CNY_PER_GRAM", "CNY_PER_KG", "CNY_PER_TON", "CNY_PER_BARREL"]
TargetUnit = Literal["USD_PER_OUNCE", "USD_PER_POUND", "USD_PER_BARREL"]
AlertSeverity = Literal["info", "warning", "critical"]


class ThresholdConfig(BaseModel):
    spread_pct_abs: float = 0.02
    zscore_abs: float = 2.5
    stale_seconds: int = 120
    max_skew_seconds: int = 120
    alert_cooldown_seconds: int = 300


class PairConfig(BaseModel):
    group_name: str
    domestic_source: str
    domestic_symbol: str
    domestic_label: str
    overseas_source: str
    overseas_symbol: str
    overseas_label: str
    formula: Literal["gold", "silver", "copper", "crude_oil"]
    formula_version: str = "2026-03-13-v1"
    domestic_unit: DomesticUnit
    target_unit: TargetUnit
    tax_mode: TaxMode = "gross"
    enabled: bool = True
    thresholds: ThresholdConfig = Field(default_factory=ThresholdConfig)


class SourceConfig(BaseModel):
    kind: Literal["sina_futures", "okx_swap", "frankfurter_fx", "mock_quote", "mock_fx"]
    base_url: str
    headers: dict[str, str] = Field(default_factory=dict)
    fallback_rate: float | None = None


class AppConfig(BaseModel):
    name: str = "Cross Market Spread Monitor"
    timezone: str = "Asia/Hong_Kong"
    poll_interval_sec: int = 10
    history_limit: int = 1000
    rolling_window_size: int = 120
    http_timeout_sec: int = 8
    sqlite_path: str = "data/monitor.db"
    fx_source: str = "frankfurter"
    bind_host: str = "127.0.0.1"
    bind_port: int = 8000
    export_dir: str = "exports"


class NotifierConfig(BaseModel):
    name: str
    kind: Literal["console", "webhook"]
    enabled: bool = True
    min_severity: AlertSeverity = "warning"
    url: str | None = None
    timeout_sec: int = 8
    headers: dict[str, str] = Field(default_factory=dict)


class MonitorConfig(BaseModel):
    app: AppConfig
    sources: dict[str, SourceConfig]
    pairs: list[PairConfig]
    notifiers: list[NotifierConfig] = Field(default_factory=list)


class MarketQuote(BaseModel):
    source_name: str
    symbol: str
    label: str
    ts: datetime
    last: float | None = None
    bid: float | None = None
    ask: float | None = None
    raw_payload: str | None = None


class FXQuote(BaseModel):
    source_name: str
    pair: str
    ts: datetime
    rate: float
    raw_payload: str | None = None


class AlertEvent(BaseModel):
    ts: datetime
    group_name: str
    category: Literal["spread_pct", "zscore", "data_quality", "fx"]
    severity: AlertSeverity
    message: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class NotificationDelivery(BaseModel):
    ts: datetime
    notifier_name: str
    group_name: str
    category: str
    severity: AlertSeverity
    success: bool
    response_message: str
    payload: dict[str, Any] = Field(default_factory=dict)


class SpreadSnapshot(BaseModel):
    ts: datetime
    group_name: str
    domestic_symbol: str
    overseas_symbol: str
    fx_source: str
    fx_rate: float | None = None
    formula: str
    formula_version: str
    tax_mode: TaxMode
    target_unit: TargetUnit
    status: Literal["ok", "partial", "stale", "error"]
    errors: list[str] = Field(default_factory=list)

    domestic_last_raw: float | None = None
    domestic_bid_raw: float | None = None
    domestic_ask_raw: float | None = None
    overseas_last: float | None = None
    overseas_bid: float | None = None
    overseas_ask: float | None = None

    normalized_last: float | None = None
    normalized_bid: float | None = None
    normalized_ask: float | None = None
    spread: float | None = None
    spread_pct: float | None = None
    rolling_mean: float | None = None
    rolling_std: float | None = None
    zscore: float | None = None
    delta_spread: float | None = None

    executable_buy_domestic_sell_overseas: float | None = None
    executable_buy_overseas_sell_domestic: float | None = None

    domestic_age_sec: float | None = None
    overseas_age_sec: float | None = None
    fx_age_sec: float | None = None
    max_skew_sec: float | None = None


class RuntimeHealth(BaseModel):
    started_at: datetime
    last_poll_started_at: datetime | None = None
    last_poll_finished_at: datetime | None = None
    poll_interval_sec: int
    rolling_window_size: int
    history_limit: int
    is_polling: bool
    total_cycles: int
    latest_fx_rate: float | None = None


class ReplayHighlight(BaseModel):
    ts: datetime
    metric: Literal["spread_pct", "zscore", "spread_abs"]
    score: float
    spread: float | None = None
    spread_pct: float | None = None
    zscore: float | None = None
    status: str


class ReplaySignalEvent(BaseModel):
    ts: datetime
    group_name: str
    trigger: Literal["spread_pct", "zscore"]
    value: float
    threshold: float
    direction: Literal["positive", "negative"]


class ReplayReport(BaseModel):
    group_name: str
    sample_count: int
    ok_count: int = 0
    partial_count: int = 0
    stale_count: int = 0
    error_count: int = 0
    start_ts: datetime | None = None
    end_ts: datetime | None = None
    latest_spread: float | None = None
    latest_spread_pct: float | None = None
    latest_zscore: float | None = None
    spread_mean: float | None = None
    spread_std: float | None = None
    spread_min: float | None = None
    spread_max: float | None = None
    spread_pct_mean: float | None = None
    spread_pct_min: float | None = None
    spread_pct_max: float | None = None
    max_abs_zscore: float | None = None
    spread_pct_breach_count: int = 0
    zscore_breach_count: int = 0
    convergence_count: int = 0
    divergence_count: int = 0
    flat_count: int = 0
    convergence_ratio: float | None = None
    divergence_ratio: float | None = None
    top_highlights: list[ReplayHighlight] = Field(default_factory=list)
    signal_entries: list[ReplaySignalEvent] = Field(default_factory=list)
