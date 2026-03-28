from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


TaxMode = Literal["gross", "net"]
DomesticUnit = Literal["CNY_PER_GRAM", "CNY_PER_KG", "CNY_PER_TON", "CNY_PER_BARREL"]
TargetUnit = Literal["USD_PER_OUNCE", "USD_PER_POUND", "USD_PER_BARREL", "USD_PER_TON", "USD_PER_BUSHEL"]
AlertSeverity = Literal["info", "warning", "critical"]


class ThresholdConfig(BaseModel):
    spread_pct_abs: float | None = None
    zscore_abs: float | None = None
    spread_alert_above: float | None = None
    spread_alert_below: float | None = None
    spread_pct_alert_above: float | None = None
    spread_pct_alert_below: float | None = None
    zscore_alert_above: float | None = None
    zscore_alert_below: float | None = None
    stale_seconds: int = 120
    max_skew_seconds: int = 120
    alert_cooldown_seconds: int = 300
    data_quality_alert_delay_sec: int = 30
    fx_jump_abs_pct: float = 0.005
    pause_on_fx_jump: bool = True
    stale_alert_grace_sec: int = 0

    @field_validator(
        "spread_pct_abs",
        "zscore_abs",
        "spread_alert_above",
        "spread_alert_below",
        "spread_pct_alert_above",
        "spread_pct_alert_below",
        "zscore_alert_above",
        "zscore_alert_below",
        "fx_jump_abs_pct",
        mode="before",
    )
    @classmethod
    def parse_threshold_values(cls, value: object, info) -> float | None:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            parsed = float(value)
        else:
            text = str(value).strip()
            if not text:
                return None
            compact = "".join(text.split())
            for prefix in (">=", "<=", ">", "<"):
                if compact.startswith(prefix):
                    compact = compact[len(prefix):]
                    break
            is_percent = info.field_name in {
                "spread_pct_abs",
                "spread_pct_alert_above",
                "spread_pct_alert_below",
                "fx_jump_abs_pct",
            } or "%" in compact
            compact = compact.replace("%", "")
            parsed = float(compact)
            if is_percent:
                parsed /= 100
        return parsed


class CostModelConfig(BaseModel):
    domestic_fee_bps: float = 1.0
    overseas_fee_bps: float = 1.0
    domestic_slippage_bps: float = 1.0
    overseas_slippage_bps: float = 1.0
    funding_bps_per_day: float = 0.0
    holding_hours: float = 24.0


class QuoteRouteConfig(BaseModel):
    source: str
    symbol: str
    label: str
    enabled: bool = True


class PairConfig(BaseModel):
    group_name: str
    domestic_source: str
    domestic_symbol: str
    domestic_label: str
    domestic_history_source: str | None = None
    domestic_history_symbol: str | None = None
    domestic_history_label: str | None = None
    overseas_source: str
    overseas_symbol: str
    overseas_label: str
    formula: Literal["gold", "silver", "copper", "crude_oil", "cotton", "sugar", "aluminium", "soybean"]
    formula_version: str = "2026-03-13-v1"
    domestic_unit: DomesticUnit
    target_unit: TargetUnit
    tax_mode: TaxMode = "gross"
    enabled: bool = True
    thresholds: ThresholdConfig = Field(default_factory=ThresholdConfig)
    costs: CostModelConfig = Field(default_factory=CostModelConfig)
    domestic_candidates: list[QuoteRouteConfig] = Field(default_factory=list)
    overseas_candidates: list[QuoteRouteConfig] = Field(default_factory=list)
    hedge_contract_size: float | None = None
    domestic_lot_size: float | None = None
    domestic_product_code: str | None = None
    trading_sessions_local: list[str] = Field(default_factory=list)


class SourceConfig(BaseModel):
    kind: Literal[
        "sina_futures",
        "sina_fx",
        "shfe_delaymarket",
        "tqsdk_main",
        "okx_swap",
        "binance_futures",
        "gate_futures",
        "gate_tradfi",
        "hyperliquid",
        "cme_reference",
        "frankfurter_fx",
        "open_er_api_fx",
        "mock_quote",
        "mock_fx",
    ]
    base_url: str
    headers: dict[str, str] = Field(default_factory=dict)
    fallback_rate: float | None = None
    params: dict[str, str] = Field(default_factory=dict)
    verify_ssl: bool = False


class AppConfig(BaseModel):
    name: str = "Cross Market Spread Monitor"
    timezone: str = "Asia/Shanghai"
    domestic_trading_calendar_path: str | None = None
    domestic_weekends_closed: bool = True
    domestic_non_trading_dates_local: list[date] = Field(default_factory=list)
    poll_interval_sec: int = 10
    fx_poll_interval_sec: int = 3600
    fx_max_age_sec: int = 86400
    history_limit: int = 1000
    rolling_window_size: int = 120
    zscore_window_days: int = 30
    http_timeout_sec: int = 8
    sqlite_path: str = "data/monitor.db"
    fx_source: str = "frankfurter"
    fx_backup_sources: list[str] = Field(default_factory=list)
    bind_host: str = "0.0.0.0"
    bind_port: int = 6080
    export_dir: str = "exports"
    fx_window_size: int = 60
    replay_target_daily_vol_pct: float = 0.015
    startup_history_backfill_enabled: bool = True
    startup_history_backfill_range_key: Literal["24h", "7d", "30d", "90d", "1y", "all"] = "30d"
    tqsdk_shadow_source: str | None = None
    tqsdk_shadow_enabled: bool = False
    tqsdk_shadow_poll_interval_sec: int = 10
    tqsdk_startup_backfill_enabled: bool = False
    tqsdk_startup_backfill_interval: Literal["5m", "15m", "30m", "60m", "1d"] = "30m"
    tqsdk_startup_backfill_range_key: Literal["24h", "7d", "30d", "90d", "1y", "all"] = "30d"
    retention_enabled: bool = True
    retention_interval_sec: int = 21600
    raw_quote_retention_days: int = 30
    fx_rate_retention_days: int = 30
    normalized_quote_retention_days: int = 30
    snapshot_retention_days: int = 180
    alert_retention_days: int = 30
    delivery_retention_days: int = 30


class NotifierConfig(BaseModel):
    name: str
    kind: Literal["console", "webhook", "feishu", "telegram", "wecom"]
    enabled: bool = True
    min_severity: AlertSeverity = "warning"
    categories: list[str] = Field(default_factory=list)
    group_names: list[str] = Field(default_factory=list)
    url: str | None = None
    timeout_sec: int = 8
    headers: dict[str, str] = Field(default_factory=dict)
    bot_token: str | None = None
    chat_id: str | None = None


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
    category: Literal["spread_pct", "spread_level", "zscore", "data_quality", "fx", "executable"]
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
    status: Literal["ok", "partial", "stale", "error", "paused"]
    errors: list[str] = Field(default_factory=list)
    domestic_source: str | None = None
    overseas_source: str | None = None
    domestic_label: str | None = None
    overseas_label: str | None = None
    signal_state: Literal["active", "paused"] = "active"
    pause_reason: str | None = None
    fx_jump_pct: float | None = None

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
    ts_local: datetime | None = None
    route_detail: dict[str, Any] = Field(default_factory=dict)


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
    latest_fx_source: str | None = None
    latest_fx_jump_pct: float | None = None
    last_heartbeat_at: datetime | None = None
    fx_is_live: bool = False
    fx_is_frozen: bool = False
    fx_last_live_at: datetime | None = None
    fx_frozen_since: datetime | None = None


class SourceHealth(BaseModel):
    source_name: str
    kind: str
    success_count: int = 0
    failure_count: int = 0
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    last_error: str | None = None
    last_symbol: str | None = None
    last_latency_ms: float | None = None
    updated_at: datetime | None = None


class WorkerRuntimeState(BaseModel):
    state_name: str = "worker"
    started_at: datetime
    last_poll_started_at: datetime | None = None
    last_poll_finished_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    is_polling: bool = False
    total_cycles: int = 0
    latest_fx_rate: float | None = None
    latest_fx_source: str | None = None
    latest_fx_jump_pct: float | None = None
    fx_is_live: bool = False
    fx_is_frozen: bool = False
    fx_last_live_at: datetime | None = None
    fx_frozen_since: datetime | None = None


class JobRun(BaseModel):
    job_name: str
    status: Literal["idle", "running", "succeeded", "failed"]
    started_at: datetime | None = None
    finished_at: datetime | None = None
    updated_at: datetime
    details: dict[str, Any] = Field(default_factory=dict)


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
    trigger: Literal["spread_pct", "zscore", "cost_edge"]
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
    spread_pct_std: float | None = None
    spread_pct_median: float | None = None
    spread_pct_min: float | None = None
    spread_pct_max: float | None = None
    latest_spread_pct_percentile: float | None = None
    max_abs_zscore: float | None = None
    spread_pct_breach_count: int = 0
    zscore_breach_count: int = 0
    convergence_count: int = 0
    divergence_count: int = 0
    flat_count: int = 0
    convergence_ratio: float | None = None
    divergence_ratio: float | None = None
    hedge_ratio_ols: float | None = None
    hedge_intercept: float | None = None
    realized_daily_vol_pct: float | None = None
    recommended_position_scale: float | None = None
    average_round_trip_cost: float | None = None
    average_net_edge_after_cost: float | None = None
    profitable_after_cost_count: int = 0
    top_highlights: list[ReplayHighlight] = Field(default_factory=list)
    signal_entries: list[ReplaySignalEvent] = Field(default_factory=list)
