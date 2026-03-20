from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta

from cross_market_monitor.domain.models import FXQuote, MarketQuote, PairConfig, QuoteRouteConfig, WorkerRuntimeState

DEFAULT_HISTORY_RANGE_KEY = "24h"
HISTORY_RANGE_CONFIG: dict[str, dict[str, int | timedelta | None]] = {
    "24h": {"duration": timedelta(hours=24), "target_points": 240},
    "7d": {"duration": timedelta(days=7), "target_points": 336},
    "30d": {"duration": timedelta(days=30), "target_points": 480},
    "90d": {"duration": timedelta(days=90), "target_points": 720},
    "1y": {"duration": timedelta(days=365), "target_points": 960},
    "all": {"duration": None, "target_points": 900},
}

OVERSEAS_HISTORY_INTERVAL_BY_RANGE = {
    "24h": "5m",
    "7d": "15m",
    "30d": "60m",
    "90d": "4h",
    "1y": "1d",
    "all": "1d",
}

FX_ALIGNMENT_TOLERANCE_SEC = 300


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class RouteFetchResult:
    candidate: QuoteRouteConfig
    quote: MarketQuote | None
    error: str | None
    latency_ms: float


@dataclass(slots=True)
class FXContext:
    quote: FXQuote | None
    jump_pct: float | None
    previous_rate: float | None
    is_live: bool = False
    fetched: bool = False


def age_seconds(timestamp: datetime) -> float:
    return max((utc_now() - timestamp.astimezone(UTC)).total_seconds(), 0.0)


def max_skew_seconds(
    domestic: MarketQuote | None,
    overseas: MarketQuote | None,
    fx: FXQuote | None,
) -> float | None:
    timestamps = [
        item.ts.astimezone(UTC)
        for item in (domestic, overseas, fx)
        if item is not None
    ]
    if len(timestamps) < 2:
        return None
    seconds = [ts.timestamp() for ts in timestamps]
    return max(seconds) - min(seconds)


def dedupe_candidates(candidates: list[QuoteRouteConfig]) -> list[QuoteRouteConfig]:
    results: list[QuoteRouteConfig] = []
    seen: set[tuple[str, str]] = set()
    for candidate in candidates:
        key = (candidate.source, candidate.symbol.lower())
        if key in seen:
            continue
        seen.add(key)
        results.append(candidate)
    return results


def prioritize_candidates(
    candidates: list[QuoteRouteConfig],
    preferred_symbol: str | None,
) -> list[QuoteRouteConfig]:
    if not preferred_symbol:
        return candidates

    preferred_lower = preferred_symbol.lower()
    prioritized: list[QuoteRouteConfig] = []
    for candidate in candidates:
        if candidate.symbol.lower() == preferred_lower:
            prioritized.append(candidate.model_copy(update={"enabled": True}))
    prioritized.extend(
        candidate
        for candidate in candidates
        if candidate.symbol.lower() != preferred_lower
    )
    return prioritized


def default_overseas_symbol(pair: PairConfig) -> str:
    candidates = dedupe_candidates(pair.overseas_candidates) if pair.overseas_candidates else [
        QuoteRouteConfig(
            source=pair.overseas_source,
            symbol=pair.overseas_symbol,
            label=pair.overseas_label,
        )
    ]
    source_priority = {
        "binance_futures": 0,
        "okx_swap": 1,
        "hyperliquid": 2,
    }
    enabled_candidates = [candidate for candidate in candidates if candidate.enabled]
    pool = enabled_candidates or candidates
    selected = min(
        enumerate(pool),
        key=lambda item: (source_priority.get(item[1].source, 99), item[0]),
    )[1]
    return selected.symbol


def variant_group_base(group_name: str) -> str:
    for suffix in ("_GROSS", "_NET"):
        if group_name.endswith(suffix):
            return group_name[: -len(suffix)]
    return group_name


def infer_product_code(symbol: str) -> str | None:
    mapping = {
        "nf_AU0": "au",
        "nf_AG0": "ag",
        "nf_CU0": "cu",
        "nf_BC0": "bc",
        "nf_SC0": "sc",
    }
    if symbol in mapping:
        return mapping[symbol]
    stripped = symbol.replace("nf_", "")
    letters = "".join(ch for ch in stripped if ch.isalpha())
    return letters.lower() or None


def is_within_trading_sessions(local_dt: datetime, sessions: list[str], *, grace_sec: int = 0) -> bool:
    if not sessions:
        return True
    timezone = local_dt.tzinfo
    if timezone is None:
        return False
    for session in sessions:
        try:
            start_text, end_text = session.split("-", 1)
            start_time = time.fromisoformat(start_text)
            end_time = time.fromisoformat(end_text)
        except ValueError:
            continue
        if _session_matches(local_dt, timezone, start_time, end_time, grace_sec):
            return True
    return False


def latest_session_end_before(local_dt: datetime, sessions: list[str]) -> datetime | None:
    if not sessions:
        return None
    timezone = local_dt.tzinfo
    if timezone is None:
        return None

    candidates: list[datetime] = []
    for session in sessions:
        try:
            start_text, end_text = session.split("-", 1)
            start_time = time.fromisoformat(start_text)
            end_time = time.fromisoformat(end_text)
        except ValueError:
            continue
        for anchor_date in (local_dt.date(), local_dt.date() - timedelta(days=1)):
            start_dt = datetime.combine(anchor_date, start_time, tzinfo=timezone)
            end_dt = datetime.combine(anchor_date, end_time, tzinfo=timezone)
            if end_time <= start_time:
                end_dt += timedelta(days=1)
            if end_dt <= local_dt and end_dt >= start_dt:
                candidates.append(end_dt)
    return max(candidates) if candidates else None


def _session_matches(
    local_dt: datetime,
    timezone,
    start_time: time,
    end_time: time,
    grace_sec: int,
) -> bool:
    anchor_dates = [local_dt.date()]
    if end_time <= start_time:
        anchor_dates.append(local_dt.date() - timedelta(days=1))
    for anchor_date in anchor_dates:
        start_dt = datetime.combine(anchor_date, start_time, tzinfo=timezone)
        end_dt = datetime.combine(anchor_date, end_time, tzinfo=timezone)
        if end_time <= start_time:
            end_dt += timedelta(days=1)
        if start_dt <= local_dt <= end_dt + timedelta(seconds=grace_sec):
            return True
    return False


def build_worker_runtime_state(context) -> WorkerRuntimeState:
    return WorkerRuntimeState(
        started_at=context.started_at,
        last_poll_started_at=context.last_poll_started_at,
        last_poll_finished_at=context.last_poll_finished_at,
        last_heartbeat_at=utc_now(),
        is_polling=context.is_polling,
        total_cycles=context.total_cycles,
        latest_fx_rate=context.latest_fx_quote.rate if context.latest_fx_quote else None,
        latest_fx_source=context.latest_fx_quote.source_name if context.latest_fx_quote else None,
        latest_fx_jump_pct=context.latest_fx_jump_pct,
        fx_is_live=context.latest_fx_is_live,
        fx_is_frozen=bool(context.latest_fx_quote is not None and not context.latest_fx_is_live),
        fx_last_live_at=context.latest_fx_last_live_at,
        fx_frozen_since=context.latest_fx_frozen_since,
    )
