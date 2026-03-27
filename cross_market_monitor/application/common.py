from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

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
    "24h": "15m",
    "7d": "15m",
    "30d": "15m",
    "90d": "15m",
    "1y": "15m",
    "all": "15m",
}

DOMESTIC_HISTORY_INTERVAL_BY_RANGE = {
    "24h": "15m",
    "7d": "15m",
    "30d": "15m",
    "90d": "15m",
    "1y": "15m",
    "all": "15m",
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
        "gate_futures": 2,
        "gate_tradfi": 3,
        "hyperliquid": 4,
        "hyperliquid_xyz": 5,
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


def display_group_name(group_name: str) -> str:
    base_name = variant_group_base(group_name)
    if group_name.endswith("_NET"):
        return f"{base_name}除税"
    return base_name


def data_quality_group_name(group_name: str) -> str:
    return variant_group_base(group_name)


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


def parse_non_trading_dates(non_trading_dates: list[str] | set[date] | None) -> set[date]:
    if not non_trading_dates:
        return set()
    parsed: set[date] = set()
    for item in non_trading_dates:
        if isinstance(item, date) and not isinstance(item, datetime):
            parsed.add(item)
            continue
        if isinstance(item, str):
            try:
                parsed.add(date.fromisoformat(item))
            except ValueError:
                continue
    return parsed


def is_trading_day_local(local_day: date, *, non_trading_dates: list[str] | set[date] | None = None, weekends_closed: bool = True) -> bool:
    if weekends_closed and local_day.weekday() >= 5:
        return False
    return local_day not in parse_non_trading_dates(non_trading_dates)


def is_within_trading_sessions(
    local_dt: datetime,
    sessions: list[str],
    *,
    grace_sec: int = 0,
    non_trading_dates: list[str] | set[date] | None = None,
    weekends_closed: bool = True,
) -> bool:
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
        if _session_matches(
            local_dt,
            timezone,
            start_time,
            end_time,
            grace_sec,
            non_trading_dates=non_trading_dates,
            weekends_closed=weekends_closed,
        ):
            return True
    return False


def latest_session_end_before(
    local_dt: datetime,
    sessions: list[str],
    *,
    non_trading_dates: list[str] | set[date] | None = None,
    weekends_closed: bool = True,
    lookback_days: int = 14,
) -> datetime | None:
    if not sessions:
        return None
    timezone = local_dt.tzinfo
    if timezone is None:
        return None

    parsed_non_trading_dates = parse_non_trading_dates(non_trading_dates)
    candidates: list[datetime] = []
    for offset in range(lookback_days + 1):
        anchor_date = local_dt.date() - timedelta(days=offset)
        for session in sessions:
            try:
                start_text, end_text = session.split("-", 1)
                start_time = time.fromisoformat(start_text)
                end_time = time.fromisoformat(end_text)
            except ValueError:
                continue
            candidate = _session_end_for_anchor(
                anchor_date,
                timezone,
                start_time,
                end_time,
                non_trading_dates=parsed_non_trading_dates,
                weekends_closed=weekends_closed,
            )
            if candidate is not None and candidate <= local_dt:
                candidates.append(candidate)
    return max(candidates) if candidates else None


def _session_matches(
    local_dt: datetime,
    timezone,
    start_time: time,
    end_time: time,
    grace_sec: int,
    *,
    non_trading_dates: list[str] | set[date] | None = None,
    weekends_closed: bool = True,
) -> bool:
    parsed_non_trading_dates = parse_non_trading_dates(non_trading_dates)
    anchor_dates = [local_dt.date()]
    if end_time <= start_time:
        anchor_dates.append(local_dt.date() - timedelta(days=1))
    for anchor_date in anchor_dates:
        window = _session_window_for_anchor(
            anchor_date,
            timezone,
            start_time,
            end_time,
            non_trading_dates=parsed_non_trading_dates,
            weekends_closed=weekends_closed,
        )
        if window is None:
            continue
        start_dt, end_dt = window
        if start_dt <= local_dt <= end_dt + timedelta(seconds=grace_sec):
            return True
    return False


def _session_window_for_anchor(
    anchor_date: date,
    timezone,
    start_time: time,
    end_time: time,
    *,
    non_trading_dates: set[date],
    weekends_closed: bool,
) -> tuple[datetime, datetime] | None:
    start_dt = datetime.combine(anchor_date, start_time, tzinfo=timezone)
    end_dt = datetime.combine(anchor_date, end_time, tzinfo=timezone)
    if end_time <= start_time:
        if not is_trading_day_local(
            anchor_date,
            non_trading_dates=non_trading_dates,
            weekends_closed=weekends_closed,
        ):
            return None
        if _has_holiday_gap_before_next_trading_day(
            anchor_date,
            non_trading_dates=non_trading_dates,
            weekends_closed=weekends_closed,
        ):
            return None
        end_dt += timedelta(days=1)
        return start_dt, end_dt
    if not is_trading_day_local(anchor_date, non_trading_dates=non_trading_dates, weekends_closed=weekends_closed):
        return None
    return start_dt, end_dt


def _session_end_for_anchor(
    anchor_date: date,
    timezone,
    start_time: time,
    end_time: time,
    *,
    non_trading_dates: set[date],
    weekends_closed: bool,
) -> datetime | None:
    window = _session_window_for_anchor(
        anchor_date,
        timezone,
        start_time,
        end_time,
        non_trading_dates=non_trading_dates,
        weekends_closed=weekends_closed,
    )
    if window is None:
        return None
    return window[1]


def _has_holiday_gap_before_next_trading_day(
    anchor_date: date,
    *,
    non_trading_dates: set[date],
    weekends_closed: bool,
    lookahead_days: int = 14,
) -> bool:
    next_trading_day = _next_trading_day_after(
        anchor_date,
        non_trading_dates=non_trading_dates,
        weekends_closed=weekends_closed,
        lookahead_days=lookahead_days,
    )
    if next_trading_day is None:
        return True
    gap_days = (next_trading_day - anchor_date).days
    if gap_days <= 1:
        return False
    for offset in range(1, gap_days):
        gap_date = anchor_date + timedelta(days=offset)
        if gap_date in non_trading_dates:
            return True
    return False


def _next_trading_day_after(
    anchor_date: date,
    *,
    non_trading_dates: set[date],
    weekends_closed: bool,
    lookahead_days: int = 14,
) -> date | None:
    for offset in range(1, lookahead_days + 1):
        candidate = anchor_date + timedelta(days=offset)
        if is_trading_day_local(candidate, non_trading_dates=non_trading_dates, weekends_closed=weekends_closed):
            return candidate
    return None


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
