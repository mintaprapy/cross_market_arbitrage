from __future__ import annotations

import json
from datetime import UTC, datetime

from cross_market_monitor.domain.models import MarketQuote, SourceConfig
from cross_market_monitor.infrastructure.http_client import HttpClient

GATE_TRADFI_HISTORY_INTERVALS = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "60m": "1h",
    "4h": "4h",
    "1d": "1d",
}

GATE_TRADFI_INTERVAL_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1_800,
    "60m": 3_600,
    "4h": 14_400,
    "1d": 86_400,
}

GATE_TRADFI_INTERVAL_ORDER = ["1m", "5m", "15m", "30m", "60m", "4h", "1d"]


class GateTradFiAdapter:
    def __init__(self, source_name: str, source_config: SourceConfig, http_client: HttpClient) -> None:
        self.source_name = source_name
        self.source_config = source_config
        self.http_client = http_client

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        payload = self.http_client.get_json(
            f"{self.source_config.base_url}/api/v4/tradfi/symbols/{symbol}/tickers",
            headers=self.source_config.headers,
        )
        ticker = payload.get("data") or {}
        last = _float_or_none(ticker.get("last_price"))
        bid = _float_or_none(ticker.get("bid_price")) or _float_or_none(ticker.get("buy_price"))
        ask = _float_or_none(ticker.get("ask_price")) or _float_or_none(ticker.get("sell_price"))
        if last is None and bid is not None and ask is not None:
            last = (bid + ask) / 2
        if last is None and bid is None and ask is None:
            raise ValueError(f"Gate TradFi ticker payload for {symbol} did not include quote fields")

        return MarketQuote(
            source_name=self.source_name,
            symbol=symbol,
            label=label,
            ts=_parse_ms_timestamp(payload.get("timestamp") or ticker.get("timestamp")),
            last=last,
            bid=bid,
            ask=ask,
            raw_payload=json.dumps(payload, ensure_ascii=False),
        )

    def fetch_history(
        self,
        symbol: str,
        label: str,
        *,
        interval: str = "60m",
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
    ) -> list[MarketQuote]:
        history_limit = min(int(self.source_config.params.get("history_limit", "500")), 500)
        selected_interval = _coarsen_interval(interval, start_ts, end_ts, history_limit)
        api_interval = GATE_TRADFI_HISTORY_INTERVALS.get(selected_interval)
        if api_interval is None:
            raise ValueError(f"Unsupported Gate TradFi history interval: {interval}")

        payload = self.http_client.get_json(
            f"{self.source_config.base_url}/api/v4/tradfi/symbols/{symbol}/klines",
            params={"kline_type": api_interval, "limit": str(history_limit)},
            headers=self.source_config.headers,
        )
        rows = payload.get("data", {}).get("list") or []
        results: dict[str, MarketQuote] = {}
        for row in rows:
            raw_timestamp = row.get("t")
            if raw_timestamp in (None, ""):
                continue
            ts = datetime.fromtimestamp(int(raw_timestamp), tz=UTC)
            if start_ts is not None and ts < start_ts:
                continue
            if end_ts is not None and ts > end_ts:
                continue
            close_px = _float_or_none(row.get("c"))
            if close_px is None:
                continue
            results[ts.isoformat()] = MarketQuote(
                source_name=self.source_name,
                symbol=symbol,
                label=label,
                ts=ts,
                last=close_px,
                bid=None,
                ask=None,
                raw_payload=json.dumps(row, ensure_ascii=False),
            )
        return sorted(results.values(), key=lambda item: item.ts)


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    parsed = float(value)
    return parsed if parsed > 0 else None


def _parse_ms_timestamp(raw: str | int | None) -> datetime:
    if raw is None:
        return datetime.now(UTC)
    text = str(raw)
    if text.isdigit():
        return datetime.fromtimestamp(int(text) / 1000, tz=UTC)
    return datetime.now(UTC)


def _coarsen_interval(
    interval: str,
    start_ts: datetime | None,
    end_ts: datetime | None,
    history_limit: int,
) -> str:
    if start_ts is None or end_ts is None:
        return interval
    requested_seconds = GATE_TRADFI_INTERVAL_SECONDS.get(interval)
    if requested_seconds is None or history_limit <= 0:
        return interval
    window_seconds = max(int((end_ts - start_ts).total_seconds()), 0)
    if window_seconds <= requested_seconds * history_limit:
        return interval

    try:
        start_index = GATE_TRADFI_INTERVAL_ORDER.index(interval)
    except ValueError:
        return interval

    for candidate in GATE_TRADFI_INTERVAL_ORDER[start_index + 1 :]:
        candidate_seconds = GATE_TRADFI_INTERVAL_SECONDS[candidate]
        if window_seconds <= candidate_seconds * history_limit:
            return candidate
    return GATE_TRADFI_INTERVAL_ORDER[-1]
