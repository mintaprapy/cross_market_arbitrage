from __future__ import annotations

import json
from datetime import UTC, datetime

from cross_market_monitor.domain.models import MarketQuote, SourceConfig
from cross_market_monitor.infrastructure.http_client import HttpClient

GATE_HISTORY_INTERVALS = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "60m": "1h",
    "4h": "4h",
    "1d": "1d",
}

GATE_INTERVAL_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1_800,
    "60m": 3_600,
    "4h": 14_400,
    "1d": 86_400,
}


class GateFuturesAdapter:
    def __init__(self, source_name: str, source_config: SourceConfig, http_client: HttpClient) -> None:
        self.source_name = source_name
        self.source_config = source_config
        self.http_client = http_client
        self._settle = self.source_config.params.get("settle", "usdt")

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        payload = json.loads(
            self.http_client.get_text(
                f"{self.source_config.base_url}/api/v4/futures/{self._settle}/tickers",
                params={"contract": symbol},
                headers=self.source_config.headers,
            )
        )
        if not payload:
            raise ValueError(f"Gate returned no data for {symbol}")

        ticker = payload[0]
        last = _float_or_none(ticker.get("last")) or _float_or_none(ticker.get("mark_price"))
        bid = _float_or_none(ticker.get("highest_bid"))
        ask = _float_or_none(ticker.get("lowest_ask"))
        if last is None and bid is not None and ask is not None:
            last = (bid + ask) / 2
        if last is None and bid is None and ask is None:
            raise ValueError(f"Gate ticker payload for {symbol} did not include quote fields")

        return MarketQuote(
            source_name=self.source_name,
            symbol=symbol,
            label=label,
            ts=datetime.now(UTC),
            last=last,
            bid=bid,
            ask=ask,
            raw_payload=json.dumps(ticker, ensure_ascii=False),
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
        api_interval = GATE_HISTORY_INTERVALS.get(interval)
        if api_interval is None:
            raise ValueError(f"Unsupported Gate history interval: {interval}")

        page_limit = min(int(self.source_config.params.get("history_page_limit", "1000")), 1000)
        max_pages = max(int(self.source_config.params.get("history_max_pages", "12")), 1)
        step_seconds = GATE_INTERVAL_SECONDS[interval]
        cursor_from = _to_seconds(start_ts)
        end_seconds = _to_seconds(end_ts)
        results: dict[str, MarketQuote] = {}

        for _ in range(max_pages):
            params = {
                "contract": symbol,
                "interval": api_interval,
            }
            if cursor_from is not None:
                params["from"] = str(cursor_from)
            if end_seconds is not None:
                params["to"] = str(end_seconds)

            payload = json.loads(
                self.http_client.get_text(
                    f"{self.source_config.base_url}/api/v4/futures/{self._settle}/candlesticks",
                    params=params,
                    headers=self.source_config.headers,
                )
            )
            if not payload:
                break

            last_open_seconds: int | None = None
            for row in payload:
                open_seconds = _parse_seconds(row.get("t"))
                if open_seconds is None:
                    continue
                ts = datetime.fromtimestamp(open_seconds, tz=UTC)
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
                last_open_seconds = open_seconds

            if len(payload) < page_limit or last_open_seconds is None:
                break
            next_cursor_from = last_open_seconds + step_seconds
            if end_seconds is not None and next_cursor_from > end_seconds:
                break
            if cursor_from is not None and next_cursor_from <= cursor_from:
                break
            cursor_from = next_cursor_from

        return sorted(results.values(), key=lambda item: item.ts)


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    parsed = float(value)
    return parsed if parsed > 0 else None


def _parse_seconds(raw: object) -> int | None:
    if raw in (None, ""):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _to_seconds(timestamp: datetime | None) -> int | None:
    if timestamp is None:
        return None
    return int(timestamp.astimezone(UTC).timestamp())
