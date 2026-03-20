from __future__ import annotations

import json
from datetime import UTC, datetime

from cross_market_monitor.domain.models import MarketQuote, SourceConfig
from cross_market_monitor.infrastructure.http_client import HttpClient

BINANCE_HISTORY_INTERVALS = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "60m": "1h",
    "4h": "4h",
    "1d": "1d",
}

BINANCE_INTERVAL_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "60m": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


class BinanceFuturesAdapter:
    def __init__(self, source_name: str, source_config: SourceConfig, http_client: HttpClient) -> None:
        self.source_name = source_name
        self.source_config = source_config
        self.http_client = http_client

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        payload = self.http_client.get_json(
            f"{self.source_config.base_url}/fapi/v1/ticker/24hr",
            params={"symbol": symbol},
            headers=self.source_config.headers,
        )
        ts = _parse_ms_timestamp(payload.get("closeTime"))
        return MarketQuote(
            source_name=self.source_name,
            symbol=symbol,
            label=label,
            ts=ts,
            last=_float_or_none(payload.get("lastPrice")),
            bid=_float_or_none(payload.get("bidPrice")),
            ask=_float_or_none(payload.get("askPrice")),
            raw_payload=str(payload),
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
        api_interval = BINANCE_HISTORY_INTERVALS.get(interval)
        if api_interval is None:
            raise ValueError(f"Unsupported Binance history interval: {interval}")

        page_limit = min(int(self.source_config.params.get("history_page_limit", "1500")), 1500)
        max_pages = max(int(self.source_config.params.get("history_max_pages", "12")), 1)
        step_ms = BINANCE_INTERVAL_MS[interval]
        start_ms = _to_ms(start_ts)
        end_ms = _to_ms(end_ts)
        cursor_ms = start_ms
        results: dict[str, MarketQuote] = {}

        for _ in range(max_pages):
            params = {
                "symbol": symbol,
                "interval": api_interval,
                "limit": str(page_limit),
            }
            if cursor_ms is not None:
                params["startTime"] = str(cursor_ms)
            if end_ms is not None:
                params["endTime"] = str(end_ms)

            payload = json.loads(
                self.http_client.get_text(
                    f"{self.source_config.base_url}/fapi/v1/klines",
                    params=params,
                    headers=self.source_config.headers,
                )
            )
            if not payload:
                break

            last_open_ms: int | None = None
            for row in payload:
                if len(row) < 5:
                    continue
                open_ms = int(row[0])
                ts = _parse_ms_timestamp(open_ms)
                if start_ts is not None and ts < start_ts:
                    continue
                if end_ts is not None and ts > end_ts:
                    continue
                close_px = _float_or_none(str(row[4]))
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
                last_open_ms = open_ms

            if len(payload) < page_limit or last_open_ms is None or cursor_ms is None:
                break

            next_cursor_ms = last_open_ms + step_ms
            if end_ms is not None and next_cursor_ms > end_ms:
                break
            if next_cursor_ms <= cursor_ms:
                break
            cursor_ms = next_cursor_ms

        return sorted(results.values(), key=lambda item: item.ts)


def _float_or_none(value: str | None) -> float | None:
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


def _to_ms(timestamp: datetime | None) -> int | None:
    if timestamp is None:
        return None
    return int(timestamp.astimezone(UTC).timestamp() * 1000)
