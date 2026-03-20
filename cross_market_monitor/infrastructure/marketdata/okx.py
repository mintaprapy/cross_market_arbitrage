from __future__ import annotations

import json
from datetime import UTC, datetime

from cross_market_monitor.domain.models import MarketQuote, SourceConfig
from cross_market_monitor.infrastructure.http_client import HttpClient

OKX_HISTORY_INTERVALS = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "60m": "1H",
    "4h": "4H",
    "1d": "1D",
}


class OkxSwapAdapter:
    def __init__(self, source_name: str, source_config: SourceConfig, http_client: HttpClient) -> None:
        self.source_name = source_name
        self.source_config = source_config
        self.http_client = http_client

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        payload = self.http_client.get_json(
            f"{self.source_config.base_url}/api/v5/market/ticker",
            params={"instId": symbol},
            headers=self.source_config.headers,
        )
        data = payload.get("data") or []
        if not data:
            raise ValueError(f"OKX returned no data for {symbol}")

        ticker = data[0]
        ts = _parse_ms_timestamp(ticker.get("ts"))
        return MarketQuote(
            source_name=self.source_name,
            symbol=symbol,
            label=label,
            ts=ts,
            last=_float_or_none(ticker.get("last")),
            bid=_float_or_none(ticker.get("bidPx")),
            ask=_float_or_none(ticker.get("askPx")),
            raw_payload=str(ticker),
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
        api_interval = OKX_HISTORY_INTERVALS.get(interval)
        if api_interval is None:
            raise ValueError(f"Unsupported OKX history interval: {interval}")

        page_limit = min(int(self.source_config.params.get("history_page_limit", "300")), 300)
        max_pages = max(int(self.source_config.params.get("history_max_pages", "12")), 1)
        cursor_after: int | None = None
        results: dict[str, MarketQuote] = {}

        for _ in range(max_pages):
            params = {
                "instId": symbol,
                "bar": api_interval,
                "limit": str(page_limit),
            }
            if cursor_after is not None:
                params["after"] = str(cursor_after)

            payload = self.http_client.get_json(
                f"{self.source_config.base_url}/api/v5/market/history-candles",
                params=params,
                headers=_history_headers(self.source_config.headers),
            )
            data = payload.get("data") or []
            if not data:
                break

            oldest_ms: int | None = None
            for row in data:
                if len(row) < 5:
                    continue
                open_ms = int(row[0])
                ts = _parse_ms_timestamp(str(open_ms))
                if start_ts is not None and ts < start_ts:
                    oldest_ms = open_ms
                    continue
                if end_ts is not None and ts > end_ts:
                    continue
                close_px = _float_or_none(row[4])
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
                oldest_ms = open_ms

            if len(data) < page_limit or oldest_ms is None:
                break
            if start_ts is not None and oldest_ms <= _to_ms(start_ts):
                break
            if cursor_after == oldest_ms:
                break
            cursor_after = oldest_ms

        return sorted(results.values(), key=lambda item: item.ts)


def _float_or_none(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    parsed = float(value)
    return parsed if parsed > 0 else None


def _parse_ms_timestamp(raw: str | None) -> datetime:
    if raw and raw.isdigit():
        return datetime.fromtimestamp(int(raw) / 1000, tz=UTC)
    return datetime.now(UTC)


def _to_ms(timestamp: datetime | None) -> int | None:
    if timestamp is None:
        return None
    return int(timestamp.astimezone(UTC).timestamp() * 1000)


def _history_headers(configured_headers: dict[str, str]) -> dict[str, str]:
    headers = dict(configured_headers)
    headers.setdefault("Referer", "https://www.okx.com/")
    headers.setdefault("User-Agent", "Mozilla/5.0")
    return headers
