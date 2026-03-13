from __future__ import annotations

from datetime import UTC, datetime

from cross_market_monitor.domain.models import MarketQuote, SourceConfig
from cross_market_monitor.infrastructure.http_client import HttpClient


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


def _float_or_none(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    parsed = float(value)
    return parsed if parsed > 0 else None


def _parse_ms_timestamp(raw: str | None) -> datetime:
    if raw and raw.isdigit():
        return datetime.fromtimestamp(int(raw) / 1000, tz=UTC)
    return datetime.now(UTC)
