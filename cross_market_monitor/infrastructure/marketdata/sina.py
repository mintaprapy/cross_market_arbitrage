from __future__ import annotations

import re
from datetime import UTC, datetime

from cross_market_monitor.domain.models import MarketQuote, SourceConfig
from cross_market_monitor.infrastructure.http_client import HttpClient

PRICE_CANDIDATES = [8, 7, 6, 2, 3]


def parse_sina_futures_payload(source_name: str, symbol: str, label: str, payload: str) -> MarketQuote:
    match = re.search(r'="([^"]*)"', payload)
    if not match:
        raise ValueError(f"Unexpected Sina payload for {symbol}")

    parts = match.group(1).split(",")
    if not parts or all(part == "" for part in parts):
        raise ValueError(f"Empty Sina payload for {symbol}")

    values = [float(parts[index]) for index in PRICE_CANDIDATES if index < len(parts) and _is_positive(parts[index])]
    if not values:
        raise ValueError(f"Could not parse latest price for {symbol}")

    bid = _float_or_none(parts, 6)
    ask = _float_or_none(parts, 7)
    last = values[0]

    trade_date = parts[17] if len(parts) > 17 and parts[17] else datetime.now(UTC).date().isoformat()
    hhmmss = parts[1] if len(parts) > 1 else ""
    timestamp = _parse_sina_datetime(trade_date, hhmmss)

    return MarketQuote(
        source_name=source_name,
        symbol=symbol,
        label=label,
        ts=timestamp,
        last=last,
        bid=bid,
        ask=ask,
        raw_payload=payload.strip(),
    )


def _is_positive(value: str) -> bool:
    try:
        return float(value) > 0
    except ValueError:
        return False


def _float_or_none(parts: list[str], index: int) -> float | None:
    if index >= len(parts):
        return None
    try:
        value = float(parts[index])
    except ValueError:
        return None
    return value if value > 0 else None


def _parse_sina_datetime(trade_date: str, hhmmss: str) -> datetime:
    cleaned = hhmmss.strip()
    if len(cleaned) == 6 and cleaned.isdigit():
        return datetime.fromisoformat(
            f"{trade_date}T{cleaned[0:2]}:{cleaned[2:4]}:{cleaned[4:6]}+08:00"
        ).astimezone(UTC)
    return datetime.now(UTC)


class SinaFuturesAdapter:
    def __init__(self, source_name: str, source_config: SourceConfig, http_client: HttpClient) -> None:
        self.source_name = source_name
        self.source_config = source_config
        self.http_client = http_client

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        payload = self.http_client.get_text(
            f"{self.source_config.base_url}/list={symbol}",
            headers=self.source_config.headers,
        )
        return parse_sina_futures_payload(self.source_name, symbol, label, payload)
