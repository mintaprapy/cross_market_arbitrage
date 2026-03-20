from __future__ import annotations

from datetime import UTC, datetime

from cross_market_monitor.domain.models import MarketQuote, SourceConfig
from cross_market_monitor.infrastructure.http_client import HttpClient


class CmeReferenceAdapter:
    def __init__(self, source_name: str, source_config: SourceConfig, http_client: HttpClient) -> None:
        self.source_name = source_name
        self.source_config = source_config
        self.http_client = http_client

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        # `symbol` should be a CME future endpoint identifier such as `425`.
        payload = self.http_client.get_json(
            f"{self.source_config.base_url}/{symbol}/G",
            headers=self.source_config.headers,
            params=self.source_config.params,
        )
        values = _flatten_values(payload)
        last = _first_number(values, ["last", "lastprice", "lasttradeprice", "lastquote", "close"])
        bid = _first_number(values, ["bid", "bidprice"])
        ask = _first_number(values, ["ask", "askprice"])
        ts = datetime.now(UTC)
        if last is None and bid is None and ask is None:
            raise ValueError(f"CME reference payload for {symbol} did not include quote fields")
        if last is None and bid is not None and ask is not None:
            last = (bid + ask) / 2
        return MarketQuote(
            source_name=self.source_name,
            symbol=symbol,
            label=label,
            ts=ts,
            last=last,
            bid=bid,
            ask=ask,
            raw_payload=str(payload),
        )


def _flatten_values(payload: object, prefix: str = "") -> dict[str, float]:
    flattened: dict[str, float] = {}
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                flattened.update(_flatten_values(value, f"{prefix}{key}."))
            else:
                number = _to_positive_float(value)
                if number is not None:
                    flattened[f"{prefix}{key}".lower()] = number
    elif isinstance(payload, list):
        for index, item in enumerate(payload):
            flattened.update(_flatten_values(item, f"{prefix}{index}."))
    return flattened


def _first_number(values: dict[str, float], candidates: list[str]) -> float | None:
    for key, value in values.items():
        if any(candidate in key for candidate in candidates):
            return value
    return None


def _to_positive_float(value: object) -> float | None:
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None
