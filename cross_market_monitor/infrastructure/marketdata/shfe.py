from __future__ import annotations

import re
from datetime import UTC, datetime
from time import monotonic

from cross_market_monitor.domain.models import MarketQuote, SourceConfig
from cross_market_monitor.infrastructure.http_client import HttpClient


class ShfeDelayMarketAdapter:
    def __init__(self, source_name: str, source_config: SourceConfig, http_client: HttpClient) -> None:
        self.source_name = source_name
        self.source_config = source_config
        self.http_client = http_client
        self._cache: dict[str, tuple[float, dict]] = {}
        self._cache_ttl_sec = 15.0

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        product_id, contract_name = _split_symbol(symbol)
        payload = self._load_product(product_id)
        rows = payload.get("delaymarket") or []
        row = next(
            (item for item in rows if str(item.get("contractname", "")).lower() == contract_name.lower()),
            None,
        )
        if row is None:
            raise ValueError(f"SHFE delayed market returned no contract row for {symbol}")
        return MarketQuote(
            source_name=self.source_name,
            symbol=contract_name,
            label=label,
            ts=_parse_timestamp(row.get("updatetime")),
            last=_float_or_none(row.get("lastprice")),
            bid=_float_or_none(row.get("bidprice")),
            ask=_float_or_none(row.get("askprice")),
            raw_payload=str(row),
        )

    def list_contracts(self, product_id: str, *, limit: int = 12) -> list[dict]:
        payload = self._load_product(product_id.lower())
        rows = payload.get("delaymarket") or []
        candidates = []
        for row in rows:
            contract_name = str(row.get("contractname", "")).lower()
            last = _float_or_none(row.get("lastprice"))
            bid = _float_or_none(row.get("bidprice"))
            ask = _float_or_none(row.get("askprice"))
            if not contract_name or (last is None and bid is None and ask is None):
                continue
            volume = _float_or_none(row.get("volume"))
            open_interest = _float_or_none(row.get("openinterest"))
            candidates.append(
                {
                    "source": self.source_name,
                    "symbol": contract_name,
                    "label": (
                        f"SHFE/INE {contract_name.upper()} "
                        f"OI {int(open_interest or 0)} Vol {int(volume or 0)}"
                    ),
                    "last": last,
                    "bid": bid,
                    "ask": ask,
                    "volume": volume,
                    "open_interest": open_interest,
                    "ts": _parse_timestamp(row.get("updatetime")).isoformat(),
                }
            )
        return candidates[:limit]

    def _load_product(self, product_id: str) -> dict:
        cached = self._cache.get(product_id)
        if cached and (monotonic() - cached[0]) < self._cache_ttl_sec:
            return cached[1]

        payload = self.http_client.get_json(
            f"{self.source_config.base_url}/data/tradedata/future/delaymarket/delaymarket_{product_id}.dat",
            headers=self.source_config.headers,
        )
        self._cache[product_id] = (monotonic(), payload)
        return payload


def _split_symbol(symbol: str) -> tuple[str, str]:
    if ":" in symbol:
        product_id, contract_name = symbol.split(":", 1)
        return product_id.lower(), contract_name.lower()
    match = re.match(r"([A-Za-z]+)\d+", symbol)
    if not match:
        raise ValueError(f"Could not infer SHFE product from symbol {symbol}")
    product_id = match.group(1).lower()
    return product_id, symbol.lower()


def _float_or_none(value: object) -> float | None:
    if value in (None, "", "-"):
        return None
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _parse_timestamp(value: object) -> datetime:
    text = str(value or "").strip()
    if text:
        try:
            return datetime.fromisoformat(text.replace(" ", "T") + "+08:00").astimezone(UTC)
        except ValueError:
            pass
    return datetime.now(UTC)
