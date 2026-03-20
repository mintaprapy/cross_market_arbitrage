from __future__ import annotations

import json
from datetime import UTC, datetime

from cross_market_monitor.domain.models import MarketQuote, SourceConfig
from cross_market_monitor.infrastructure.http_client import HttpClient


class HyperliquidAdapter:
    def __init__(self, source_name: str, source_config: SourceConfig, http_client: HttpClient) -> None:
        self.source_name = source_name
        self.source_config = source_config
        self.http_client = http_client

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        payload = json.loads(
            self.http_client.post_json(
                f"{self.source_config.base_url}/info",
                {"type": "metaAndAssetCtxs"},
                headers=self.source_config.headers,
            )
        )
        meta, contexts = payload
        index = next(
            (idx for idx, item in enumerate(meta["universe"]) if item["name"] == symbol),
            None,
        )
        if index is None:
            raise ValueError(f"Hyperliquid symbol {symbol} was not found in universe")

        context = contexts[index]
        book_payload = json.loads(
            self.http_client.post_json(
                f"{self.source_config.base_url}/info",
                {"type": "l2Book", "coin": symbol},
                headers=self.source_config.headers,
            )
        )
        bid, ask = _best_bid_ask(book_payload)
        last = _float_or_none(context.get("markPx")) or _float_or_none(context.get("oraclePx"))
        if last is None and bid is not None and ask is not None:
            last = (bid + ask) / 2

        return MarketQuote(
            source_name=self.source_name,
            symbol=symbol,
            label=label,
            ts=_parse_ms_timestamp(book_payload.get("time")),
            last=last,
            bid=bid,
            ask=ask,
            raw_payload=str({"context": context, "book": book_payload}),
        )


def _float_or_none(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    parsed = float(value)
    return parsed if parsed > 0 else None


def _best_bid_ask(payload: dict) -> tuple[float | None, float | None]:
    levels = payload.get("levels") or []
    bid = ask = None
    if len(levels) >= 1 and levels[0]:
        bid = _float_or_none(levels[0][0].get("px"))
    if len(levels) >= 2 and levels[1]:
        ask = _float_or_none(levels[1][0].get("px"))
    return bid, ask


def _parse_ms_timestamp(raw: str | int | None) -> datetime:
    if raw is None:
        return datetime.now(UTC)
    text = str(raw)
    if text.isdigit():
        return datetime.fromtimestamp(int(text) / 1000, tz=UTC)
    return datetime.now(UTC)
