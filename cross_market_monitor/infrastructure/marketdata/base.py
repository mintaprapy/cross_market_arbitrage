from __future__ import annotations

from typing import Protocol

from cross_market_monitor.domain.models import FXQuote, MarketQuote, SourceConfig


class QuoteAdapter(Protocol):
    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        ...


class FxAdapter(Protocol):
    def fetch_rate(self, base: str, quote: str) -> FXQuote:
        ...


class AdapterFactory(Protocol):
    def __call__(self, source_name: str, source_config: SourceConfig):
        ...
