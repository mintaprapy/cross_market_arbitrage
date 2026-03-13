from __future__ import annotations

from datetime import UTC, datetime

from cross_market_monitor.domain.models import FXQuote, SourceConfig
from cross_market_monitor.infrastructure.http_client import HttpClient


class FrankfurterFxAdapter:
    def __init__(self, source_name: str, source_config: SourceConfig, http_client: HttpClient) -> None:
        self.source_name = source_name
        self.source_config = source_config
        self.http_client = http_client

    def fetch_rate(self, base: str, quote: str) -> FXQuote:
        payload = self.http_client.get_json(
            f"{self.source_config.base_url}/latest",
            params={"from": base, "to": quote},
            headers=self.source_config.headers,
        )
        rate = float(payload["rates"][quote])
        return FXQuote(
            source_name=self.source_name,
            pair=f"{base}/{quote}",
            ts=datetime.now(UTC),
            rate=rate,
            raw_payload=str(payload),
        )
