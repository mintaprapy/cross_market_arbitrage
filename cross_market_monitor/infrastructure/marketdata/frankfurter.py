from __future__ import annotations

from datetime import UTC, datetime
from urllib.parse import urlparse

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

    def fetch_history(
        self,
        base: str,
        quote: str,
        *,
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
    ) -> list[FXQuote]:
        start_dt = (start_ts or datetime.now(UTC)).astimezone(UTC)
        end_dt = (end_ts or datetime.now(UTC)).astimezone(UTC)
        if end_dt < start_dt:
            start_dt, end_dt = end_dt, start_dt
        payload = self.http_client.get_json(
            f"{self._history_base_url()}/{start_dt.date().isoformat()}..{end_dt.date().isoformat()}",
            params={"base": base, "symbols": quote},
            headers=self.source_config.headers,
        )
        rates = payload.get("rates", {})
        quotes: list[FXQuote] = []
        for date_text in sorted(rates):
            quote_payload = rates.get(date_text, {})
            rate = quote_payload.get(quote)
            if rate is None:
                continue
            quotes.append(
                FXQuote(
                    source_name=self.source_name,
                    pair=f"{base}/{quote}",
                    ts=datetime.fromisoformat(date_text).replace(tzinfo=UTC),
                    rate=float(rate),
                    raw_payload=str(quote_payload),
                )
            )
        return quotes

    def _history_base_url(self) -> str:
        configured = self.source_config.base_url.rstrip("/")
        parsed = urlparse(configured)
        if parsed.netloc == "api.frankfurter.app":
            return "https://api.frankfurter.dev/v1"
        return configured
