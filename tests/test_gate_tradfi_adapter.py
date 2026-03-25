import json
import unittest
from datetime import UTC, datetime

from cross_market_monitor.domain.models import SourceConfig
from cross_market_monitor.infrastructure.marketdata.gate_tradfi import GateTradFiAdapter


class FakeGateTradFiHttpClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict | None]] = []

    def get_json(self, url: str, *, headers=None, params=None) -> dict:
        del headers
        self.calls.append((url, dict(params) if params is not None else None))
        if url.endswith("/tickers"):
            return {
                "timestamp": 1711929600000,
                "data": {
                    "last_price": "4327.4",
                    "bid_price": "4327.3",
                    "ask_price": "4327.5",
                },
            }
        if url.endswith("/klines"):
            return {
                "data": {
                    "list": [
                        {"t": 1711929600, "c": "10.5"},
                        {"t": 1711933200, "c": "11.5"},
                        {"t": 1711936800, "c": "12.5"},
                    ]
                }
            }
        raise AssertionError(f"Unexpected url: {url}")


class GateTradFiAdapterTests(unittest.TestCase):
    def test_fetch_quote_returns_last_bid_ask(self) -> None:
        adapter = GateTradFiAdapter(
            "gate_tradfi",
            SourceConfig(kind="gate_tradfi", base_url="https://api.gateio.ws"),
            FakeGateTradFiHttpClient(),
        )

        quote = adapter.fetch_quote("XAUUSD", "Gate TradFi XAUUSD")

        self.assertEqual(quote.symbol, "XAUUSD")
        self.assertEqual(quote.last, 4327.4)
        self.assertEqual(quote.bid, 4327.3)
        self.assertEqual(quote.ask, 4327.5)
        self.assertEqual(quote.ts, datetime(2024, 4, 1, 0, 0, tzinfo=UTC))

    def test_fetch_history_filters_window_and_sorts_rows(self) -> None:
        http_client = FakeGateTradFiHttpClient()
        adapter = GateTradFiAdapter(
            "gate_tradfi",
            SourceConfig(
                kind="gate_tradfi",
                base_url="https://api.gateio.ws",
                params={"history_limit": "5"},
            ),
            http_client,
        )

        rows = adapter.fetch_history(
            "XAUUSD",
            "Gate TradFi XAUUSD",
            interval="60m",
            start_ts=datetime(2024, 4, 1, 0, 0, tzinfo=UTC),
            end_ts=datetime(2024, 4, 1, 2, 0, tzinfo=UTC),
        )

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].ts, datetime(2024, 4, 1, 0, 0, tzinfo=UTC))
        self.assertEqual(rows[-1].last, 12.5)
        self.assertEqual(http_client.calls[-1][1], {"kline_type": "1h", "limit": "5"})

    def test_fetch_history_coarsens_interval_when_window_exceeds_limit(self) -> None:
        http_client = FakeGateTradFiHttpClient()
        adapter = GateTradFiAdapter(
            "gate_tradfi",
            SourceConfig(
                kind="gate_tradfi",
                base_url="https://api.gateio.ws",
                params={"history_limit": "100"},
            ),
            http_client,
        )

        adapter.fetch_history(
            "XAUUSD",
            "Gate TradFi XAUUSD",
            interval="15m",
            start_ts=datetime(2024, 4, 1, 0, 0, tzinfo=UTC),
            end_ts=datetime(2024, 4, 7, 23, 59, tzinfo=UTC),
        )

        self.assertEqual(http_client.calls[-1][1], {"kline_type": "4h", "limit": "100"})


if __name__ == "__main__":
    unittest.main()
