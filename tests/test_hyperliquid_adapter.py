import json
import unittest
from datetime import UTC, datetime

from cross_market_monitor.domain.models import SourceConfig
from cross_market_monitor.infrastructure.marketdata.hyperliquid import HyperliquidAdapter


class FakeHyperliquidHttpClient:
    def __init__(self) -> None:
        self.payloads: list[dict] = []

    def post_json(self, url: str, payload: dict, *, headers=None) -> str:
        del url, headers
        self.payloads.append(dict(payload))
        if payload["type"] == "metaAndAssetCtxs":
            return json.dumps(
                [
                    {"universe": [{"name": "xyz:GOLD"}, {"name": "xyz:CL"}]},
                    [{"markPx": "4327.4"}, {"markPx": "91.63"}],
                ]
            )
        if payload == {"type": "l2Book", "coin": "xyz:GOLD"}:
            return json.dumps(
                {
                    "coin": "xyz:GOLD",
                    "time": 1711929600000,
                    "levels": [
                        [{"px": "4327.3"}],
                        [{"px": "4327.5"}],
                    ],
                }
            )
        raise AssertionError(f"Unexpected payload: {payload}")


class HyperliquidAdapterTests(unittest.TestCase):
    def test_fetch_quote_supports_xyz_dex_symbols(self) -> None:
        adapter = HyperliquidAdapter(
            "hyperliquid_xyz",
            SourceConfig(
                kind="hyperliquid",
                base_url="https://api.hyperliquid.xyz",
                params={"dex": "xyz"},
            ),
            FakeHyperliquidHttpClient(),
        )

        quote = adapter.fetch_quote("xyz:GOLD", "Hyperliquid XYZ GOLD")

        self.assertEqual(quote.symbol, "xyz:GOLD")
        self.assertEqual(quote.last, 4327.4)
        self.assertEqual(quote.bid, 4327.3)
        self.assertEqual(quote.ask, 4327.5)
        self.assertEqual(quote.ts, datetime(2024, 4, 1, 0, 0, tzinfo=UTC))


if __name__ == "__main__":
    unittest.main()
