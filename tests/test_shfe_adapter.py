import unittest

from cross_market_monitor.domain.models import SourceConfig
from cross_market_monitor.infrastructure.marketdata.shfe import ShfeDelayMarketAdapter


class FakeHttpClient:
    def get_json(self, url: str, *, headers=None):
        self.last_url = url
        return {
            "delaymarket": [
                {
                    "contractname": "au2604",
                    "updatetime": "2026-03-13 11:30:00",
                    "lastprice": "1138.20",
                    "bidprice": "1138.20",
                    "askprice": "1138.36",
                    "volume": "147229",
                    "openinterest": "103714.00",
                },
                {
                    "contractname": "au2605",
                    "updatetime": "2026-03-13 11:30:00",
                    "lastprice": "1140.04",
                    "bidprice": "1139.00",
                    "askprice": "1140.18",
                    "volume": "416",
                    "openinterest": "1610.00",
                },
            ]
        }


class ShfeDelayMarketAdapterTests(unittest.TestCase):
    def test_fetch_quote_and_list_contracts(self) -> None:
        adapter = ShfeDelayMarketAdapter(
            "shfe_domestic",
            SourceConfig(kind="shfe_delaymarket", base_url="https://www.shfe.com.cn"),
            FakeHttpClient(),
        )

        quote = adapter.fetch_quote("au2604", "SHFE AU2604")
        contracts = adapter.list_contracts("au")

        self.assertEqual(quote.symbol, "au2604")
        self.assertEqual(quote.last, 1138.20)
        self.assertEqual(len(contracts), 2)
        self.assertEqual(contracts[0]["symbol"], "au2604")
        self.assertIn("OI 103714", contracts[0]["label"])


if __name__ == "__main__":
    unittest.main()
