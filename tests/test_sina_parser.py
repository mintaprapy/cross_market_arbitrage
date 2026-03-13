import unittest

from cross_market_monitor.infrastructure.marketdata.sina import parse_sina_futures_payload


class SinaParserTests(unittest.TestCase):
    def test_parse_sina_payload_extracts_prices(self) -> None:
        payload = (
            'var hq_str_nf_AU0="沪金连续,110548,1152.020,1155.100,1140.340,0.000,1141.880,1142.000,1142.000,0.000,1151.560,1,2,142000.000,65238,日,沪金,2026-03-13,1,,,,,,,,,1146.513,0.000,0";'
        )
        quote = parse_sina_futures_payload("sina_domestic", "nf_AU0", "SHFE AU Continuous", payload)
        self.assertEqual(quote.symbol, "nf_AU0")
        self.assertEqual(quote.last, 1142.0)
        self.assertEqual(quote.bid, 1141.88)
        self.assertEqual(quote.ask, 1142.0)


if __name__ == "__main__":
    unittest.main()
