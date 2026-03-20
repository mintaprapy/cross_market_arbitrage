import unittest
from datetime import UTC, datetime

from cross_market_monitor.infrastructure.marketdata.sina import parse_sina_futures_payload, parse_sina_history_payload


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

    def test_parse_sina_history_payload_orders_and_filters_rows(self) -> None:
        payload = (
            '[["2026-03-13 09:10:00","540.000","541.000","539.000","540.500","1200"],'
            '["2026-03-13 09:05:00","539.500","540.200","539.100","540.000","1100"]]'
        )
        rows = parse_sina_history_payload(
            "sina_domestic",
            "nf_AU0",
            "SHFE AU Continuous",
            payload,
            interval="5m",
            start_ts=datetime(2026, 3, 13, 1, 6, tzinfo=UTC),
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].symbol, "nf_AU0")
        self.assertEqual(rows[0].last, 540.5)
        self.assertEqual(rows[0].ts, datetime(2026, 3, 13, 1, 10, tzinfo=UTC))


if __name__ == "__main__":
    unittest.main()
