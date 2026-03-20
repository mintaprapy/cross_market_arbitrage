import json
import unittest
from datetime import UTC, datetime

from cross_market_monitor.domain.models import SourceConfig
from cross_market_monitor.infrastructure.marketdata.binance import BinanceFuturesAdapter
from cross_market_monitor.infrastructure.marketdata.okx import OkxSwapAdapter


class FakeBinanceHttpClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def get_text(self, url: str, *, headers=None, params=None) -> str:
        del url, headers
        assert params is not None
        self.calls.append(dict(params))
        start = params.get("startTime")
        if start == "1711929600000":
            payload = [
                [1711929600000, "10", "11", "9", "10.5"],
                [1711933200000, "11", "12", "10", "11.5"],
            ]
        elif start == "1711936800000":
            payload = [
                [1711936800000, "12", "13", "11", "12.5"],
            ]
        else:
            payload = []
        return json.dumps(payload)


class FakeOkxHttpClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def get_json(self, url: str, *, headers=None, params=None) -> dict:
        del url, headers
        assert params is not None
        self.calls.append(dict(params))
        after = params.get("after")
        if after is None:
            data = [
                ["1711936800000", "12", "13", "11", "12.5"],
                ["1711933200000", "11", "12", "10", "11.5"],
            ]
        elif after == "1711933200000":
            data = [
                ["1711929600000", "10", "11", "9", "10.5"],
            ]
        else:
            data = []
        return {"code": "0", "data": data}


class OverseasHistoryAdapterTests(unittest.TestCase):
    def test_binance_fetch_history_paginates_forward_and_returns_sorted_rows(self) -> None:
        adapter = BinanceFuturesAdapter(
            "binance_futures",
            SourceConfig(
                kind="binance_futures",
                base_url="https://fapi.binance.com",
                params={"history_page_limit": "2", "history_max_pages": "3"},
            ),
            FakeBinanceHttpClient(),
        )

        rows = adapter.fetch_history(
            "XAUUSDT",
            "Binance XAU",
            interval="60m",
            start_ts=datetime(2024, 4, 1, 0, 0, tzinfo=UTC),
            end_ts=datetime(2024, 4, 1, 2, 0, tzinfo=UTC),
        )

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].ts, datetime(2024, 4, 1, 0, 0, tzinfo=UTC))
        self.assertEqual(rows[-1].last, 12.5)

    def test_okx_fetch_history_paginates_backward_and_returns_sorted_rows(self) -> None:
        adapter = OkxSwapAdapter(
            "okx_swap",
            SourceConfig(
                kind="okx_swap",
                base_url="https://www.okx.com",
                params={"history_page_limit": "2", "history_max_pages": "3"},
            ),
            FakeOkxHttpClient(),
        )

        rows = adapter.fetch_history(
            "XAU-USDT-SWAP",
            "OKX XAU",
            interval="60m",
            start_ts=datetime(2024, 4, 1, 0, 0, tzinfo=UTC),
            end_ts=datetime(2024, 4, 1, 2, 0, tzinfo=UTC),
        )

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].ts, datetime(2024, 4, 1, 0, 0, tzinfo=UTC))
        self.assertEqual(rows[-1].ts, datetime(2024, 4, 1, 2, 0, tzinfo=UTC))
        self.assertEqual(rows[1].last, 11.5)


if __name__ == "__main__":
    unittest.main()
