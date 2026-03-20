import unittest
from datetime import UTC, datetime

from cross_market_monitor.domain.models import SourceConfig
from cross_market_monitor.infrastructure.marketdata.sina import SinaFxAdapter, parse_sina_fx_payload


class FakeHttpClient:
    def __init__(self, payload: str) -> None:
        self.payload = payload
        self.last_url: str | None = None
        self.last_headers: dict[str, str] | None = None

    def get_text(self, url: str, headers: dict[str, str] | None = None) -> str:
        self.last_url = url
        self.last_headers = headers or {}
        return self.payload


class SinaFxAdapterTests(unittest.TestCase):
    def test_parse_sina_fx_payload_uses_latest_rate_and_timestamp(self) -> None:
        payload = (
            'var hq_str_fx_susdcny="02:55:30,6.8993000000,6.9003000000,6.8998000000,310.0000000000,'
            '6.8721000000,6.9024000000,6.8714000000,6.8998000000,在岸人民币,0.0000,0.0000,0.0310,'
            '此行情由新浪财经计算得出,0.0000,0.0000,,2026-03-20";'
        )
        quote = parse_sina_fx_payload("sina_fx", "USD/CNY", payload)

        self.assertEqual(quote.source_name, "sina_fx")
        self.assertEqual(quote.pair, "USD/CNY")
        self.assertEqual(quote.rate, 6.8998)
        self.assertEqual(quote.ts, datetime(2026, 3, 19, 18, 55, 30, tzinfo=UTC))

    def test_adapter_uses_configured_symbol(self) -> None:
        payload = 'var hq_str_fx_susdcny="08:50:40,6.883000,6.883500,6.878300,98,6.879400,6.884200,6.874400,6.883000,离岸人民币（香港）,, ,,, ,,,2026-03-20";'
        http_client = FakeHttpClient(payload)
        source_config = SourceConfig.model_validate(
            {
                "kind": "sina_fx",
                "base_url": "https://hq.sinajs.cn",
                "headers": {"Referer": "https://finance.sina.com.cn"},
                "params": {"symbol": "fx_susdcny"},
                "verify_ssl": True,
            }
        )

        adapter = SinaFxAdapter("sina_fx", source_config, http_client)
        quote = adapter.fetch_rate("USD", "CNY")

        self.assertEqual(http_client.last_url, "https://hq.sinajs.cn/list=fx_susdcny")
        self.assertEqual(quote.pair, "USD/CNY")
        self.assertEqual(quote.rate, 6.883)


if __name__ == "__main__":
    unittest.main()
