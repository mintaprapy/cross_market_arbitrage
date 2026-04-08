import asyncio
import unittest
from types import SimpleNamespace
from unittest import mock

from cross_market_monitor.application.monitor.telegram_command_service import TelegramCommandService
from cross_market_monitor.domain.models import NotifierConfig


class FakeQuery:
    def __init__(self, rows: dict[str, dict]) -> None:
        self.rows = rows

    def get_snapshot_row(self, group_name: str) -> dict | None:
        return self.rows.get(group_name)


class FakeHttpClient:
    instances: list["FakeHttpClient"] = []
    next_response: dict = {"ok": True, "result": []}

    def __init__(self, timeout_sec: int = 8) -> None:
        self.timeout_sec = timeout_sec
        self.get_requests: list[tuple[str, dict | None]] = []
        self.post_requests: list[tuple[str, dict]] = []
        self.response: dict = dict(FakeHttpClient.next_response)
        FakeHttpClient.instances.append(self)

    def get_json(self, url: str, *, headers=None, params=None) -> dict:
        self.get_requests.append((url, params))
        return self.response

    def post_json(self, url: str, payload: dict, *, headers=None) -> str:
        self.post_requests.append((url, payload))
        return "ok"


def build_context(*, notifiers: list[NotifierConfig], enabled_group_names: list[str]) -> SimpleNamespace:
    enabled_pairs = [SimpleNamespace(group_name=group_name) for group_name in enabled_group_names]
    pair_map = {pair.group_name: pair for pair in enabled_pairs}
    return SimpleNamespace(
        config=SimpleNamespace(
            app=SimpleNamespace(timezone="Asia/Shanghai"),
            notifiers=notifiers,
        ),
        enabled_pairs=enabled_pairs,
        pair_map=pair_map,
        stop_event=asyncio.Event(),
    )


class TelegramCommandServiceTests(unittest.TestCase):
    def test_quote_command_supports_base_and_net_aliases(self) -> None:
        context = build_context(
            notifiers=[],
            enabled_group_names=["CU_COPPER_GROSS", "CU_COPPER_NET"],
        )
        query = FakeQuery(
            {
                "CU_COPPER_GROSS": {
                    "group_name": "CU_COPPER_GROSS",
                    "status": "ok",
                    "spread_pct": 0.0123,
                    "spread": 1.2345,
                    "zscore": 1.5,
                    "domestic_last_raw": 81234.0,
                    "normalized_last": 11.2233,
                    "overseas_last": 10.9988,
                    "fx_rate": 7.1234,
                    "domestic_age_sec": 5.0,
                    "overseas_age_sec": 3.0,
                    "fx_age_sec": 12.0,
                    "ts_local": "2026-04-07T21:05:06+08:00",
                    "commodity_spec": {"normalized_unit_label": "USD/lb"},
                    "target_unit": "USD_PER_POUND",
                },
                "CU_COPPER_NET": {
                    "group_name": "CU_COPPER_NET",
                    "status": "ok",
                    "spread_pct": -0.01,
                    "spread": -0.5,
                    "zscore": -1.2,
                    "domestic_last_raw": 81234.0,
                    "normalized_last": 10.2233,
                    "overseas_last": 10.9988,
                    "fx_rate": 7.1234,
                    "domestic_age_sec": 5.0,
                    "overseas_age_sec": 3.0,
                    "fx_age_sec": 12.0,
                    "ts_local": "2026-04-07T21:05:06+08:00",
                    "commodity_spec": {"normalized_unit_label": "USD/lb"},
                    "target_unit": "USD_PER_POUND",
                },
            }
        )

        service = TelegramCommandService(context, query)

        gross_text = service._handle_text("/quote CU_COPPER")
        net_text = service._handle_text("/quote CU_COPPER除税")

        self.assertIn("CU_COPPER", gross_text)
        self.assertIn("价差百分比: 1.23%", gross_text)
        self.assertIn("Z-Score: 1.5000", gross_text)
        self.assertIn("CU_COPPER除税", net_text)
        self.assertIn("价差百分比: -1.00%", net_text)

    def test_poll_channel_once_replies_only_to_configured_chat(self) -> None:
        FakeHttpClient.instances.clear()
        FakeHttpClient.next_response = {
            "ok": True,
            "result": [
                {
                    "update_id": 10,
                    "message": {
                        "chat": {"id": 12345},
                        "text": "/quote AU_XAU",
                    },
                },
                {
                    "update_id": 11,
                    "message": {
                        "chat": {"id": 99999},
                        "text": "/quote AU_XAU",
                    },
                },
            ],
        }
        notifier = NotifierConfig(
            name="telegram_alerts",
            kind="telegram",
            enabled=True,
            bot_token="token",
            chat_id="12345",
            timeout_sec=5,
        )
        context = build_context(
            notifiers=[notifier],
            enabled_group_names=["AU_XAU"],
        )
        query = FakeQuery(
            {
                "AU_XAU": {
                    "group_name": "AU_XAU",
                    "status": "ok",
                    "spread_pct": 0.01,
                    "spread": 1.0,
                    "zscore": 2.0,
                    "domestic_last_raw": 100.0,
                    "normalized_last": 101.0,
                    "overseas_last": 100.0,
                    "fx_rate": 7.2,
                    "domestic_age_sec": 1.0,
                    "overseas_age_sec": 1.0,
                    "fx_age_sec": 60.0,
                    "ts_local": "2026-04-07T21:05:06+08:00",
                    "commodity_spec": {"normalized_unit_label": "USD/oz"},
                    "target_unit": "USD_PER_OUNCE",
                }
            }
        )
        service = TelegramCommandService(context, query)

        with mock.patch(
            "cross_market_monitor.application.monitor.telegram_command_service.HttpClient",
            FakeHttpClient,
        ):
            service._poll_channel_once(service.channels[0])

        self.assertEqual(len(FakeHttpClient.instances), 1)
        http = FakeHttpClient.instances[0]
        self.assertEqual(service.channels[0].update_offset, 12)
        self.assertEqual(len(http.post_requests), 2)
        menu_url, menu_payload = http.post_requests[0]
        self.assertIn("/bottoken/setMyCommands", menu_url)
        self.assertEqual(
            [item["command"] for item in menu_payload["commands"]],
            ["help", "pairs", "quote", "status"],
        )
        url, payload = http.post_requests[1]
        self.assertIn("/bottoken/sendMessage", url)
        self.assertEqual(payload["chat_id"], "12345")
        self.assertIn("AU_XAU", payload["text"])

    def test_registers_menu_only_once_per_channel(self) -> None:
        FakeHttpClient.instances.clear()
        FakeHttpClient.next_response = {"ok": True, "result": []}
        notifier = NotifierConfig(
            name="telegram_alerts",
            kind="telegram",
            enabled=True,
            bot_token="token",
            chat_id="12345",
            timeout_sec=5,
        )
        context = build_context(
            notifiers=[notifier],
            enabled_group_names=["AU_XAU"],
        )
        service = TelegramCommandService(context, FakeQuery({}))

        with mock.patch(
            "cross_market_monitor.application.monitor.telegram_command_service.HttpClient",
            FakeHttpClient,
        ):
            service._poll_channel_once(service.channels[0])
            service._poll_channel_once(service.channels[0])

        http_first = FakeHttpClient.instances[0]
        http_second = FakeHttpClient.instances[1]
        self.assertEqual(len(http_first.post_requests), 1)
        self.assertIn("/bottoken/setMyCommands", http_first.post_requests[0][0])
        self.assertEqual(len(http_second.post_requests), 0)


if __name__ == "__main__":
    unittest.main()
