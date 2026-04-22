import asyncio
import unittest
from types import SimpleNamespace
from unittest import mock

from cross_market_monitor.application.monitor.telegram_command_service import (
    MENU_HELP,
    MENU_QUERY,
    TelegramCommandService,
)
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


def build_rows() -> dict[str, dict]:
    return {
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
        },
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


class TelegramCommandServiceTests(unittest.TestCase):
    def test_help_menu_returns_persistent_two_button_keyboard(self) -> None:
        context = build_context(
            notifiers=[],
            enabled_group_names=["AU_XAU"],
        )
        service = TelegramCommandService(context, FakeQuery(build_rows()))

        response = service._handle_text("/help")

        self.assertIn("跨市场交易查询", response.text)
        self.assertEqual(
            response.reply_markup["keyboard"],
            [[{"text": MENU_HELP}, {"text": MENU_QUERY}]],
        )

    def test_query_menu_returns_inline_buttons_for_enabled_pairs(self) -> None:
        context = build_context(
            notifiers=[],
            enabled_group_names=["AU_XAU", "CU_COPPER_GROSS", "CU_COPPER_NET"],
        )
        service = TelegramCommandService(context, FakeQuery(build_rows()))

        response = service._handle_text(MENU_QUERY)

        self.assertEqual(response.text, "请选择交易对：")
        buttons = response.reply_markup["inline_keyboard"]
        self.assertEqual(len(buttons), 1)
        self.assertEqual([item["text"] for item in buttons[0]], ["AU_XAU", "CU_COPPER", "CU_COPPER除税"])
        self.assertEqual([item["callback_data"] for item in buttons[0]], ["pair:AU_XAU", "pair:CU_COPPER_GROSS", "pair:CU_COPPER_NET"])

    def test_pair_callback_returns_snapshot_text(self) -> None:
        context = build_context(
            notifiers=[],
            enabled_group_names=["AU_XAU"],
        )
        service = TelegramCommandService(context, FakeQuery(build_rows()))

        response = service._handle_callback_data("pair:AU_XAU")

        self.assertIn("AU_XAU", response.text)
        self.assertIn("价差百分比: 1.00%", response.text)
        self.assertEqual(
            response.reply_markup["keyboard"],
            [[{"text": MENU_HELP}, {"text": MENU_QUERY}]],
        )

    def test_quote_command_supports_base_and_net_aliases(self) -> None:
        context = build_context(
            notifiers=[],
            enabled_group_names=["CU_COPPER_GROSS", "CU_COPPER_NET"],
        )
        service = TelegramCommandService(context, FakeQuery(build_rows()))

        gross_response = service._handle_text("/quote CU_COPPER")
        net_response = service._handle_text("/quote CU_COPPER除税")

        self.assertIn("CU_COPPER", gross_response.text)
        self.assertIn("价差百分比: 1.23%", gross_response.text)
        self.assertIn("Z-Score: 1.5000", gross_response.text)
        self.assertIn("CU_COPPER除税", net_response.text)
        self.assertIn("价差百分比: -1.00%", net_response.text)

    def test_poll_channel_once_handles_callback_query_for_configured_chat(self) -> None:
        FakeHttpClient.instances.clear()
        FakeHttpClient.next_response = {
            "ok": True,
            "result": [
                {
                    "update_id": 10,
                    "callback_query": {
                        "id": "cb1",
                        "data": "pair:AU_XAU",
                        "message": {"chat": {"id": 12345}},
                    },
                }
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
        service = TelegramCommandService(context, FakeQuery(build_rows()))

        with mock.patch(
            "cross_market_monitor.application.monitor.telegram_command_service.HttpClient",
            FakeHttpClient,
        ):
            service._poll_channel_once(service.channels[0])

        http = FakeHttpClient.instances[0]
        self.assertEqual(service.channels[0].update_offset, 11)
        self.assertEqual(len(http.post_requests), 3)
        self.assertIn("/bottoken/setMyCommands", http.post_requests[0][0])
        self.assertEqual(
            [item["command"] for item in http.post_requests[0][1]["commands"]],
            ["help", "query"],
        )
        self.assertIn("/bottoken/answerCallbackQuery", http.post_requests[1][0])
        self.assertEqual(http.post_requests[1][1]["callback_query_id"], "cb1")
        self.assertIn("/bottoken/sendMessage", http.post_requests[2][0])
        self.assertIn("AU_XAU", http.post_requests[2][1]["text"])

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
