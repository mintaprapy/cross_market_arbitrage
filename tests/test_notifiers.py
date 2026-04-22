import unittest
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from cross_market_monitor.application.common import display_group_name
from cross_market_monitor.domain.models import AlertEvent, NotifierConfig
from cross_market_monitor.infrastructure.notifiers import ConsoleNotifier, alert_payload, human_notification_text


class NotifierFilterTests(unittest.TestCase):
    def test_notifier_filters_by_group_and_severity_only(self) -> None:
        notifier = ConsoleNotifier(
            NotifierConfig(
                name="console_filtered",
                kind="console",
                min_severity="warning",
                categories=["spread_level"],
                group_names=["AU_XAU"],
            )
        )

        matching_alert = AlertEvent(
            ts=datetime(2026, 3, 19, 0, 0, tzinfo=UTC),
            group_name="AU_XAU",
            category="spread_level",
            severity="warning",
            message="match",
            metadata={},
        )
        wrong_category = AlertEvent(
            ts=datetime(2026, 3, 19, 0, 0, tzinfo=UTC),
            group_name="AU_XAU",
            category="data_quality",
            severity="warning",
            message="skip-category",
            metadata={},
        )
        wrong_group = AlertEvent(
            ts=datetime(2026, 3, 19, 0, 0, tzinfo=UTC),
            group_name="SC_CL",
            category="spread_level",
            severity="warning",
            message="skip-group",
            metadata={},
        )

        self.assertTrue(notifier.should_send(matching_alert))
        self.assertTrue(notifier.should_send(wrong_category))
        self.assertFalse(notifier.should_send(wrong_group))

    def test_spread_notifications_use_plain_compact_message(self) -> None:
        alert = AlertEvent(
            ts=datetime(2026, 3, 19, 0, 0, tzinfo=UTC),
            group_name="AG_XAG",
            category="spread_pct",
            severity="warning",
            message="AG_XAG：6.90%  |  5.00\n中 17,728 | 换 77.21 | 外 72.21",
            metadata={},
        )

        self.assertEqual(
            human_notification_text(alert),
            "AG_XAG：6.90%  |  5.00\n中 17,728 | 换 77.21 | 外 72.21",
        )

    def test_display_group_name_formats_tax_variants(self) -> None:
        self.assertEqual(display_group_name("AG_XAG_GROSS"), "AG_XAG")
        self.assertEqual(display_group_name("AG_XAG_NET"), "AG_XAG除税")
        self.assertEqual(display_group_name("AU_XAU"), "AU_XAU")

    def test_data_quality_notification_uses_asia_shanghai_time(self) -> None:
        alert = AlertEvent(
            ts=datetime(2026, 4, 3, 3, 10, 57, 731783, tzinfo=UTC),
            group_name="CU_COPPER_GROSS",
            category="data_quality",
            severity="warning",
            message="CU_COPPER 数据状态异常：已过期",
            metadata={},
        )

        self.assertEqual(
            human_notification_text(alert),
            "[警告] CU_COPPER 数据质量\n"
            "CU_COPPER 数据状态异常：已过期\n"
            "2026-04-03T11:10:57.731783+08:00",
        )

    def test_alert_payload_includes_local_and_utc_timestamps(self) -> None:
        alert = AlertEvent(
            ts=datetime(2026, 4, 3, 3, 10, 57, 731783, tzinfo=UTC),
            group_name="BC_COPPER",
            category="data_quality",
            severity="warning",
            message="BC_COPPER 数据状态异常：已过期",
            metadata={},
        )

        payload = alert_payload(alert, ZoneInfo("Asia/Shanghai"))
        self.assertEqual(payload["timestamp"], "2026-04-03T11:10:57.731783+08:00")
        self.assertEqual(payload["timestamp_local"], "2026-04-03T11:10:57.731783+08:00")
        self.assertEqual(payload["timestamp_utc"], "2026-04-03T03:10:57.731783+00:00")
        self.assertEqual(payload["title"], "BC_COPPER 数据质量 警告")


if __name__ == "__main__":
    unittest.main()
