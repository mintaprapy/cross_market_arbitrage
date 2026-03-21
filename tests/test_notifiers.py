import unittest
from datetime import UTC, datetime

from cross_market_monitor.application.common import display_group_name
from cross_market_monitor.domain.models import AlertEvent, NotifierConfig
from cross_market_monitor.infrastructure.notifiers import ConsoleNotifier, human_notification_text


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


if __name__ == "__main__":
    unittest.main()
