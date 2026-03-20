import unittest
from datetime import UTC, datetime

from cross_market_monitor.domain.models import AlertEvent, NotifierConfig
from cross_market_monitor.infrastructure.notifiers import ConsoleNotifier


class NotifierFilterTests(unittest.TestCase):
    def test_notifier_filters_by_category_and_group(self) -> None:
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
        self.assertFalse(notifier.should_send(wrong_category))
        self.assertFalse(notifier.should_send(wrong_group))


if __name__ == "__main__":
    unittest.main()
