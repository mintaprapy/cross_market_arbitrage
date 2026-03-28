import unittest
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from cross_market_monitor.application.common import active_trading_session_window
from cross_market_monitor.tools.tqsdk_connectivity_check import _effective_age_sec


class TqSdkConnectivityCheckTests(unittest.TestCase):
    def test_active_trading_session_window_returns_current_session_bounds(self) -> None:
        local_tz = ZoneInfo("Asia/Hong_Kong")
        local_dt = datetime(2026, 3, 30, 9, 5, tzinfo=local_tz)
        window = active_trading_session_window(
            local_dt,
            ["09:00-10:15", "10:30-11:30", "13:30-15:00", "21:00-02:30"],
            non_trading_dates=[],
            weekends_closed=True,
        )

        self.assertIsNotNone(window)
        assert window is not None
        self.assertEqual(window[0], datetime(2026, 3, 30, 9, 0, tzinfo=local_tz))
        self.assertEqual(window[1], datetime(2026, 3, 30, 10, 15, tzinfo=local_tz))

    def test_active_trading_session_window_honors_lead_time(self) -> None:
        local_tz = ZoneInfo("Asia/Hong_Kong")
        local_dt = datetime(2026, 3, 30, 8, 56, tzinfo=local_tz)
        window = active_trading_session_window(
            local_dt,
            ["09:00-10:15"],
            lead_sec=300,
            non_trading_dates=[],
            weekends_closed=True,
        )

        self.assertIsNotNone(window)
        assert window is not None
        self.assertEqual(window[0], datetime(2026, 3, 30, 9, 0, tzinfo=local_tz))
        self.assertEqual(window[1], datetime(2026, 3, 30, 10, 15, tzinfo=local_tz))

    def test_effective_age_anchors_to_session_start_when_quote_is_from_previous_session(self) -> None:
        now_utc = datetime(2026, 3, 30, 1, 1, tzinfo=UTC)  # 09:01 HKT
        previous_quote_utc = datetime(2026, 3, 29, 18, 30, tzinfo=UTC)  # 02:30 HKT previous session close
        session_start_utc = datetime(2026, 3, 30, 1, 0, tzinfo=UTC)  # 09:00 HKT

        age_sec = _effective_age_sec(previous_quote_utc, now=now_utc, session_start_utc=session_start_utc)

        self.assertEqual(age_sec, 60.0)

    def test_effective_age_uses_quote_time_once_quote_is_inside_current_session(self) -> None:
        now_utc = datetime(2026, 3, 30, 1, 5, tzinfo=UTC)
        quote_utc = datetime(2026, 3, 30, 1, 4, 30, tzinfo=UTC)
        session_start_utc = datetime(2026, 3, 30, 1, 0, tzinfo=UTC)

        age_sec = _effective_age_sec(quote_utc, now=now_utc, session_start_utc=session_start_utc)

        self.assertEqual(age_sec, 30.0)


if __name__ == "__main__":
    unittest.main()
