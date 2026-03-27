import unittest
from datetime import UTC, datetime, timedelta

from cross_market_monitor.domain.stats import RollingWindow, mean, std_dev


class StatsTests(unittest.TestCase):
    def test_mean_and_std_dev(self) -> None:
        values = [1.0, 2.0, 3.0, 4.0]
        self.assertEqual(mean(values), 2.5)
        self.assertEqual(round(std_dev(values) or 0, 6), 1.118034)

    def test_rolling_window_summary_before_append(self) -> None:
        window = RollingWindow(5, seed=[1.0, 2.0, 3.0])
        avg, sd, zscore, delta = window.summary(4.0)
        self.assertEqual(avg, 2.0)
        self.assertEqual(round(sd or 0, 6), 0.816497)
        self.assertEqual(round(zscore or 0, 6), 2.44949)
        self.assertEqual(delta, 1.0)

    def test_rolling_window_prunes_points_older_than_max_age(self) -> None:
        now = datetime.now(UTC)
        window = RollingWindow(
            None,
            max_age=timedelta(days=30),
            seed_points=[
                (now - timedelta(days=31), 1.0),
                (now - timedelta(days=5), 2.0),
                (now - timedelta(days=1), 3.0),
            ],
        )

        avg, sd, zscore, delta = window.summary(4.0, current_ts=now)

        self.assertEqual(window.values(as_of=now), [2.0, 3.0])
        self.assertEqual(avg, 2.5)
        self.assertEqual(round(sd or 0, 6), 0.5)
        self.assertEqual(round(zscore or 0, 6), 3.0)
        self.assertEqual(delta, 1.0)

    def test_rolling_window_keeps_one_point_per_15m_bucket(self) -> None:
        now = datetime(2026, 3, 27, 1, 1, tzinfo=UTC)
        window = RollingWindow(
            None,
            bucket_size=timedelta(minutes=15),
        )

        window.append(1.0, ts=now)
        window.append(2.0, ts=now + timedelta(minutes=10))
        window.append(3.0, ts=now + timedelta(minutes=16))

        self.assertEqual(window.values(as_of=now + timedelta(minutes=16)), [2.0, 3.0])


if __name__ == "__main__":
    unittest.main()
