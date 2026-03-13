import unittest

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


if __name__ == "__main__":
    unittest.main()
