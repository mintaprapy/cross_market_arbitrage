import tempfile
import unittest
from datetime import UTC, datetime, timedelta

from cross_market_monitor.application.replay import ReplayAnalyzer
from cross_market_monitor.domain.models import PairConfig, SpreadSnapshot
from cross_market_monitor.infrastructure.repository import SQLiteRepository


class ReplayAnalyzerTests(unittest.TestCase):
    def test_replay_report_counts_breaches_and_convergence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            pair = PairConfig(
                group_name="AU_XAU",
                domestic_source="domestic",
                domestic_symbol="AU",
                domestic_label="AU",
                overseas_source="overseas",
                overseas_symbol="XAU",
                overseas_label="XAU",
                formula="gold",
                domestic_unit="CNY_PER_GRAM",
                target_unit="USD_PER_OUNCE",
                thresholds={
                    "spread_pct_alert_above": 0.02,
                    "zscore_alert_above": 2.5,
                },
            )

            base = datetime(2026, 3, 13, 0, 0, tzinfo=UTC)
            spreads = [5.0, 3.0, 4.0, 1.0]
            spread_pcts = [0.03, 0.01, 0.025, 0.005]
            zscores = [3.1, 0.5, 2.8, 0.1]
            for index, spread in enumerate(spreads):
                repository.insert_snapshot(
                    SpreadSnapshot(
                        ts=base + timedelta(minutes=index * 15),
                        group_name="AU_XAU",
                        domestic_symbol="AU",
                        overseas_symbol="XAU",
                        fx_source="fx",
                        fx_rate=6.9,
                        formula="gold",
                        formula_version="v1",
                        tax_mode="gross",
                        target_unit="USD_PER_OUNCE",
                        status="ok",
                        normalized_last=100.0 + index,
                        overseas_last=100.0 + spread,
                        spread=spread,
                        spread_pct=spread_pcts[index],
                        zscore=zscores[index],
                    )
                )

            analyzer = ReplayAnalyzer(repository, [pair])
            report = analyzer.analyze("AU_XAU", limit=100)

            self.assertEqual(report["sample_count"], 4)
            self.assertAlmostEqual(report["spread_pct_mean"], 0.0175)
            self.assertAlmostEqual(report["spread_pct_std"], 0.010307764064044151)
            self.assertAlmostEqual(report["spread_pct_median"], 0.0175)
            self.assertAlmostEqual(report["latest_spread_pct_percentile"], 0.125)
            self.assertEqual(report["spread_pct_breach_count"], 2)
            self.assertEqual(report["zscore_breach_count"], 2)
            self.assertEqual(report["convergence_count"], 2)
            self.assertEqual(report["divergence_count"], 1)
            self.assertEqual(len(report["signal_entries"]), 4)
            self.assertIsNotNone(report["average_round_trip_cost"])
            self.assertIsNotNone(report["realized_daily_vol_pct"])

    def test_replay_report_uses_latest_snapshot_per_15m_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            pair = PairConfig(
                group_name="AU_XAU",
                domestic_source="domestic",
                domestic_symbol="AU",
                domestic_label="AU",
                overseas_source="overseas",
                overseas_symbol="XAU",
                overseas_label="XAU",
                formula="gold",
                domestic_unit="CNY_PER_GRAM",
                target_unit="USD_PER_OUNCE",
            )

            base = datetime(2026, 3, 13, 0, 0, tzinfo=UTC)
            rows = [
                (0, 0.010, 1.0),
                (5, 0.020, 2.0),
                (10, 0.030, 3.0),
                (17, 0.040, 4.0),
            ]
            for minute, spread_pct, spread in rows:
                repository.insert_snapshot(
                    SpreadSnapshot(
                        ts=base + timedelta(minutes=minute),
                        group_name="AU_XAU",
                        domestic_symbol="AU",
                        overseas_symbol="XAU",
                        fx_source="fx",
                        fx_rate=6.9,
                        formula="gold",
                        formula_version="v1",
                        tax_mode="gross",
                        target_unit="USD_PER_OUNCE",
                        status="ok",
                        normalized_last=100.0,
                        overseas_last=100.0 + spread,
                        spread=spread,
                        spread_pct=spread_pct,
                        zscore=spread,
                    )
                )

            analyzer = ReplayAnalyzer(repository, [pair], bucket_minutes=15)
            report = analyzer.analyze("AU_XAU", limit=100)

            self.assertEqual(report["sample_count"], 2)
            self.assertAlmostEqual(report["latest_spread_pct"], 0.040)
            self.assertAlmostEqual(report["spread_pct_mean"], 0.035)


if __name__ == "__main__":
    unittest.main()
