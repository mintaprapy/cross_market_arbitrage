import json
import tarfile
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from cross_market_monitor.tools import tqsdk_weekly_report


class TqSdkWeeklyReportTests(unittest.TestCase):
    def _write_report(self, root: Path, name: str, payload: dict) -> Path:
        path = root / name
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def test_aggregate_reports_marks_stable_when_thresholds_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            input_dir = Path(tmp)
            self._write_report(
                input_dir,
                "tqsdk_connectivity_20260328_010000.json",
                {
                    "started_at": "2026-03-28T01:00:00+00:00",
                    "ended_at": "2026-03-28T02:00:00+00:00",
                    "connect_success": True,
                    "refresh_update_ratio": 0.998,
                    "refresh_latency": {"median_ms": 220.5, "p95_ms": 450.0, "max_ms": 1200.0},
                    "symbols": {
                        code: {
                            "success_rate": 1.0,
                            "in_session_fresh_rate": 1.0,
                            "stale_in_session_count": 0,
                            "in_session_cycles": 100,
                            "out_of_session_cycles": 0,
                            "max_age_in_session_sec": 0.8,
                            "resolved_symbols": [f"KQ.m@{code}"],
                        }
                        for code in ("au", "ag", "cu", "bc", "sc")
                    },
                },
            )

            reports = tqsdk_weekly_report.load_recent_reports(input_dir, days=7, now=datetime(2026, 3, 28, 3, tzinfo=UTC))
            summary = tqsdk_weekly_report.aggregate_reports(
                reports,
                days=7,
                min_connect_success_rate=0.99,
                min_in_session_fresh_rate=0.99,
                max_refresh_latency_median_ms=None,
                exclude_windows=["19:00-19:30"],
                exclude_timezone="Asia/Hong_Kong",
                excluded_reports=[],
            )

            self.assertTrue(summary["is_stable"])
            self.assertEqual(summary["report_count"], 1)
            self.assertEqual(summary["overall"]["connect_success_rate"], 1.0)
            self.assertEqual(summary["symbols"]["au"]["total_stale_in_session"], 0)
            self.assertEqual(summary["breaches"], [])

    def test_aggregate_reports_marks_unstable_when_thresholds_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            input_dir = Path(tmp)
            self._write_report(
                input_dir,
                "tqsdk_connectivity_20260328_010000.json",
                {
                    "started_at": "2026-03-28T01:00:00+00:00",
                    "ended_at": "2026-03-28T02:00:00+00:00",
                    "connect_success": False,
                    "setup_error": {"error": "timeout"},
                    "refresh_update_ratio": 0.85,
                    "refresh_latency": {"median_ms": 1500.0, "p95_ms": 2200.0, "max_ms": 5000.0},
                    "symbols": {
                        "au": {
                            "success_rate": 0.9,
                            "in_session_fresh_rate": 0.8,
                            "stale_in_session_count": 12,
                            "in_session_cycles": 60,
                            "out_of_session_cycles": 0,
                            "max_age_in_session_sec": 25.0,
                            "resolved_symbols": ["KQ.m@SHFE.au"],
                        }
                    },
                },
            )

            reports = tqsdk_weekly_report.load_recent_reports(input_dir, days=7, now=datetime(2026, 3, 28, 3, tzinfo=UTC))
            summary = tqsdk_weekly_report.aggregate_reports(
                reports,
                days=7,
                min_connect_success_rate=0.99,
                min_in_session_fresh_rate=0.99,
                max_refresh_latency_median_ms=None,
                exclude_windows=["19:00-19:30"],
                exclude_timezone="Asia/Hong_Kong",
                excluded_reports=[],
            )

            self.assertFalse(summary["is_stable"])
            self.assertGreaterEqual(len(summary["breaches"]), 2)
            self.assertEqual(summary["connection_failures"][0]["setup_error"], {"error": "timeout"})

    def test_split_excluded_reports_removes_maintenance_window_samples(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            input_dir = Path(tmp)
            self._write_report(
                input_dir,
                "tqsdk_connectivity_20260328_110000.json",
                {
                    "started_at": "2026-03-28T10:30:00+00:00",
                    "ended_at": "2026-03-28T11:05:00+00:00",
                    "connect_success": False,
                    "refresh_update_ratio": 0.0,
                    "refresh_latency": {"median_ms": None, "p95_ms": None, "max_ms": None},
                    "symbols": {},
                },
            )
            self._write_report(
                input_dir,
                "tqsdk_connectivity_20260328_120000.json",
                {
                    "started_at": "2026-03-28T12:00:00+00:00",
                    "ended_at": "2026-03-28T13:00:00+00:00",
                    "connect_success": True,
                    "refresh_update_ratio": 1.0,
                    "refresh_latency": {"median_ms": 100.0, "p95_ms": 120.0, "max_ms": 150.0},
                    "symbols": {},
                },
            )

            reports = tqsdk_weekly_report.load_recent_reports(input_dir, days=7, now=datetime(2026, 3, 28, 13, tzinfo=UTC))
            included, excluded = tqsdk_weekly_report.split_excluded_reports(
                reports,
                exclude_windows=["19:00-19:30"],
                exclude_timezone="Asia/Hong_Kong",
            )

            self.assertEqual(len(included), 1)
            self.assertEqual(len(excluded), 1)
            self.assertEqual(excluded[0]["_excluded_window"], "19:00-19:30")

    def test_export_bundle_writes_report_summary_and_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()

            report_path = self._write_report(
                input_dir,
                "tqsdk_connectivity_20260328_010000.json",
                {
                    "started_at": "2026-03-28T01:00:00+00:00",
                    "ended_at": "2026-03-28T02:00:00+00:00",
                    "connect_success": True,
                    "refresh_update_ratio": 1.0,
                    "refresh_latency": {"median_ms": 100.0, "p95_ms": 150.0, "max_ms": 300.0},
                    "symbols": {},
                },
            )

            reports = tqsdk_weekly_report.load_recent_reports(input_dir, days=7, now=datetime(2026, 3, 28, 3, tzinfo=UTC))
            summary = tqsdk_weekly_report.aggregate_reports(
                reports,
                days=7,
                min_connect_success_rate=0.99,
                min_in_session_fresh_rate=0.99,
                max_refresh_latency_median_ms=None,
                exclude_windows=["19:00-19:30"],
                exclude_timezone="Asia/Hong_Kong",
                excluded_reports=[],
            )
            bundle_dir, archive_path = tqsdk_weekly_report.export_bundle(summary, reports=reports, output_root=output_dir)

            self.assertTrue((bundle_dir / "summary.json").exists())
            self.assertTrue((bundle_dir / "REPORT.md").exists())
            self.assertTrue((bundle_dir / "source_reports" / report_path.name).exists())
            self.assertTrue(archive_path.exists())
            with tarfile.open(archive_path, "r:gz") as tar:
                self.assertTrue(any(member.name.endswith("REPORT.md") for member in tar.getmembers()))


if __name__ == "__main__":
    unittest.main()
