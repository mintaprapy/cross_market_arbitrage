import csv
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

from cross_market_monitor.domain.models import FXQuote, MarketQuote, SourceHealth, SpreadSnapshot, WorkerRuntimeState
from cross_market_monitor.infrastructure.repository import SQLiteRepository
from cross_market_monitor.infrastructure.storage.sqlite_base import SQLiteRepositoryBase

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - optional dependency for local test env
    pq = None


class RepositoryTests(unittest.TestCase):
    def test_persists_runtime_state_and_source_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            repository.upsert_runtime_state(
                WorkerRuntimeState(
                    started_at=datetime(2026, 3, 13, 0, 0, tzinfo=UTC),
                    last_poll_started_at=datetime(2026, 3, 13, 0, 1, tzinfo=UTC),
                    last_poll_finished_at=datetime(2026, 3, 13, 0, 2, tzinfo=UTC),
                    last_heartbeat_at=datetime(2026, 3, 13, 0, 2, 5, tzinfo=UTC),
                    total_cycles=12,
                    latest_fx_rate=6.9,
                    latest_fx_source="frankfurter",
                    latest_fx_jump_pct=0.001,
                    fx_is_live=False,
                    fx_is_frozen=True,
                    fx_last_live_at=datetime(2026, 3, 13, 0, 1, 58, tzinfo=UTC),
                    fx_frozen_since=datetime(2026, 3, 13, 0, 2, tzinfo=UTC),
                )
            )
            repository.upsert_source_health(
                SourceHealth(
                    source_name="binance_futures",
                    kind="binance_futures",
                    success_count=4,
                    failure_count=1,
                    last_success_at=datetime(2026, 3, 13, 0, 2, tzinfo=UTC),
                    last_failure_at=datetime(2026, 3, 13, 0, 1, tzinfo=UTC),
                    last_error="timeout",
                    last_symbol="XAUUSDT",
                    last_latency_ms=120.5,
                    updated_at=datetime(2026, 3, 13, 0, 2, 5, tzinfo=UTC),
                )
            )

            runtime_state = repository.load_runtime_state()
            source_health = repository.load_source_health_state()

            self.assertIsNotNone(runtime_state)
            self.assertEqual(runtime_state.total_cycles, 12)
            self.assertEqual(runtime_state.latest_fx_source, "frankfurter")
            self.assertTrue(runtime_state.fx_is_frozen)
            self.assertEqual(source_health[0].source_name, "binance_futures")
            self.assertEqual(source_health[0].success_count, 4)

    def test_retries_initialization_when_database_is_locked(self) -> None:
        original_connect = SQLiteRepositoryBase._connect
        attempts = {"count": 0}

        def flaky_connect(repository: SQLiteRepositoryBase) -> sqlite3.Connection:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise sqlite3.OperationalError("database is locked")
            return original_connect(repository)

        with tempfile.TemporaryDirectory() as tmp_dir:
            with mock.patch.object(SQLiteRepositoryBase, "_connect", autospec=True, side_effect=flaky_connect):
                with mock.patch.object(SQLiteRepositoryBase, "_INITIALIZE_RETRY_DELAY_SEC", 0):
                    repository = SQLiteRepository(f"{tmp_dir}/monitor.db")

            self.assertGreaterEqual(attempts["count"], 2)
            rows = repository.fetch_snapshots(limit=1)
            self.assertEqual(rows, [])

    def test_persists_and_reads_normalized_domestic_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            repository.insert_normalized_domestic_quote(
                "AG_XAG_GROSS",
                MarketQuote(
                    source_name="shfe_domestic",
                    symbol="ag2604",
                    label="AG2604",
                    ts=datetime(2026, 3, 13, 1, 0, tzinfo=UTC),
                    last=21103.0,
                    bid=21102.0,
                    ask=21104.0,
                    raw_payload="seed",
                ),
                fx_source="fx",
                fx_rate=6.9,
                formula="silver",
                formula_version="v1",
                tax_mode="gross",
                target_unit="USD_PER_OUNCE",
                normalized_last=95.55,
                normalized_bid=95.54,
                normalized_ask=95.56,
                timezone_name="Asia/Shanghai",
            )

            rows = repository.fetch_normalized_domestic_history("AG_XAG_GROSS", symbol="ag2604", limit=10)

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["symbol"], "ag2604")
            self.assertEqual(rows[0]["normalized_last"], 95.55)
            self.assertTrue(rows[0]["ts_local"].endswith("+08:00"))

    def test_reports_history_coverage_ranges(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            raw_quote = MarketQuote(
                source_name="domestic",
                symbol="nf_AU0",
                label="AU Main",
                ts=datetime(2026, 3, 10, 0, 0, tzinfo=UTC),
                last=100.0,
                bid=None,
                ask=None,
                raw_payload="raw",
            )
            later_raw_quote = raw_quote.model_copy(update={"ts": datetime(2026, 3, 12, 0, 0, tzinfo=UTC), "last": 101.0})
            repository.insert_raw_quote("AU_XAU", "domestic", raw_quote, timezone_name="Asia/Shanghai")
            repository.insert_raw_quote("AU_XAU", "domestic", later_raw_quote, timezone_name="Asia/Shanghai")
            repository.insert_normalized_domestic_quote(
                "AU_XAU",
                later_raw_quote,
                fx_source="fx",
                fx_rate=6.9,
                formula="gold",
                formula_version="v1",
                tax_mode="gross",
                target_unit="USD_PER_OUNCE",
                normalized_last=455.0,
                normalized_bid=None,
                normalized_ask=None,
                timezone_name="Asia/Shanghai",
            )
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=datetime(2026, 3, 11, 0, 0, tzinfo=UTC),
                    rate=6.9,
                    raw_payload="fx-1",
                ),
                timezone_name="Asia/Shanghai",
            )
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=datetime(2026, 3, 12, 0, 0, tzinfo=UTC),
                    rate=6.95,
                    raw_payload="fx-2",
                ),
                timezone_name="Asia/Shanghai",
            )

            raw_coverage = repository.fetch_raw_quote_history_coverage("AU_XAU", "domestic", symbol="nf_AU0")
            normalized_coverage = repository.fetch_normalized_domestic_history_coverage("AU_XAU", symbol="nf_AU0")
            fx_coverage = repository.fetch_fx_history_coverage("fx")

            self.assertEqual(raw_coverage["row_count"], 2)
            self.assertEqual(raw_coverage["start_ts"], "2026-03-10T00:00:00+00:00")
            self.assertEqual(raw_coverage["end_ts"], "2026-03-12T00:00:00+00:00")
            self.assertEqual(normalized_coverage["row_count"], 1)
            self.assertEqual(normalized_coverage["start_ts"], "2026-03-12T00:00:00+00:00")
            self.assertEqual(fx_coverage["row_count"], 2)
            self.assertEqual(fx_coverage["start_ts"], "2026-03-11T00:00:00+00:00")
            self.assertEqual(fx_coverage["end_ts"], "2026-03-12T00:00:00+00:00")

    def test_persists_dual_timestamps_and_exports_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=datetime(2026, 3, 13, 0, 0, tzinfo=UTC),
                    group_name="AU_XAU",
                    domestic_symbol="AU",
                    overseas_symbol="XAUUSDT",
                    fx_source="fx",
                    fx_rate=7.0,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    status="ok",
                    normalized_last=100.0,
                    overseas_last=101.0,
                    spread=1.0,
                    spread_pct=0.01,
                    zscore=1.5,
                ),
                timezone_name="Asia/Shanghai",
            )

            rows = repository.fetch_snapshots(group_name="AU_XAU", limit=10)
            self.assertEqual(len(rows), 1)
            self.assertIn("ts_utc", rows[0])
            self.assertIn("ts_local", rows[0])
            self.assertTrue(rows[0]["ts_local"].endswith("+08:00"))

            output = Path(tmp_dir) / "snapshots.csv"
            count = repository.export_dataset_to_csv("snapshots", str(output), group_name="AU_XAU")
            self.assertEqual(count, 1)
            with output.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                row = next(reader)
            self.assertIn("ts_utc", row)
            self.assertIn("ts_local", row)

    def test_exports_parquet_when_pyarrow_is_available(self) -> None:
        if pq is None:
            self.skipTest("pyarrow is not installed")

        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=datetime(2026, 3, 13, 0, 0, tzinfo=UTC),
                    group_name="AU_XAU",
                    domestic_symbol="AU",
                    overseas_symbol="XAUUSDT",
                    fx_source="fx",
                    fx_rate=7.0,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    status="ok",
                    normalized_last=100.0,
                    overseas_last=101.0,
                    spread=1.0,
                    spread_pct=0.01,
                    zscore=1.5,
                ),
                timezone_name="Asia/Shanghai",
            )

            output = Path(tmp_dir) / "snapshots.parquet"
            count = repository.export_dataset_to_parquet("snapshots", str(output), group_name="AU_XAU")
            self.assertEqual(count, 1)
            table = pq.read_table(output)
            self.assertGreaterEqual(table.num_rows, 1)

    def test_loads_latest_snapshot_per_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=datetime(2026, 3, 13, 0, 0, tzinfo=UTC),
                    group_name="AU_XAU",
                    domestic_symbol="AU",
                    overseas_symbol="XAUUSDT",
                    fx_source="fx",
                    fx_rate=7.0,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    status="ok",
                    normalized_last=100.0,
                    overseas_last=101.0,
                    spread=1.0,
                    spread_pct=0.01,
                    zscore=1.5,
                ),
                timezone_name="Asia/Shanghai",
            )
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=datetime(2026, 3, 13, 0, 5, tzinfo=UTC),
                    group_name="AU_XAU",
                    domestic_symbol="AU",
                    overseas_symbol="XAUUSDT",
                    fx_source="fx",
                    fx_rate=7.0,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    status="ok",
                    normalized_last=102.0,
                    overseas_last=101.0,
                    spread=2.0,
                    spread_pct=0.02,
                    zscore=1.7,
                ),
                timezone_name="Asia/Shanghai",
            )
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=datetime(2026, 3, 13, 0, 3, tzinfo=UTC),
                    group_name="SC_CL",
                    domestic_symbol="SC",
                    overseas_symbol="CL",
                    fx_source="fx",
                    fx_rate=7.0,
                    formula="crude_oil",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_BARREL",
                    status="ok",
                    normalized_last=70.0,
                    overseas_last=71.0,
                    spread=1.0,
                    spread_pct=0.014,
                    zscore=1.1,
                ),
                timezone_name="Asia/Shanghai",
            )

            latest = repository.load_latest_snapshots()

            self.assertEqual([item.group_name for item in latest], ["AU_XAU", "SC_CL"])
            self.assertEqual(latest[0].spread, 2.0)
            self.assertEqual(latest[1].spread, 1.0)


if __name__ == "__main__":
    unittest.main()
