import asyncio
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest import mock

from cross_market_monitor.application.service import MonitorService
from cross_market_monitor.application.monitor.runtime import MonitorRuntime
from cross_market_monitor.domain.models import FXQuote, MarketQuote, MonitorConfig, SourceHealth, SpreadSnapshot, WorkerRuntimeState
from cross_market_monitor.infrastructure.marketdata.tqsdk import TqSdkMainAdapter
from cross_market_monitor.infrastructure.repository import SQLiteRepository


class StaticQuoteAdapter:
    def __init__(self, source_name: str, last: float | None, bid: float | None, ask: float | None) -> None:
        self.source_name = source_name
        self.last = last
        self.bid = bid
        self.ask = ask

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        return MarketQuote(
            source_name=self.source_name,
            symbol=symbol,
            label=label,
            ts=datetime.now(UTC),
            last=self.last,
            bid=self.bid,
            ask=self.ask,
            raw_payload="static",
        )


class FixedTimestampQuoteAdapter:
    def __init__(
        self,
        source_name: str,
        timestamp: datetime,
        last: float | None,
        bid: float | None,
        ask: float | None,
    ) -> None:
        self.source_name = source_name
        self.timestamp = timestamp
        self.last = last
        self.bid = bid
        self.ask = ask

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        return MarketQuote(
            source_name=self.source_name,
            symbol=symbol,
            label=label,
            ts=self.timestamp,
            last=self.last,
            bid=self.bid,
            ask=self.ask,
            raw_payload="fixed-ts",
        )


class CountingQuoteAdapter:
    def __init__(self, source_name: str, base_last: float, step: float = 1.0) -> None:
        self.source_name = source_name
        self.base_last = base_last
        self.step = step
        self.calls = 0

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        self.calls += 1
        last = self.base_last + (self.calls - 1) * self.step
        return MarketQuote(
            source_name=self.source_name,
            symbol=symbol,
            label=label,
            ts=datetime.now(UTC),
            last=last,
            bid=last - 0.1,
            ask=last + 0.1,
            raw_payload=f"count-{self.calls}",
        )


class StaticFxAdapter:
    def __init__(self, source_name: str, rate: float) -> None:
        self.source_name = source_name
        self.rate = rate

    def fetch_rate(self, base: str, quote: str) -> FXQuote:
        return FXQuote(
            source_name=self.source_name,
            pair=f"{base}/{quote}",
            ts=datetime.now(UTC),
            rate=self.rate,
            raw_payload="static",
        )


class FixedTimestampFxAdapter:
    def __init__(self, source_name: str, timestamp: datetime, rate: float) -> None:
        self.source_name = source_name
        self.timestamp = timestamp
        self.rate = rate

    def fetch_rate(self, base: str, quote: str) -> FXQuote:
        return FXQuote(
            source_name=self.source_name,
            pair=f"{base}/{quote}",
            ts=self.timestamp,
            rate=self.rate,
            raw_payload="fixed-ts",
        )


class HistoryCapableFxAdapter:
    def __init__(self, source_name: str = "fx_history", rate: float = 7.0) -> None:
        self.source_name = source_name
        self.rate = rate

    def fetch_rate(self, base: str, quote: str) -> FXQuote:
        return FXQuote(
            source_name=self.source_name,
            pair=f"{base}/{quote}",
            ts=datetime.now(UTC),
            rate=self.rate,
            raw_payload="fx-history-live",
        )

    def fetch_history(
        self,
        base: str,
        quote: str,
        *,
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
    ) -> list[FXQuote]:
        rows = [
            FXQuote(
                source_name=self.source_name,
                pair=f"{base}/{quote}",
                ts=datetime(2026, 3, 12, 0, 0, tzinfo=UTC),
                rate=6.95,
                raw_payload="fx-hist-1",
            ),
            FXQuote(
                source_name=self.source_name,
                pair=f"{base}/{quote}",
                ts=datetime(2026, 3, 13, 0, 0, tzinfo=UTC),
                rate=6.96,
                raw_payload="fx-hist-2",
            ),
        ]
        return [
            row
            for row in rows
            if (start_ts is None or row.ts >= start_ts) and (end_ts is None or row.ts <= end_ts)
        ]


class CountingFxAdapter:
    def __init__(self, source_name: str, rate: float) -> None:
        self.source_name = source_name
        self.rate = rate
        self.calls = 0

    def fetch_rate(self, base: str, quote: str) -> FXQuote:
        self.calls += 1
        return FXQuote(
            source_name=self.source_name,
            pair=f"{base}/{quote}",
            ts=datetime.now(UTC),
            rate=self.rate,
            raw_payload=f"count-{self.calls}",
        )


class RaisingFxAdapter:
    def fetch_rate(self, base: str, quote: str) -> FXQuote:
        raise RuntimeError(f"{base}/{quote} unavailable")


class RaisingQuoteAdapter:
    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        raise RuntimeError(f"{symbol} unavailable")


class HistoryCapableDomesticAdapter:
    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        return MarketQuote(
            source_name="sina_domestic",
            symbol=symbol,
            label=label,
            ts=datetime.now(UTC),
            last=100.0,
            bid=None,
            ask=None,
            raw_payload="history-capable",
        )

    def fetch_history(
        self,
        symbol: str,
        label: str,
        *,
        interval: str = "5m",
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
    ) -> list[MarketQuote]:
        rows = [
            MarketQuote(
                source_name="sina_domestic",
                symbol=symbol,
                label=label,
                ts=datetime(2026, 3, 12, 1, 0, tzinfo=UTC),
                last=100.0,
                bid=None,
                ask=None,
                raw_payload="hist-1",
            ),
            MarketQuote(
                source_name="sina_domestic",
                symbol=symbol,
                label=label,
                ts=datetime(2026, 3, 12, 1, 5, tzinfo=UTC),
                last=101.0,
                bid=None,
                ask=None,
                raw_payload="hist-2",
            ),
        ]
        return [
            row
            for row in rows
            if (start_ts is None or row.ts >= start_ts) and (end_ts is None or row.ts <= end_ts)
        ]


class HistoryCapableOverseasAdapter:
    def __init__(self, source_name: str = "binance_futures") -> None:
        self.source_name = source_name

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        return MarketQuote(
            source_name=self.source_name,
            symbol=symbol,
            label=label,
            ts=datetime.now(UTC),
            last=82.5,
            bid=None,
            ask=None,
            raw_payload="history-capable-overseas",
        )

    def fetch_history(
        self,
        symbol: str,
        label: str,
        *,
        interval: str = "60m",
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
    ) -> list[MarketQuote]:
        rows = [
            MarketQuote(
                source_name=self.source_name,
                symbol=symbol,
                label=label,
                ts=datetime(2026, 3, 12, 1, 0, tzinfo=UTC),
                last=82.1,
                bid=None,
                ask=None,
                raw_payload="ovs-1",
            ),
            MarketQuote(
                source_name=self.source_name,
                symbol=symbol,
                label=label,
                ts=datetime(2026, 3, 12, 2, 0, tzinfo=UTC),
                last=82.4,
                bid=None,
                ask=None,
                raw_payload="ovs-2",
            ),
        ]
        return [
            row
            for row in rows
            if (start_ts is None or row.ts >= start_ts) and (end_ts is None or row.ts <= end_ts)
        ]


class FakeTqSdkAdapter(TqSdkMainAdapter):
    def __init__(self) -> None:
        self.source_name = "tqsdk_domestic"

    def is_configured(self) -> bool:
        return True

    def fetch_history(
        self,
        symbol: str,
        label: str,
        *,
        interval: str = "30m",
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
    ) -> list[MarketQuote]:
        rows = [
            MarketQuote(
                source_name="tqsdk_domestic",
                symbol=symbol,
                label=label,
                ts=datetime(2026, 3, 12, 1, 0, tzinfo=UTC),
                last=1120.0,
                bid=None,
                ask=None,
                raw_payload="tqsdk-1",
            ),
            MarketQuote(
                source_name="tqsdk_domestic",
                symbol=symbol,
                label=label,
                ts=datetime(2026, 3, 12, 1, 30, tzinfo=UTC),
                last=1121.0,
                bid=None,
                ask=None,
                raw_payload="tqsdk-2",
            ),
        ]
        return [
            row
            for row in rows
            if (start_ts is None or row.ts >= start_ts) and (end_ts is None or row.ts <= end_ts)
        ]


class MonitorServiceTests(unittest.TestCase):
    def test_snapshot_includes_overseas_hedge_position_for_one_domestic_lot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "hedge_contract_size": 32.1507,
                            "domestic_lot_size": 1000,
                        }
                    ],
                }
            )

            service = MonitorService(config, repository)
            asyncio.run(service.poll_once())

            payload = service.get_snapshot()
            self.assertEqual(len(payload["snapshots"]), 1)
            self.assertEqual(payload["snapshots"][0]["group_name"], "AU_XAU_TEST")
            self.assertEqual(payload["snapshots"][0]["hedge_contract_size"], 32.1507)
            self.assertEqual(payload["snapshots"][0]["domestic_lot_size"], 1000)
            self.assertEqual(
                payload["snapshots"][0]["domestic_lot_notional"],
                payload["snapshots"][0]["domestic_last_raw"] * 1000,
            )

    def test_card_view_exposes_trading_sessions_for_chart_filtering(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "domestic_weekends_closed": True,
                        "domestic_non_trading_dates_local": ["2026-09-25"],
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "trading_sessions_local": ["09:00-10:15", "10:30-11:30", "13:30-15:00"],
                        }
                    ],
                }
            )

            service = MonitorService(config, repository)
            asyncio.run(service.poll_once())

            payload = service.get_card_view("AU_XAU_TEST", range_key="24h")
            self.assertEqual(
                payload["card_group"]["selected_item"]["trading_sessions_local"],
                ["09:00-10:15", "10:30-11:30", "13:30-15:00"],
            )
            self.assertTrue(payload["card_group"]["selected_item"]["domestic_weekends_closed"])
            self.assertEqual(
                payload["card_group"]["selected_item"]["domestic_non_trading_dates_local"],
                ["2026-09-25"],
            )

    def test_snapshot_reads_latest_rows_from_repository_when_service_is_not_polling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "hedge_contract_size": 32.1507,
                        }
                    ],
                }
            )

            service = MonitorService(config, repository)
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=datetime(2026, 3, 13, 0, 0, tzinfo=UTC),
                    group_name="AU_XAU_TEST",
                    domestic_symbol="nf_AU0",
                    overseas_symbol="XAUUSDT",
                    fx_source="fx",
                    fx_rate=7.0,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    status="ok",
                    normalized_last=100.0,
                    overseas_last=99.0,
                    spread=1.0,
                    spread_pct=0.01,
                    zscore=1.0,
                ),
                timezone_name="Asia/Shanghai",
            )
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=datetime(2026, 3, 13, 0, 5, tzinfo=UTC),
                    group_name="AU_XAU_TEST",
                    domestic_symbol="nf_AU0",
                    overseas_symbol="XAUUSDT",
                    fx_source="fx",
                    fx_rate=7.1,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    status="stale",
                    normalized_last=101.0,
                    overseas_last=99.5,
                    spread=1.5,
                    spread_pct=0.015,
                    zscore=1.2,
                ),
                timezone_name="Asia/Shanghai",
            )

            payload = service.get_snapshot()
            self.assertEqual(payload["as_of"], "2026-03-13T00:05:00+00:00")
            self.assertEqual(payload["snapshots"][0]["spread"], 1.5)
            self.assertEqual(payload["snapshots"][0]["status"], "stale")
            self.assertEqual(payload["health"]["pairs"][0]["status"], "stale")
            self.assertEqual(payload["health"]["latest_fx_rate"], 7.1)

    def test_snapshot_excludes_disabled_groups_even_if_repository_has_old_latest_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        },
                        {
                            "group_name": "CF_COTTON_TEST",
                            "enabled": False,
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_CF0",
                            "domestic_label": "CF Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "COTTON",
                            "overseas_label": "Gate COTTON",
                            "formula": "cotton",
                            "domestic_unit": "CNY_PER_TON",
                            "target_unit": "USD_PER_POUND",
                        },
                    ],
                }
            )

            service = MonitorService(config, repository)
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=datetime(2026, 3, 13, 0, 0, tzinfo=UTC),
                    group_name="AU_XAU_TEST",
                    domestic_symbol="nf_AU0",
                    overseas_symbol="XAUUSDT",
                    fx_source="fx",
                    fx_rate=7.0,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    status="ok",
                    normalized_last=100.0,
                    overseas_last=99.0,
                    spread=1.0,
                    spread_pct=0.01,
                    zscore=1.0,
                ),
                timezone_name="Asia/Shanghai",
            )
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=datetime(2026, 3, 13, 0, 5, tzinfo=UTC),
                    group_name="CF_COTTON_TEST",
                    domestic_symbol="nf_CF0",
                    overseas_symbol="COTTON",
                    fx_source="fx",
                    fx_rate=7.0,
                    formula="cotton",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_POUND",
                    status="ok",
                    normalized_last=1.0,
                    overseas_last=0.7,
                    spread=0.3,
                    spread_pct=0.35,
                    zscore=2.0,
                ),
                timezone_name="Asia/Shanghai",
            )

            payload = service.get_snapshot()

            self.assertEqual([item["group_name"] for item in payload["snapshots"]], ["AU_XAU_TEST"])
            self.assertEqual([item["group_name"] for item in payload["health"]["pairs"]], ["AU_XAU_TEST"])

    def test_snapshot_default_is_lightweight_without_histories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )

            service = MonitorService(config, repository)
            service.history.get_history = mock.Mock(side_effect=AssertionError("should not load history"))
            service.history.get_shadow_comparison = mock.Mock(side_effect=AssertionError("should not load shadow"))

            payload = service.get_snapshot()

            self.assertEqual(payload["snapshot_mode"], "lightweight")
            self.assertEqual(payload["card_endpoint"], "/api/card")
            self.assertNotIn("histories", payload)
            self.assertNotIn("shadow_comparisons", payload)

    def test_snapshot_can_opt_in_full_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )

            service = MonitorService(config, repository)
            service.history.get_history = mock.Mock(return_value=[{"ts": "2026-03-13T00:00:00+00:00"}])
            service.history.get_shadow_comparison = mock.Mock(return_value={"summary": None})

            payload = service.get_snapshot(include_cards=True)

            self.assertEqual(payload["snapshot_mode"], "full")
            self.assertIn("histories", payload)
            self.assertIn("shadow_comparisons", payload)
            self.assertIn("AU_XAU_TEST", payload["histories"])

    def test_startup_clears_disabled_group_latest_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        },
                        {
                            "group_name": "CF_COTTON_TEST",
                            "enabled": False,
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_CF0",
                            "domestic_label": "CF Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "COTTON",
                            "overseas_label": "Gate COTTON",
                            "formula": "cotton",
                            "domestic_unit": "CNY_PER_TON",
                            "target_unit": "USD_PER_POUND",
                        },
                    ],
                }
            )

            for group_name, domestic_symbol, overseas_symbol, formula, target_unit in [
                ("AU_XAU_TEST", "nf_AU0", "XAUUSDT", "gold", "USD_PER_OUNCE"),
                ("CF_COTTON_TEST", "nf_CF0", "COTTON", "cotton", "USD_PER_POUND"),
            ]:
                repository.insert_snapshot(
                    SpreadSnapshot(
                        ts=datetime(2026, 3, 13, 0, 0, tzinfo=UTC),
                        group_name=group_name,
                        domestic_symbol=domestic_symbol,
                        overseas_symbol=overseas_symbol,
                        fx_source="fx",
                        fx_rate=7.0,
                        formula=formula,
                        formula_version="v1",
                        tax_mode="gross",
                        target_unit=target_unit,
                        status="ok",
                        normalized_last=100.0,
                        overseas_last=99.0,
                        spread=1.0,
                        spread_pct=0.01,
                        zscore=1.0,
                    ),
                    timezone_name="Asia/Shanghai",
                )

            service = MonitorService(config, repository)
            asyncio.run(service.startup())

            latest_groups = [item.group_name for item in repository.load_latest_snapshots()]
            self.assertEqual(latest_groups, ["AU_XAU_TEST"])

    def test_snapshot_payload_includes_commodity_spec(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AL_ALUMINIUM_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AL0",
                            "domestic_label": "AL Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "xyz:ALUMINIUM",
                            "overseas_label": "XYZ AL",
                            "formula": "aluminium",
                            "domestic_unit": "CNY_PER_TON",
                            "target_unit": "USD_PER_TON",
                            "domestic_lot_size": 5,
                            "hedge_contract_size": 5,
                        }
                    ],
                }
            )

            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 23740.0, 23739.0, 23741.0)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 3080.0, 3079.0, 3081.0)
            service.adapters["fx"] = StaticFxAdapter("fx", 6.9)

            asyncio.run(service.poll_once())
            payload = service.get_snapshot_summary()
            spec = payload["snapshots"][0]["commodity_spec"]

            self.assertEqual(spec["formula"], "aluminium")
            self.assertEqual(spec["normalized_unit_label"], "USD/ton")
            self.assertEqual(spec["domestic_lot_size"], 5)
            self.assertEqual(spec["hedge_contract_size"], 5)

    def test_health_and_route_options_expose_source_capabilities(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "sina_futures", "base_url": "https://hq.sinajs.cn"},
                        "overseas": {"kind": "gate_tradfi", "base_url": "https://api.gateio.ws"},
                        "fx": {"kind": "frankfurter_fx", "base_url": "https://api.frankfurter.app"},
                    },
                    "pairs": [
                        {
                            "group_name": "CF_COTTON_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_CF0",
                            "domestic_label": "CF Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "COTTON",
                            "overseas_label": "Gate COTTON",
                            "formula": "cotton",
                            "domestic_unit": "CNY_PER_TON",
                            "target_unit": "USD_PER_POUND",
                        }
                    ],
                }
            )

            service = MonitorService(config, repository)
            health = service.get_health()
            overseas_options = service.get_overseas_route_options("CF_COTTON_TEST")

            overseas_source = next(item for item in health["sources"] if item["source_name"] == "overseas")
            self.assertTrue(overseas_source["capability"]["supports_history"])
            self.assertEqual(overseas_source["capability"]["history_limit"], 500)
            self.assertTrue(overseas_options["options"][0]["capability"]["supports_history"])
            self.assertEqual(overseas_options["options"][0]["source_kind"], "gate_tradfi")

    def test_health_reads_persisted_worker_runtime_and_source_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx_primary",
                        "fx_backup_sources": ["fx_backup"],
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx_primary": {"kind": "mock_fx", "base_url": "http://local"},
                        "fx_backup": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            repository.upsert_runtime_state(
                WorkerRuntimeState(
                    started_at=datetime(2026, 3, 13, 0, 0, tzinfo=UTC),
                    last_poll_finished_at=datetime(2026, 3, 13, 0, 5, tzinfo=UTC),
                    last_heartbeat_at=datetime(2026, 3, 13, 0, 5, 3, tzinfo=UTC),
                    total_cycles=42,
                    latest_fx_rate=6.95,
                    latest_fx_source="fx_backup",
                    latest_fx_jump_pct=0.002,
                    fx_is_live=False,
                    fx_is_frozen=True,
                    fx_last_live_at=datetime(2026, 3, 13, 0, 4, 59, tzinfo=UTC),
                    fx_frozen_since=datetime(2026, 3, 13, 0, 5, tzinfo=UTC),
                )
            )
            repository.upsert_source_health(
                SourceHealth(
                    source_name="fx_backup",
                    kind="mock_fx",
                    success_count=8,
                    failure_count=1,
                    last_success_at=datetime(2026, 3, 13, 0, 5, tzinfo=UTC),
                    last_symbol="USD/CNY",
                    last_latency_ms=12.4,
                    updated_at=datetime(2026, 3, 13, 0, 5, 3, tzinfo=UTC),
                )
            )
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=datetime(2026, 3, 13, 0, 5, tzinfo=UTC),
                    group_name="AU_XAU_TEST",
                    domestic_symbol="nf_AU0",
                    overseas_symbol="XAUUSDT",
                    fx_source="fx_backup",
                    fx_rate=6.95,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    status="stale",
                    normalized_last=100.0,
                    overseas_last=99.0,
                    spread=1.0,
                    spread_pct=0.01,
                    zscore=1.0,
                ),
                timezone_name="Asia/Shanghai",
            )

            service = MonitorService(config, repository)
            health = service.get_health()

            self.assertEqual(health["total_cycles"], 42)
            self.assertEqual(health["latest_fx_source"], "fx_backup")
            self.assertTrue(health["fx_is_frozen"])
            fx_backup = next(item for item in health["sources"] if item["source_name"] == "fx_backup")
            self.assertEqual(fx_backup["success_count"], 8)

    def test_uses_fx_backup_source_when_primary_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx_primary",
                        "fx_backup_sources": ["fx_backup"],
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx_primary": {"kind": "mock_fx", "base_url": "http://local"},
                        "fx_backup": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )

            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 100.0, 99.9, 100.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 101.0, 100.9, 101.1)
            service.adapters["fx_primary"] = RaisingFxAdapter()
            service.adapters["fx_backup"] = StaticFxAdapter("fx_backup", 6.8)

            asyncio.run(service.poll_once())

            snapshot = service.get_snapshot()["snapshots"][0]
            health = service.get_health()
            self.assertEqual(snapshot["fx_source"], "fx_backup")
            self.assertEqual(health["latest_fx_source"], "fx_backup")
            self.assertTrue(health["fx_is_live"])

    def test_history_uses_backup_fx_rows_when_primary_has_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx_primary",
                        "fx_backup_sources": ["fx_backup"],
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx_primary": {"kind": "mock_fx", "base_url": "http://local"},
                        "fx_backup": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            first_ts = datetime(2026, 3, 13, 0, 0, tzinfo=UTC)
            second_ts = datetime(2026, 3, 13, 1, 0, tzinfo=UTC)
            repository.insert_raw_quote(
                "AU_XAU_TEST",
                "domestic",
                MarketQuote(
                    source_name="domestic",
                    symbol="nf_AU0",
                    label="AU Main",
                    ts=first_ts,
                    last=100.0,
                    bid=None,
                    ask=None,
                    raw_payload="domestic-1",
                ),
            )
            repository.insert_raw_quote(
                "AU_XAU_TEST",
                "domestic",
                MarketQuote(
                    source_name="domestic",
                    symbol="nf_AU0",
                    label="AU Main",
                    ts=second_ts,
                    last=101.0,
                    bid=None,
                    ask=None,
                    raw_payload="domestic-2",
                ),
            )
            repository.insert_raw_quote(
                "AU_XAU_TEST",
                "overseas",
                MarketQuote(
                    source_name="overseas",
                    symbol="XAUUSDT",
                    label="Binance XAU",
                    ts=first_ts,
                    last=82.0,
                    bid=None,
                    ask=None,
                    raw_payload="overseas-1",
                ),
            )
            repository.insert_raw_quote(
                "AU_XAU_TEST",
                "overseas",
                MarketQuote(
                    source_name="overseas",
                    symbol="XAUUSDT",
                    label="Binance XAU",
                    ts=second_ts,
                    last=83.0,
                    bid=None,
                    ask=None,
                    raw_payload="overseas-2",
                ),
            )
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx_primary",
                    pair="USD/CNY",
                    ts=first_ts,
                    rate=6.90,
                    raw_payload="primary-1",
                )
            )
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx_backup",
                    pair="USD/CNY",
                    ts=second_ts,
                    rate=6.80,
                    raw_payload="backup-1",
                )
            )

            history = service.history.build_chart_history(
                service.config.pairs[0],
                "nf_AU0",
                "XAUUSDT",
                start_ts="2026-03-12T23:00:00+00:00",
            )

            self.assertEqual(len(history), 2)
            self.assertEqual(history[0]["ts"], "2026-03-13T00:00:00+00:00")
            self.assertEqual(history[1]["ts"], "2026-03-13T01:00:00+00:00")

    def test_chart_history_does_not_backfill_future_domestic_quotes_into_closed_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            pair = service.config.pairs[0]

            repository.insert_normalized_domestic_quote(
                "AU_XAU_TEST",
                MarketQuote(
                    source_name="domestic",
                    symbol="nf_AU0",
                    label="AU Main",
                    ts=datetime(2026, 3, 19, 18, 30, tzinfo=UTC),
                    last=1030.32,
                    bid=None,
                    ask=None,
                    raw_payload="close-domestic",
                ),
                fx_source="fx",
                fx_rate=6.9,
                formula=pair.formula,
                formula_version=pair.formula_version,
                tax_mode=pair.tax_mode,
                target_unit=pair.target_unit,
                normalized_last=4644.156022343052,
                normalized_bid=None,
                normalized_ask=None,
            )
            repository.insert_normalized_domestic_quote(
                "AU_XAU_TEST",
                MarketQuote(
                    source_name="domestic",
                    symbol="nf_AU0",
                    label="AU Main",
                    ts=datetime(2026, 3, 20, 0, 59, tzinfo=UTC),
                    last=1036.6,
                    bid=None,
                    ask=None,
                    raw_payload="reopen-domestic",
                ),
                fx_source="fx",
                fx_rate=6.9,
                formula=pair.formula,
                formula_version=pair.formula_version,
                tax_mode=pair.tax_mode,
                target_unit=pair.target_unit,
                normalized_last=4672.463052994029,
                normalized_bid=None,
                normalized_ask=None,
            )

            for ts_text, price in [
                ("2026-03-19T21:50:19+00:00", 4670.25),
                ("2026-03-19T23:35:26+00:00", 4642.60),
                ("2026-03-20T01:00:02+00:00", 4651.77),
            ]:
                repository.insert_raw_quote(
                    "AU_XAU_TEST",
                    "overseas",
                    MarketQuote(
                        source_name="overseas",
                        symbol="XAUUSDT",
                        label="Binance XAU",
                        ts=datetime.fromisoformat(ts_text),
                        last=price,
                        bid=None,
                        ask=None,
                        raw_payload=f"overseas-{ts_text}",
                    ),
                )

            history = service.history.build_chart_history(pair, "nf_AU0", "XAUUSDT")
            overnight_rows = [
                row
                for row in history
                if row["ts"] in {"2026-03-19T21:50:19+00:00", "2026-03-19T23:35:26+00:00"}
            ]
            reopen_row = next(row for row in history if row["ts"] == "2026-03-20T01:00:02+00:00")

            self.assertEqual(len(overnight_rows), 2)
            self.assertTrue(all(row["domestic_last_raw"] == 1030.32 for row in overnight_rows))
            self.assertEqual(reopen_row["domestic_last_raw"], 1036.6)

    def test_linked_tax_variants_share_same_leg_quote_within_one_poll_cycle(self) -> None:
        cases = [
            {
                "base_group": "AG_XAG",
                "domestic_symbol": "nf_AG0",
                "domestic_label": "AG Main",
                "overseas_symbol": "XAGUSDT",
                "overseas_label": "Binance XAG",
                "formula": "silver",
                "domestic_unit": "CNY_PER_KG",
                "target_unit": "USD_PER_OUNCE",
                "domestic_base_last": 8000.0,
                "overseas_base_last": 30.0,
            },
            {
                "base_group": "CU_COPPER",
                "domestic_symbol": "nf_CU0",
                "domestic_label": "CU Main",
                "overseas_symbol": "COPPERUSDT",
                "overseas_label": "Binance COPPER",
                "formula": "copper",
                "domestic_unit": "CNY_PER_TON",
                "target_unit": "USD_PER_POUND",
                "domestic_base_last": 76000.0,
                "overseas_base_last": 5.5,
            },
        ]

        for case in cases:
            with self.subTest(base_group=case["base_group"]):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
                    config = MonitorConfig.model_validate(
                        {
                            "app": {
                                "name": "test",
                                "fx_source": "fx",
                                "sqlite_path": f"{tmp_dir}/monitor.db",
                            },
                            "sources": {
                                "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                                "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                                "fx": {"kind": "mock_fx", "base_url": "http://local"},
                            },
                            "pairs": [
                                {
                                    "group_name": f"{case['base_group']}_GROSS",
                                    "domestic_source": "domestic",
                                    "domestic_symbol": case["domestic_symbol"],
                                    "domestic_label": case["domestic_label"],
                                    "overseas_source": "overseas",
                                    "overseas_symbol": case["overseas_symbol"],
                                    "overseas_label": case["overseas_label"],
                                    "formula": case["formula"],
                                    "domestic_unit": case["domestic_unit"],
                                    "target_unit": case["target_unit"],
                                    "tax_mode": "gross",
                                },
                                {
                                    "group_name": f"{case['base_group']}_NET",
                                    "domestic_source": "domestic",
                                    "domestic_symbol": case["domestic_symbol"],
                                    "domestic_label": case["domestic_label"],
                                    "overseas_source": "overseas",
                                    "overseas_symbol": case["overseas_symbol"],
                                    "overseas_label": case["overseas_label"],
                                    "formula": case["formula"],
                                    "domestic_unit": case["domestic_unit"],
                                    "target_unit": case["target_unit"],
                                    "tax_mode": "net",
                                },
                            ],
                        }
                    )
                    service = MonitorService(config, repository)
                    domestic_adapter = CountingQuoteAdapter(
                        "domestic", case["domestic_base_last"], step=10.0
                    )
                    overseas_adapter = CountingQuoteAdapter(
                        "overseas", case["overseas_base_last"], step=1.0
                    )
                    service.adapters["domestic"] = domestic_adapter
                    service.adapters["overseas"] = overseas_adapter
                    service.adapters["fx"] = StaticFxAdapter("fx", 7.0)

                    asyncio.run(service.poll_once())

                    gross = service.latest_snapshots[f"{case['base_group']}_GROSS"]
                    net = service.latest_snapshots[f"{case['base_group']}_NET"]
                    self.assertEqual(overseas_adapter.calls, 1)
                    self.assertEqual(domestic_adapter.calls, 1)
                    self.assertEqual(gross.overseas_last, net.overseas_last)
                    self.assertEqual(gross.domestic_last_raw, net.domestic_last_raw)

    def test_suppresses_stale_alert_outside_trading_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "trading_sessions_local": ["09:00-11:30"],
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            pair = service.config.pairs[0]
            snapshot = SpreadSnapshot(
                ts=datetime(2026, 3, 13, 7, 0, tzinfo=UTC),
                group_name="AU_XAU_TEST",
                domestic_symbol="nf_AU0",
                overseas_symbol="XAUUSDT",
                fx_source="fx",
                fx_rate=6.9,
                formula="gold",
                formula_version="v1",
                tax_mode="gross",
                target_unit="USD_PER_OUNCE",
                status="stale",
                errors=["data_quality: one or more quotes are stale"],
            )

            alerts = service.alert_service.evaluate_alerts(pair, snapshot)
            self.assertEqual(alerts, [])

    def test_suppresses_partial_alert_outside_trading_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "trading_sessions_local": ["09:00-11:30"],
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            pair = service.config.pairs[0]
            snapshot = SpreadSnapshot(
                ts=datetime(2026, 3, 13, 8, 0, tzinfo=UTC),
                group_name="AU_XAU_TEST",
                domestic_symbol="nf_AU0",
                overseas_symbol="XAUUSDT",
                fx_source="fx",
                fx_rate=6.9,
                formula="gold",
                formula_version="v1",
                tax_mode="gross",
                target_unit="USD_PER_OUNCE",
                status="partial",
                errors=["route: overseas quote unavailable"],
            )

            alerts = service.alert_service.evaluate_alerts(pair, snapshot)
            self.assertEqual(alerts, [])

    def test_delays_stale_alert_until_issue_persists_for_30_seconds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "trading_sessions_local": ["09:00-11:30"],
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            pair = service.config.pairs[0]

            first_snapshot = SpreadSnapshot(
                ts=datetime(2026, 3, 13, 1, 0, tzinfo=UTC),
                group_name="AU_XAU_TEST",
                domestic_symbol="nf_AU0",
                overseas_symbol="XAUUSDT",
                fx_source="fx",
                fx_rate=6.9,
                formula="gold",
                formula_version="v1",
                tax_mode="gross",
                target_unit="USD_PER_OUNCE",
                status="stale",
                errors=["data_quality: one or more quotes are stale"],
            )
            second_snapshot = first_snapshot.model_copy(update={"ts": datetime(2026, 3, 13, 1, 0, 20, tzinfo=UTC)})
            third_snapshot = first_snapshot.model_copy(update={"ts": datetime(2026, 3, 13, 1, 0, 31, tzinfo=UTC)})

            self.assertEqual(service.alert_service.evaluate_alerts(pair, first_snapshot), [])
            self.assertEqual(service.alert_service.evaluate_alerts(pair, second_snapshot), [])
            alerts = service.alert_service.evaluate_alerts(pair, third_snapshot)
            self.assertEqual(len(alerts), 1)
            self.assertEqual(alerts[0].category, "data_quality")

    def test_data_quality_alerts_dedupe_tax_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AG_XAG_GROSS",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAGUSDT",
                            "overseas_label": "Binance XAG",
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "gross",
                            "trading_sessions_local": ["09:00-11:30"],
                        },
                        {
                            "group_name": "AG_XAG_NET",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAGUSDT",
                            "overseas_label": "Binance XAG",
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "net",
                            "trading_sessions_local": ["09:00-11:30"],
                        },
                    ],
                }
            )
            service = MonitorService(config, repository)
            gross_pair, net_pair = service.config.pairs
            base_snapshot = SpreadSnapshot(
                ts=datetime(2026, 3, 13, 1, 1, tzinfo=UTC),
                group_name="AG_XAG_GROSS",
                domestic_symbol="nf_AG0",
                overseas_symbol="XAGUSDT",
                fx_source="fx",
                fx_rate=6.9,
                formula="silver",
                formula_version="v1",
                tax_mode="gross",
                target_unit="USD_PER_OUNCE",
                status="stale",
                errors=["data_quality: one or more quotes are stale"],
            )
            service.alert_service.evaluate_alerts(gross_pair, base_snapshot.model_copy(update={"ts": datetime(2026, 3, 13, 1, 0, 20, tzinfo=UTC)}))
            gross_alerts = service.alert_service.evaluate_alerts(gross_pair, base_snapshot)
            net_alerts = service.alert_service.evaluate_alerts(
                net_pair,
                base_snapshot.model_copy(
                    update={
                        "group_name": "AG_XAG_NET",
                        "tax_mode": "net",
                    }
                ),
            )
            self.assertEqual(len(gross_alerts), 1)
            self.assertEqual(net_alerts, [])

    def test_suppresses_stale_alert_immediately_after_session_close_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "trading_sessions_local": ["09:00-11:30"],
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            pair = service.config.pairs[0]
            snapshot = SpreadSnapshot(
                ts=datetime(2026, 3, 13, 3, 35, tzinfo=UTC),
                group_name="AU_XAU_TEST",
                domestic_symbol="nf_AU0",
                overseas_symbol="XAUUSDT",
                fx_source="fx",
                fx_rate=6.9,
                formula="gold",
                formula_version="v1",
                tax_mode="gross",
                target_unit="USD_PER_OUNCE",
                status="stale",
                errors=["data_quality: one or more quotes are stale"],
            )

            alerts = service.alert_service.evaluate_alerts(pair, snapshot)
            self.assertEqual(alerts, [])

    def test_can_opt_in_to_post_close_stale_alert_grace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "trading_sessions_local": ["09:00-11:30"],
                            "thresholds": {
                                "stale_alert_grace_sec": 600,
                                "data_quality_alert_delay_sec": 0,
                            },
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            pair = service.config.pairs[0]
            snapshot = SpreadSnapshot(
                ts=datetime(2026, 3, 13, 3, 35, tzinfo=UTC),
                group_name="AU_XAU_TEST",
                domestic_symbol="nf_AU0",
                overseas_symbol="XAUUSDT",
                fx_source="fx",
                fx_rate=6.9,
                formula="gold",
                formula_version="v1",
                tax_mode="gross",
                target_unit="USD_PER_OUNCE",
                status="stale",
                errors=["data_quality: one or more quotes are stale"],
            )

            alerts = service.alert_service.evaluate_alerts(pair, snapshot)
            self.assertEqual(len(alerts), 1)
            self.assertEqual(alerts[0].category, "data_quality")

    def test_retention_service_prunes_old_rows_and_keeps_latest_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                        "raw_quote_retention_days": 1,
                        "fx_rate_retention_days": 1,
                        "normalized_quote_retention_days": 1,
                        "snapshot_retention_days": 1,
                        "alert_retention_days": 1,
                        "delivery_retention_days": 1,
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            old_ts = datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
            fresh_ts = datetime(2026, 3, 13, 0, 0, tzinfo=UTC)
            repository.insert_raw_quote(
                "AU_XAU_TEST",
                "domestic",
                MarketQuote(
                    source_name="domestic",
                    symbol="nf_AU0",
                    label="AU",
                    ts=old_ts,
                    last=100.0,
                    raw_payload="old",
                ),
                timezone_name="Asia/Shanghai",
            )
            repository.insert_raw_quote(
                "AU_XAU_TEST",
                "domestic",
                MarketQuote(
                    source_name="domestic",
                    symbol="nf_AU0",
                    label="AU",
                    ts=fresh_ts,
                    last=101.0,
                    raw_payload="new",
                ),
                timezone_name="Asia/Shanghai",
            )
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=old_ts,
                    group_name="AU_XAU_TEST",
                    domestic_symbol="nf_AU0",
                    overseas_symbol="XAUUSDT",
                    fx_source="fx",
                    fx_rate=6.9,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    status="ok",
                    normalized_last=100.0,
                    overseas_last=99.0,
                    spread=1.0,
                    spread_pct=0.01,
                    zscore=1.0,
                ),
                timezone_name="Asia/Shanghai",
            )
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=fresh_ts,
                    group_name="AU_XAU_TEST",
                    domestic_symbol="nf_AU0",
                    overseas_symbol="XAUUSDT",
                    fx_source="fx",
                    fx_rate=6.9,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    status="ok",
                    normalized_last=101.0,
                    overseas_last=100.0,
                    spread=1.0,
                    spread_pct=0.01,
                    zscore=1.1,
                ),
                timezone_name="Asia/Shanghai",
            )

            report = service.retention.run_once(started_at=datetime(2026, 3, 14, 0, 0, tzinfo=UTC))

            raw_rows = repository.fetch_raw_quote_history("AU_XAU_TEST", "domestic", limit=None)
            latest_snapshots = repository.load_latest_snapshots()
            self.assertEqual(report["deleted_rows"]["raw_quotes"], 1)
            self.assertEqual(len(raw_rows), 1)
            self.assertEqual(raw_rows[0]["last_px"], 101.0)
            self.assertEqual(latest_snapshots[0].normalized_last, 101.0)

    def test_syncs_domestic_and_overseas_preferences_across_tax_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas_primary": {"kind": "binance_futures", "base_url": "http://local"},
                        "overseas_backup": {"kind": "okx_swap", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AG_XAG_GROSS",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "domestic_candidates": [
                                {"source": "domestic", "symbol": "nf_AG0", "label": "AG Main"},
                                {"source": "domestic", "symbol": "ag2604", "label": "AG2604"},
                            ],
                            "overseas_source": "overseas_primary",
                            "overseas_symbol": "XAG_BN",
                            "overseas_label": "Binance XAG",
                            "overseas_candidates": [
                                {"source": "overseas_primary", "symbol": "XAG_BN", "label": "Binance XAG"},
                                {"source": "overseas_backup", "symbol": "XAG_OKX", "label": "OKX XAG"},
                            ],
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "gross",
                        },
                        {
                            "group_name": "AG_XAG_NET",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "domestic_candidates": [
                                {"source": "domestic", "symbol": "nf_AG0", "label": "AG Main"},
                                {"source": "domestic", "symbol": "ag2604", "label": "AG2604"},
                            ],
                            "overseas_source": "overseas_primary",
                            "overseas_symbol": "XAG_BN",
                            "overseas_label": "Binance XAG",
                            "overseas_candidates": [
                                {"source": "overseas_primary", "symbol": "XAG_BN", "label": "Binance XAG"},
                                {"source": "overseas_backup", "symbol": "XAG_OKX", "label": "OKX XAG"},
                            ],
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "net",
                        },
                    ],
                }
            )

            service = MonitorService(config, repository)
            service.set_overseas_route_preference("AG_XAG_GROSS", "XAG_OKX")

            gross_domestic = service.get_domestic_route_options("AG_XAG_GROSS")
            net_domestic = service.get_domestic_route_options("AG_XAG_NET")
            gross_overseas = service.get_overseas_route_options("AG_XAG_GROSS")
            net_overseas = service.get_overseas_route_options("AG_XAG_NET")

            self.assertEqual(gross_domestic["selected_symbol"], "nf_AG0")
            self.assertEqual(net_domestic["selected_symbol"], "nf_AG0")
            self.assertEqual(gross_overseas["selected_symbol"], "XAG_OKX")
            self.assertEqual(net_overseas["selected_symbol"], "XAG_OKX")

    def test_falls_back_to_secondary_route_and_records_source_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic_primary": {"kind": "mock_quote", "base_url": "http://local"},
                        "domestic_backup": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "SC_CL_TEST",
                            "domestic_source": "domestic_primary",
                            "domestic_symbol": "SC",
                            "domestic_label": "SC",
                            "domestic_candidates": [
                                {"source": "domestic_primary", "symbol": "SC_MAIN", "label": "SC Main"},
                                {"source": "domestic_backup", "symbol": "SC_BACKUP", "label": "SC Backup"},
                            ],
                            "overseas_source": "overseas",
                            "overseas_symbol": "CL",
                            "overseas_label": "CL",
                            "overseas_candidates": [
                                {"source": "overseas", "symbol": "CL_MAIN", "label": "CL Main"},
                            ],
                            "formula": "crude_oil",
                            "domestic_unit": "CNY_PER_BARREL",
                            "target_unit": "USD_PER_BARREL",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic_primary"] = RaisingQuoteAdapter()
            service.adapters["domestic_backup"] = StaticQuoteAdapter("domestic_backup", 490.0, 489.5, 490.5)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 72.0, 71.8, 72.2)
            service.adapters["fx"] = StaticFxAdapter("fx", 7.0)

            asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["SC_CL_TEST"]
            self.assertEqual(snapshot.status, "ok")
            self.assertEqual(snapshot.domestic_source, "domestic_backup")
            self.assertEqual(snapshot.route_detail["domestic"]["selected"]["source"], "domestic_backup")
            self.assertEqual(service.source_health["domestic_primary"].failure_count, 1)
            self.assertEqual(service.source_health["domestic_backup"].success_count, 1)

    def test_pauses_signal_when_fx_jump_exceeds_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=datetime(2026, 3, 12, 23, 59, tzinfo=UTC),
                    rate=7.0,
                    raw_payload="seed",
                )
            )
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "fx_window_size": 10,
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "SC_CL_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "SC",
                            "domestic_label": "SC",
                            "overseas_source": "overseas",
                            "overseas_symbol": "CL",
                            "overseas_label": "CL",
                            "formula": "crude_oil",
                            "domestic_unit": "CNY_PER_BARREL",
                            "target_unit": "USD_PER_BARREL",
                            "thresholds": {
                                "fx_jump_abs_pct": 0.01,
                                "pause_on_fx_jump": True,
                            },
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 490.0, 489.5, 490.5)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 72.0, 71.8, 72.2)
            service.adapters["fx"] = StaticFxAdapter("fx", 7.2)

            asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["SC_CL_TEST"]
            alerts = repository.fetch_alerts(limit=10)
            self.assertEqual(snapshot.status, "paused")
            self.assertEqual(snapshot.signal_state, "paused")
            self.assertIsNotNone(snapshot.pause_reason)
            self.assertGreater(abs(snapshot.fx_jump_pct or 0), 0.01)
            self.assertTrue(any(alert["category"] == "fx" for alert in alerts))

    def test_reuses_last_successful_fx_quote_when_fetch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 100.0, 99.9, 100.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 5100.0, 5099.0, 5101.0)
            service.adapters["fx"] = StaticFxAdapter("fx", 7.0)

            asyncio.run(service.poll_once())
            self.assertEqual(service.latest_fx_quote.rate, 7.0)
            self.assertEqual(len(repository.load_recent_fx_rates("fx", 10)), 1)

            expired_ts = datetime.now(UTC) - timedelta(hours=2)
            service.context.latest_fx_quote = service.latest_fx_quote.model_copy(update={"ts": expired_ts})
            service.context.latest_fx_last_live_at = expired_ts
            service.adapters["fx"] = RaisingFxAdapter()
            asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["AU_XAU_TEST"]
            self.assertEqual(snapshot.fx_rate, 7.0)
            self.assertEqual(service.latest_fx_quote.rate, 7.0)
            self.assertIsNone(snapshot.fx_jump_pct)
            self.assertEqual(len(repository.load_recent_fx_rates("fx", 10)), 1)
            self.assertFalse(service.get_health()["fx_is_live"])

    def test_reuses_cached_fx_quote_within_hourly_refresh_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "fx_poll_interval_sec": 3600,
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 100.0, 99.9, 100.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 5100.0, 5099.0, 5101.0)
            fx_adapter = CountingFxAdapter("fx", 7.0)
            service.adapters["fx"] = fx_adapter

            asyncio.run(service.poll_once())
            asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["AU_XAU_TEST"]
            self.assertEqual(fx_adapter.calls, 1)
            self.assertEqual(snapshot.fx_rate, 7.0)
            self.assertEqual(len(repository.load_recent_fx_rates("fx", 10)), 1)
            self.assertTrue(service.get_health()["fx_is_live"])

    def test_hourly_fx_quote_remains_ok_within_refresh_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            half_hour_old = datetime.now(UTC) - timedelta(minutes=30)
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=half_hour_old,
                    rate=7.0,
                    raw_payload="half-hour-old",
                )
            )
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "fx_poll_interval_sec": 3600,
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "thresholds": {
                                "stale_seconds": 180,
                                "max_skew_seconds": 180,
                            },
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 100.0, 99.9, 100.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 5100.0, 5099.0, 5101.0)
            service.adapters["fx"] = RaisingFxAdapter()

            asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["AU_XAU_TEST"]
            self.assertEqual(snapshot.status, "ok")
            self.assertEqual(snapshot.fx_rate, 7.0)
            self.assertIsNotNone(snapshot.fx_age_sec)
            self.assertGreater(snapshot.fx_age_sec or 0, 1700)
            self.assertTrue(snapshot.route_detail["fx_is_live"])
            self.assertFalse(snapshot.route_detail["fx_is_frozen"])
            self.assertEqual(len(repository.load_recent_fx_rates("fx", 10)), 1)

    def test_fx_older_than_poll_window_but_younger_than_24_hours_does_not_alert(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            two_hour_old = datetime.now(UTC) - timedelta(hours=2)
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=two_hour_old,
                    rate=7.0,
                    raw_payload="two-hours-old",
                )
            )
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "fx_poll_interval_sec": 3600,
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "thresholds": {
                                "stale_seconds": 180,
                                "max_skew_seconds": 180,
                            },
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 100.0, 99.9, 100.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 5100.0, 5099.0, 5101.0)
            service.adapters["fx"] = RaisingFxAdapter()

            asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["AU_XAU_TEST"]
            alerts = repository.fetch_alerts(limit=10)
            self.assertEqual(snapshot.status, "ok")
            self.assertGreater(snapshot.fx_age_sec or 0, 7000)
            self.assertFalse(any(alert["category"] == "fx" for alert in alerts))

    def test_fx_older_than_24_hours_emits_global_fx_alert_not_pair_data_quality(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            service = MonitorService(
                MonitorConfig.model_validate(
                    {
                        "app": {
                            "name": "test",
                            "fx_source": "fx",
                            "sqlite_path": f"{tmp_dir}/monitor.db",
                        },
                        "sources": {
                            "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                            "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                            "fx": {"kind": "mock_fx", "base_url": "http://local"},
                        },
                        "pairs": [
                            {
                                "group_name": "AU_XAU_TEST",
                                "domestic_source": "domestic",
                                "domestic_symbol": "nf_AU0",
                                "domestic_label": "AU Main",
                                "overseas_source": "overseas",
                                "overseas_symbol": "XAU",
                                "overseas_label": "XAU",
                                "formula": "gold",
                                "domestic_unit": "CNY_PER_GRAM",
                                "target_unit": "USD_PER_OUNCE",
                            }
                        ],
                    }
                ),
                repository,
            )
            pair = service.config.pairs[0]
            snapshot = SpreadSnapshot(
                ts=datetime(2026, 3, 13, 1, 0, tzinfo=UTC),
                group_name="AU_XAU_TEST",
                domestic_symbol="nf_AU0",
                overseas_symbol="XAU",
                fx_source="fx",
                fx_rate=6.9,
                formula="gold",
                formula_version="v1",
                tax_mode="gross",
                target_unit="USD_PER_OUNCE",
                status="ok",
                fx_age_sec=90000,
            )

            alerts = service.alert_service.evaluate_alerts(pair, snapshot)
            self.assertEqual(len(alerts), 1)
            self.assertEqual(alerts[0].category, "fx")
            self.assertEqual(alerts[0].group_name, "FX")

    def test_fx_unavailable_emits_global_fx_alert_not_pair_data_quality(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            service = MonitorService(
                MonitorConfig.model_validate(
                    {
                        "app": {
                            "name": "test",
                            "fx_source": "fx",
                            "sqlite_path": f"{tmp_dir}/monitor.db",
                        },
                        "sources": {
                            "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                            "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                            "fx": {"kind": "mock_fx", "base_url": "http://local"},
                        },
                        "pairs": [
                            {
                                "group_name": "AU_XAU_TEST",
                                "domestic_source": "domestic",
                                "domestic_symbol": "nf_AU0",
                                "domestic_label": "AU Main",
                                "overseas_source": "overseas",
                                "overseas_symbol": "XAU",
                                "overseas_label": "XAU",
                                "formula": "gold",
                                "domestic_unit": "CNY_PER_GRAM",
                                "target_unit": "USD_PER_OUNCE",
                            }
                        ],
                    }
                ),
                repository,
            )
            pair = service.config.pairs[0]
            snapshot = SpreadSnapshot(
                ts=datetime(2026, 3, 13, 1, 0, tzinfo=UTC),
                group_name="AU_XAU_TEST",
                domestic_symbol="nf_AU0",
                overseas_symbol="XAU",
                fx_source="fx",
                fx_rate=None,
                formula="gold",
                formula_version="v1",
                tax_mode="gross",
                target_unit="USD_PER_OUNCE",
                status="partial",
                errors=["fx: unavailable"],
            )

            alerts = service.alert_service.evaluate_alerts(pair, snapshot)
            self.assertEqual(len(alerts), 1)
            self.assertEqual(alerts[0].category, "fx")
            self.assertEqual(alerts[0].group_name, "FX")

    def test_freezes_domestic_price_and_fx_during_closed_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "timezone": "Asia/Shanghai",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "thresholds": {
                                "stale_seconds": 7200,
                                "max_skew_seconds": 7200,
                            },
                            "trading_sessions_local": ["09:00-10:00"],
                        }
                    ],
                }
            )
            close_ts = datetime(2026, 3, 20, 2, 0, tzinfo=UTC)
            now_ts = datetime(2026, 3, 20, 4, 0, tzinfo=UTC)
            repository.insert_raw_quote(
                "AU_XAU_TEST",
                "domestic",
                MarketQuote(
                    source_name="domestic",
                    symbol="nf_AU0",
                    label="AU Main",
                    ts=close_ts,
                    last=100.0,
                    bid=99.9,
                    ask=100.1,
                    raw_payload="session-close",
                ),
            )
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=close_ts - timedelta(minutes=5),
                    rate=7.0,
                    raw_payload="close-fx",
                )
            )

            service = MonitorService(config, repository)
            service.adapters["domestic"] = FixedTimestampQuoteAdapter("domestic", now_ts, 101.0, 100.9, 101.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 5100.0, 5099.0, 5101.0)
            service.adapters["fx"] = FixedTimestampFxAdapter("fx", now_ts, 7.2)

            with mock.patch("cross_market_monitor.application.common.utc_now", return_value=now_ts):
                with mock.patch("cross_market_monitor.application.monitor.snapshot_builder.utc_now", return_value=now_ts):
                    asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["AU_XAU_TEST"]
            self.assertEqual(snapshot.domestic_last_raw, 100.0)
            self.assertEqual(snapshot.fx_rate, 7.0)
            self.assertEqual(snapshot.route_detail["effective_fx_ts"], (close_ts - timedelta(minutes=5)).isoformat())

    def test_freezes_domestic_price_and_fx_during_weekend_gap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "timezone": "Asia/Shanghai",
                        "fx_source": "fx",
                        "domestic_weekends_closed": True,
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "thresholds": {
                                "stale_seconds": 7200,
                                "max_skew_seconds": 7200,
                            },
                            "trading_sessions_local": ["09:00-10:00", "13:30-15:00", "21:00-02:30"],
                        }
                    ],
                }
            )
            close_ts = datetime(2026, 3, 20, 18, 30, tzinfo=UTC)
            now_ts = datetime(2026, 3, 20, 19, 30, tzinfo=UTC)
            repository.insert_raw_quote(
                "AU_XAU_TEST",
                "domestic",
                MarketQuote(
                    source_name="domestic",
                    symbol="nf_AU0",
                    label="AU Main",
                    ts=close_ts,
                    last=100.0,
                    bid=99.9,
                    ask=100.1,
                    raw_payload="friday-close",
                ),
            )
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=close_ts - timedelta(minutes=5),
                    rate=7.0,
                    raw_payload="friday-close-fx",
                )
            )

            service = MonitorService(config, repository)
            service.adapters["domestic"] = FixedTimestampQuoteAdapter("domestic", now_ts, 101.0, 100.9, 101.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 5100.0, 5099.0, 5101.0)
            service.adapters["fx"] = FixedTimestampFxAdapter("fx", now_ts, 7.2)

            with mock.patch("cross_market_monitor.application.common.utc_now", return_value=now_ts):
                with mock.patch("cross_market_monitor.application.monitor.snapshot_builder.utc_now", return_value=now_ts):
                    asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["AU_XAU_TEST"]
            self.assertEqual(snapshot.domestic_last_raw, 100.0)
            self.assertEqual(snapshot.fx_rate, 7.0)

    def test_keeps_domestic_price_live_during_friday_night_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "timezone": "Asia/Shanghai",
                        "fx_source": "fx",
                        "domestic_weekends_closed": True,
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "thresholds": {
                                "stale_seconds": 7200,
                                "max_skew_seconds": 7200,
                            },
                            "trading_sessions_local": ["09:00-10:00", "13:30-15:00", "21:00-02:30"],
                        }
                    ],
                }
            )
            close_ts = datetime(2026, 3, 20, 7, 0, tzinfo=UTC)
            now_ts = datetime(2026, 3, 20, 14, 30, tzinfo=UTC)
            repository.insert_raw_quote(
                "AU_XAU_TEST",
                "domestic",
                MarketQuote(
                    source_name="domestic",
                    symbol="nf_AU0",
                    label="AU Main",
                    ts=close_ts,
                    last=100.0,
                    bid=99.9,
                    ask=100.1,
                    raw_payload="day-close",
                ),
            )
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=close_ts - timedelta(minutes=5),
                    rate=7.0,
                    raw_payload="day-close-fx",
                )
            )

            service = MonitorService(config, repository)
            service.adapters["domestic"] = FixedTimestampQuoteAdapter("domestic", now_ts, 101.0, 100.9, 101.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 5100.0, 5099.0, 5101.0)
            service.adapters["fx"] = FixedTimestampFxAdapter("fx", now_ts, 7.2)

            with mock.patch("cross_market_monitor.application.common.utc_now", return_value=now_ts):
                with mock.patch("cross_market_monitor.application.monitor.snapshot_builder.utc_now", return_value=now_ts):
                    asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["AU_XAU_TEST"]
            self.assertEqual(snapshot.domestic_last_raw, 101.0)
            self.assertEqual(snapshot.fx_rate, 7.2)

    def test_freezes_domestic_price_and_fx_during_holiday_gap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "timezone": "Asia/Shanghai",
                        "fx_source": "fx",
                        "domestic_weekends_closed": True,
                        "domestic_non_trading_dates_local": ["2026-09-25"],
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "thresholds": {
                                "stale_seconds": 7200,
                                "max_skew_seconds": 7200,
                            },
                            "trading_sessions_local": ["09:00-10:00", "13:30-15:00", "21:00-02:30"],
                        }
                    ],
                }
            )
            close_ts = datetime(2026, 9, 24, 7, 0, tzinfo=UTC)
            now_ts = datetime(2026, 9, 24, 13, 30, tzinfo=UTC)
            repository.insert_raw_quote(
                "AU_XAU_TEST",
                "domestic",
                MarketQuote(
                    source_name="domestic",
                    symbol="nf_AU0",
                    label="AU Main",
                    ts=close_ts,
                    last=100.0,
                    bid=99.9,
                    ask=100.1,
                    raw_payload="pre-holiday-close",
                ),
            )
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=close_ts - timedelta(minutes=5),
                    rate=7.0,
                    raw_payload="pre-holiday-fx",
                )
            )

            service = MonitorService(config, repository)
            service.adapters["domestic"] = FixedTimestampQuoteAdapter("domestic", now_ts, 101.0, 100.9, 101.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 5100.0, 5099.0, 5101.0)
            service.adapters["fx"] = FixedTimestampFxAdapter("fx", now_ts, 7.2)

            with mock.patch("cross_market_monitor.application.common.utc_now", return_value=now_ts):
                with mock.patch("cross_market_monitor.application.monitor.snapshot_builder.utc_now", return_value=now_ts):
                    asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["AU_XAU_TEST"]
            self.assertEqual(snapshot.domestic_last_raw, 100.0)
            self.assertEqual(snapshot.fx_rate, 7.0)

    def test_freezes_fx_to_domestic_timestamp_when_domestic_quote_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            domestic_ts = datetime.now(UTC) - timedelta(minutes=30)
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=domestic_ts,
                    rate=7.0,
                    raw_payload="aligned-fx",
                )
            )
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "thresholds": {
                                "stale_seconds": 7200,
                                "max_skew_seconds": 7200,
                            },
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = FixedTimestampQuoteAdapter("domestic", domestic_ts, 100.0, 99.9, 100.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 5100.0, 5099.0, 5101.0)
            service.adapters["fx"] = FixedTimestampFxAdapter("fx", domestic_ts + timedelta(minutes=1), 7.2)

            asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["AU_XAU_TEST"]
            rows = repository.fetch_normalized_domestic_history("AU_XAU_TEST", symbol="nf_AU0", limit=10)

            self.assertEqual(snapshot.fx_rate, 7.0)
            self.assertIsNone(snapshot.fx_jump_pct)
            self.assertAlmostEqual(rows[-1]["fx_rate"], 7.0)

    def test_emits_pair_specific_spread_threshold_alert(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "thresholds": {
                                "spread_alert_above": 10.0,
                                "spread_alert_below": -10.0,
                            },
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 100.0, 99.9, 100.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 430.0, 429.5, 430.5)
            service.adapters["fx"] = StaticFxAdapter("fx", 7.0)

            asyncio.run(service.poll_once())

            alerts = repository.fetch_alerts(limit=20)
            categories = {alert["category"] for alert in alerts}

            self.assertIn("spread_level", categories)
            self.assertNotIn("price_floor", categories)

    def test_emits_pair_specific_spread_below_threshold_alert(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "thresholds": {
                                "spread_alert_above": 50.0,
                                "spread_alert_below": -5.0,
                            },
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 100.0, 99.9, 100.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 460.0, 459.5, 460.5)
            service.adapters["fx"] = StaticFxAdapter("fx", 7.0)

            asyncio.run(service.poll_once())

            alerts = repository.fetch_alerts(limit=20)
            spread_level_alerts = [alert for alert in alerts if alert["category"] == "spread_level"]

            self.assertTrue(spread_level_alerts)
            self.assertEqual(
                spread_level_alerts[0]["message"],
                "AU_XAU_TEST：-3.46%  |  -15.66\n中 100 | 换 444.34 | 外 460.00",
            )

    def test_emits_spread_pct_alert_from_unquoted_percentage_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "thresholds": {
                                "spread_pct_alert_above": "2%",
                            },
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 100.0, 99.9, 100.1)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 430.0, 429.5, 430.5)
            service.adapters["fx"] = StaticFxAdapter("fx", 7.0)

            asyncio.run(service.poll_once())

            alerts = repository.fetch_alerts(limit=20)
            spread_pct_alerts = [alert for alert in alerts if alert["category"] == "spread_pct"]

            self.assertTrue(spread_pct_alerts)
            self.assertEqual(
                spread_pct_alerts[0]["message"],
                "AU_XAU_TEST：3.28%  |  14.34\n中 100 | 换 444.34 | 外 430.00",
            )

    def test_emits_spread_level_alert_with_compact_notification_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AG_XAG_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAG",
                            "overseas_label": "XAG",
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "thresholds": {
                                "spread_alert_above": 5.0,
                            },
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 17728.0, 17727.0, 17729.0)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 72.21, 72.20, 72.22)
            service.adapters["fx"] = StaticFxAdapter("fx", 6.9)

            with (
                mock.patch(
                    "cross_market_monitor.application.monitor.snapshot_builder.compute_spread",
                    return_value=(5.0, 0.069),
                ),
                mock.patch(
                    "cross_market_monitor.application.monitor.snapshot_builder.normalize_domestic_quote",
                    return_value=SimpleNamespace(last=77.21, bid=77.20, ask=77.22),
                ),
            ):
                asyncio.run(service.poll_once())

            alerts = repository.fetch_alerts(limit=20)
            spread_level_alerts = [alert for alert in alerts if alert["category"] == "spread_level"]

            self.assertTrue(spread_level_alerts)
            self.assertEqual(
                spread_level_alerts[0]["message"],
                "AG_XAG_TEST：6.90%  |  5.00\n中 17,728 | 换 77.21 | 外 72.21",
            )

    def test_domestic_route_options_only_expose_main_route_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "shfe_domestic": {"kind": "shfe_delaymarket", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "domestic_product_code": "au",
                            "domestic_candidates": [
                                {"source": "domestic", "symbol": "nf_AU0", "label": "AU Main"},
                            ],
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)

            options = service.get_domestic_route_options("AU_XAU_TEST")

            self.assertEqual(options["selected_symbol"], "nf_AU0")
            self.assertEqual([item["symbol"] for item in options["options"]], ["nf_AU0"])

    def test_startup_backfills_tqsdk_shadow_history_without_affecting_main_route(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                        "startup_history_backfill_enabled": False,
                        "tqsdk_shadow_source": "tqsdk_domestic",
                        "tqsdk_shadow_enabled": True,
                        "tqsdk_startup_backfill_enabled": True,
                        "tqsdk_startup_backfill_interval": "30m",
                        "tqsdk_startup_backfill_range_key": "30d",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "tqsdk_domestic": {"kind": "tqsdk_main", "base_url": "wss://example.invalid"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "domestic_product_code": "au",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["tqsdk_domestic"] = FakeTqSdkAdapter()
            service.history.start_tqsdk_shadow_collector = lambda: None  # type: ignore[method-assign]

            asyncio.run(service.startup())

            shadow_rows = repository.fetch_raw_quote_history("AU_XAU", "domestic_shadow", symbol="KQ.m@SHFE.au", limit=10)
            domestic_options = service.get_domestic_route_options("AU_XAU")

            self.assertEqual(len(shadow_rows), 2)
            self.assertEqual(domestic_options["selected_symbol"], "nf_AU0")
            self.assertEqual([item["symbol"] for item in domestic_options["options"]], ["nf_AU0"])

    def test_startup_backfills_main_history_for_chart_view(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx_history",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                        "startup_history_backfill_enabled": True,
                        "startup_history_backfill_range_key": "30d",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "tqsdk_domestic": {"kind": "tqsdk_main", "base_url": "wss://example.invalid"},
                        "overseas": {"kind": "binance_futures", "base_url": "http://local"},
                        "fx_history": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "domestic_history_source": "tqsdk_domestic",
                            "domestic_product_code": "au",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["tqsdk_domestic"] = FakeTqSdkAdapter()
            service.adapters["overseas"] = HistoryCapableOverseasAdapter("overseas")
            service.adapters["fx_history"] = HistoryCapableFxAdapter("fx_history", 6.95)

            asyncio.run(service.startup())

            domestic_rows = repository.fetch_raw_quote_history("AU_XAU", "domestic", symbol="KQ.m@SHFE.au", limit=10)
            overseas_rows = repository.fetch_raw_quote_history("AU_XAU", "overseas", symbol="XAUUSDT", limit=10)
            normalized_rows = repository.fetch_normalized_domestic_history("AU_XAU", symbol="nf_AU0", limit=10)
            history = service.get_history("AU_XAU", limit=50, range_key="30d")

            self.assertEqual(len(domestic_rows), 2)
            self.assertEqual(len(overseas_rows), 2)
            self.assertEqual(len(normalized_rows), 2)
            self.assertTrue(history)
            self.assertEqual(history[0]["domestic_symbol"], "nf_AU0")
            self.assertLess(history[0]["ts"], "2026-03-19T14:09:00+00:00")

    def test_runtime_can_start_with_background_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                        "startup_history_backfill_enabled": True,
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            runtime = MonitorRuntime(service.runtime)
            startup_started = asyncio.Event()

            async def slow_backfill() -> None:
                startup_started.set()
                await asyncio.sleep(0.2)

            async def run_case() -> None:
                with (
                    mock.patch.object(service.history, "maybe_backfill_startup_history", side_effect=slow_backfill),
                    mock.patch.object(service.history, "maybe_backfill_tqsdk_shadow_history", return_value=None),
                    mock.patch.object(service.history, "start_tqsdk_shadow_collector", return_value=None),
                    mock.patch.object(service.retention, "maybe_run", return_value=None),
                ):
                    await runtime.start(background_startup=True)
                    await asyncio.sleep(0.01)
                    self.assertTrue(startup_started.is_set())
                    self.assertIsNotNone(service.context.startup_task)
                    self.assertFalse(service.context.startup_task.done())
                    self.assertTrue(service.context.startup_completed)
                    self.assertIsNotNone(runtime.task)
                    await runtime.stop()

            asyncio.run(run_case())

    def test_normalized_history_backfill_merges_tqsdk_and_live_domestic_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "tqsdk_domestic": {"kind": "tqsdk_main", "base_url": "wss://example.invalid"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "domestic_history_source": "tqsdk_domestic",
                            "domestic_product_code": "au",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            older_domestic_ts = datetime(2026, 3, 12, 1, 0, tzinfo=UTC)
            newer_domestic_ts = datetime(2026, 3, 19, 14, 9, 40, tzinfo=UTC)
            for quote in [
                MarketQuote(
                    source_name="tqsdk_domestic",
                    symbol="KQ.m@SHFE.au",
                    label="TqSdk AU",
                    ts=older_domestic_ts,
                    last=1000.0,
                    bid=None,
                    ask=None,
                    raw_payload="old",
                ),
                MarketQuote(
                    source_name="sina_domestic",
                    symbol="nf_AU0",
                    label="AU Main",
                    ts=newer_domestic_ts,
                    last=1030.0,
                    bid=None,
                    ask=None,
                    raw_payload="new",
                ),
            ]:
                repository.insert_raw_quote("AU_XAU", "domestic", quote)
            for quote in [
                MarketQuote(
                    source_name="overseas",
                    symbol="XAUUSDT",
                    label="Binance XAU",
                    ts=datetime(2026, 3, 12, 1, 0, tzinfo=UTC),
                    last=82.1,
                    bid=None,
                    ask=None,
                    raw_payload="ovs-old",
                ),
                MarketQuote(
                    source_name="overseas",
                    symbol="XAUUSDT",
                    label="Binance XAU",
                    ts=datetime(2026, 3, 19, 14, 10, tzinfo=UTC),
                    last=84.2,
                    bid=None,
                    ask=None,
                    raw_payload="ovs-new",
                ),
            ]:
                repository.insert_raw_quote("AU_XAU", "overseas", quote)
            for quote in [
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=datetime(2026, 3, 12, 0, 0, tzinfo=UTC),
                    rate=6.95,
                    raw_payload="fx-old",
                ),
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=datetime(2026, 3, 19, 14, 0, tzinfo=UTC),
                    rate=6.90,
                    raw_payload="fx-new",
                ),
            ]:
                repository.insert_fx_rate(quote)

            report = service.history.backfill_normalized_domestic_history("AU_XAU", range_key="30d")
            normalized_rows = repository.fetch_normalized_domestic_history("AU_XAU", symbol="nf_AU0", limit=10)
            history = service.get_history("AU_XAU", limit=50, range_key="30d")

            self.assertTrue(report["supported"])
            self.assertEqual(report["inserted_rows"], 2)
            self.assertEqual(len(normalized_rows), 2)
            self.assertEqual(normalized_rows[0]["ts"], older_domestic_ts.isoformat())
            self.assertEqual(history[0]["domestic_symbol"], "nf_AU0")
            self.assertEqual(history[0]["ts"], older_domestic_ts.isoformat())

    def test_builds_shadow_comparison_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                        "tqsdk_shadow_source": "tqsdk_domestic",
                        "tqsdk_shadow_enabled": True,
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "tqsdk_domestic": {"kind": "tqsdk_main", "base_url": "wss://example.invalid"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "domestic_product_code": "au",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)

            timestamps = [
                datetime(2026, 3, 13, 1, 0, tzinfo=UTC),
                datetime(2026, 3, 13, 1, 1, tzinfo=UTC),
            ]
            for index, timestamp in enumerate(timestamps):
                repository.insert_raw_quote(
                    "AU_XAU",
                    "domestic",
                    MarketQuote(
                        source_name="domestic",
                        symbol="nf_AU0",
                        label="AU Main",
                        ts=timestamp,
                        last=1120.0 + index,
                        bid=None,
                        ask=None,
                        raw_payload="main",
                    ),
                )
                repository.insert_raw_quote(
                    "AU_XAU",
                    "domestic_shadow",
                    MarketQuote(
                        source_name="tqsdk_domestic",
                        symbol="KQ.m@SHFE.au",
                        label="TqSdk AU Main",
                        ts=timestamp,
                        last=1118.5 + index,
                        bid=None,
                        ask=None,
                        raw_payload="shadow",
                    ),
                )

            report = service.get_shadow_comparison("AU_XAU", limit=10)

            self.assertIsNotNone(report)
            assert report is not None
            self.assertEqual(report["sample_count"], 2)
            self.assertEqual(report["main_symbol"], "nf_AU0")
            self.assertEqual(report["shadow_symbol"], "KQ.m@SHFE.au")
            self.assertGreater(report["latest_spread"], 0)

    def test_shadow_comparison_returns_none_when_shadow_is_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                        "tqsdk_shadow_source": "tqsdk_domestic",
                        "tqsdk_shadow_enabled": False,
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "tqsdk_domestic": {"kind": "tqsdk_main", "base_url": "wss://example.invalid"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "domestic_product_code": "au",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            timestamp = datetime(2026, 3, 13, 1, 0, tzinfo=UTC)
            repository.insert_raw_quote(
                "AU_XAU",
                "domestic",
                MarketQuote(
                    source_name="domestic",
                    symbol="nf_AU0",
                    label="AU Main",
                    ts=timestamp,
                    last=1120.0,
                    bid=None,
                    ask=None,
                    raw_payload="main",
                ),
            )
            repository.insert_raw_quote(
                "AU_XAU",
                "domestic_shadow",
                MarketQuote(
                    source_name="tqsdk_domestic",
                    symbol="KQ.m@SHFE.au",
                    label="TqSdk AU Main",
                    ts=timestamp,
                    last=1118.5,
                    bid=None,
                    ask=None,
                    raw_payload="shadow",
                ),
            )

            self.assertIsNone(service.get_shadow_comparison("AU_XAU", limit=10))

    def test_alerts_when_main_and_shadow_diverge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                        "tqsdk_shadow_source": "tqsdk_domestic",
                        "tqsdk_shadow_enabled": True,
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "tqsdk_domestic": {"kind": "tqsdk_main", "base_url": "wss://example.invalid"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "domestic_product_code": "au",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 1125.0, None, None)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 5000.0, None, None)
            service.adapters["fx"] = StaticFxAdapter("fx", 7.0)

            repository.insert_raw_quote(
                "AU_XAU",
                "domestic_shadow",
                MarketQuote(
                    source_name="tqsdk_domestic",
                    symbol="KQ.m@SHFE.au",
                    label="TqSdk AU Main",
                    ts=datetime.now(UTC),
                    last=1110.0,
                    bid=None,
                    ask=None,
                    raw_payload="shadow",
                ),
            )

            asyncio.run(service.poll_once())

            alerts = repository.fetch_alerts(limit=20)
            self.assertTrue(
                any("main vs TqSdk shadow diverged" in alert["message"] for alert in alerts)
            )

    def test_persists_normalized_domestic_history_during_poll(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AG_XAG_GROSS",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAGUSDT",
                            "overseas_label": "XAG",
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "gross",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 21000.0, 20999.0, 21001.0)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 83.0, 82.9, 83.1)
            service.adapters["fx"] = StaticFxAdapter("fx", 6.9)

            asyncio.run(service.poll_once())

            rows = repository.fetch_normalized_domestic_history("AG_XAG_GROSS", symbol="nf_AG0", limit=10)
            self.assertEqual(len(rows), 1)
            self.assertIsNotNone(rows[0]["normalized_last"])
            self.assertEqual(rows[0]["fx_source"], "fx")

    def test_marks_non_positive_prices_as_data_quality_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "SC_CL_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "SC",
                            "domestic_label": "SC",
                            "overseas_source": "overseas",
                            "overseas_symbol": "CL",
                            "overseas_label": "CL",
                            "formula": "crude_oil",
                            "domestic_unit": "CNY_PER_BARREL",
                            "target_unit": "USD_PER_BARREL",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 0.0, None, None)
            service.adapters["overseas"] = StaticQuoteAdapter("overseas", 72.0, 71.8, 72.2)
            service.adapters["fx"] = StaticFxAdapter("fx", 7.0)

            asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["SC_CL_TEST"]
            alerts = repository.fetch_alerts(limit=10)
            self.assertEqual(snapshot.status, "error")
            self.assertTrue(any("data_quality:" in error for error in snapshot.errors))
            self.assertTrue(any(alert["category"] == "data_quality" for alert in alerts))

    def test_allows_selecting_overseas_route_preference_including_standby_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas_primary": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas_backup": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas_primary",
                            "overseas_symbol": "XAU_PRIMARY",
                            "overseas_label": "Binance XAU",
                            "overseas_candidates": [
                                {"source": "overseas_primary", "symbol": "XAU_PRIMARY", "label": "Binance XAU"},
                                {"source": "overseas_backup", "symbol": "XAU_BACKUP", "label": "OKX XAU", "enabled": False},
                            ],
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["domestic"] = StaticQuoteAdapter("domestic", 100.0, 99.9, 100.1)
            service.adapters["overseas_primary"] = StaticQuoteAdapter("overseas_primary", 5100.0, 5099.0, 5101.0)
            service.adapters["overseas_backup"] = StaticQuoteAdapter("overseas_backup", 5200.0, 5199.0, 5201.0)
            service.adapters["fx"] = StaticFxAdapter("fx", 7.0)

            options_before = service.get_overseas_route_options("AU_XAU_TEST")
            self.assertTrue(any(item["symbol"] == "XAU_BACKUP" for item in options_before["options"]))
            self.assertEqual(options_before["selected_symbol"], "XAU_PRIMARY")

            selected = service.set_overseas_route_preference("AU_XAU_TEST", "XAU_BACKUP")
            self.assertEqual(selected["selected_symbol"], "XAU_BACKUP")

            reset = service.set_overseas_route_preference("AU_XAU_TEST", None)
            self.assertEqual(reset["selected_symbol"], "XAU_PRIMARY")

            selected = service.set_overseas_route_preference("AU_XAU_TEST", "XAU_BACKUP")

            asyncio.run(service.poll_once())

            snapshot = service.latest_snapshots["AU_XAU_TEST"]
            self.assertEqual(snapshot.overseas_symbol, "XAU_BACKUP")
            self.assertEqual(snapshot.route_detail["preferred_overseas_symbol"], "XAU_BACKUP")

            restarted = MonitorService(config, repository)
            restarted_options = restarted.get_overseas_route_options("AU_XAU_TEST")
            self.assertEqual(restarted_options["selected_symbol"], "XAU_BACKUP")

    def test_defaults_overseas_selection_by_exchange_priority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "okx_swap": {"kind": "okx_swap", "base_url": "http://local"},
                        "hyperliquid": {"kind": "hyperliquid", "base_url": "http://local"},
                        "binance_futures": {"kind": "binance_futures", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "okx_swap",
                            "overseas_symbol": "XAU_OKX",
                            "overseas_label": "OKX XAU",
                            "overseas_candidates": [
                                {"source": "okx_swap", "symbol": "XAU_OKX", "label": "OKX XAU"},
                                {"source": "hyperliquid", "symbol": "XAU_HL", "label": "HL XAU"},
                                {"source": "binance_futures", "symbol": "XAU_BN", "label": "Binance XAU"},
                            ],
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)

            options = service.get_overseas_route_options("AU_XAU_TEST")

            self.assertEqual(options["selected_symbol"], "XAU_BN")

    def test_preloads_latest_snapshot_from_repository(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            repository.insert_fx_rate(
                FXQuote(
                    source_name="fx",
                    pair="USD/CNY",
                    ts=datetime(2026, 3, 13, 0, 0, tzinfo=UTC),
                    rate=6.9,
                    raw_payload="seed",
                )
            )
            repository.insert_snapshot(
                SpreadSnapshot(
                    ts=datetime(2026, 3, 13, 0, 1, tzinfo=UTC),
                    group_name="AU_XAU_TEST",
                    domestic_symbol="nf_AU0",
                    overseas_symbol="XAU",
                    fx_source="fx",
                    fx_rate=6.9,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    status="ok",
                    domestic_source="domestic",
                    overseas_source="overseas",
                    domestic_label="AU Main",
                    overseas_label="XAU",
                    normalized_last=100.0,
                    overseas_last=87.7,
                    spread=12.3,
                    spread_pct=0.123,
                    zscore=1.4,
                    route_detail={},
                    errors=[],
                ),
                timezone_name="Asia/Shanghai",
            )
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )

            service = MonitorService(config, repository)

            self.assertIn("AU_XAU_TEST", service.latest_snapshots)
            self.assertEqual(service.latest_snapshots["AU_XAU_TEST"].spread, 12.3)
            self.assertIsNotNone(service.latest_fx_quote)
            self.assertIsNotNone(service.last_poll_finished_at)

    def test_preloads_zscore_window_from_last_30_days_of_snapshot_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            now = datetime.now(UTC)
            for ts, spread in [
                (now - timedelta(days=31), 10.0),
                (now - timedelta(days=15), 2.0),
                (now - timedelta(days=3), 3.0),
            ]:
                repository.insert_snapshot(
                    SpreadSnapshot(
                        ts=ts,
                        group_name="AU_XAU_TEST",
                        domestic_symbol="nf_AU0",
                        overseas_symbol="XAU",
                        fx_source="fx",
                        fx_rate=6.9,
                        formula="gold",
                        formula_version="v1",
                        tax_mode="gross",
                        target_unit="USD_PER_OUNCE",
                        status="ok",
                        domestic_source="domestic",
                        overseas_source="overseas",
                        domestic_label="AU Main",
                        overseas_label="XAU",
                        normalized_last=100.0,
                        overseas_last=87.7,
                        spread=spread,
                        spread_pct=spread / 100.0,
                        zscore=1.4,
                        route_detail={},
                        errors=[],
                    ),
                    timezone_name="Asia/Shanghai",
                )
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                        "zscore_window_days": 30,
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )

            service = MonitorService(config, repository)

            self.assertEqual(
                service.context.windows["AU_XAU_TEST"].values(as_of=now),
                [2.0, 3.0],
            )

    def test_preloads_zscore_window_from_all_snapshot_history_when_window_is_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            now = datetime.now(UTC)
            for ts, spread in [
                (now - timedelta(days=31), 10.0),
                (now - timedelta(days=15), 2.0),
                (now - timedelta(days=3), 3.0),
            ]:
                repository.insert_snapshot(
                    SpreadSnapshot(
                        ts=ts,
                        group_name="AU_XAU_TEST",
                        domestic_symbol="nf_AU0",
                        overseas_symbol="XAU",
                        fx_source="fx",
                        fx_rate=6.9,
                        formula="gold",
                        formula_version="v1",
                        tax_mode="gross",
                        target_unit="USD_PER_OUNCE",
                        status="ok",
                        domestic_source="domestic",
                        overseas_source="overseas",
                        domestic_label="AU Main",
                        overseas_label="XAU",
                        normalized_last=100.0,
                        overseas_last=87.7,
                        spread=spread,
                        spread_pct=spread / 100.0,
                        zscore=1.4,
                        route_detail={},
                        errors=[],
                    ),
                    timezone_name="Asia/Shanghai",
                )
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                        "zscore_window_days": 0,
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "mock_quote", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU_TEST",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAU",
                            "overseas_label": "XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                        }
                    ],
                }
            )

            service = MonitorService(config, repository)

            self.assertEqual(
                service.context.windows["AU_XAU_TEST"].values(as_of=now),
                [10.0, 2.0, 3.0],
            )

    def test_rebuilds_chart_history_from_main_contract_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "binance_futures", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AG_XAG_GROSS",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAGUSDT",
                            "overseas_label": "Binance XAG",
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "gross",
                        },
                        {
                            "group_name": "AG_XAG_NET",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAGUSDT",
                            "overseas_label": "Binance XAG",
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "net",
                        },
                    ],
                }
            )
            service = MonitorService(config, repository)

            timestamps = [
                datetime(2026, 3, 13, 1, 0, tzinfo=UTC),
                datetime(2026, 3, 13, 1, 1, tzinfo=UTC),
                datetime(2026, 3, 13, 1, 2, tzinfo=UTC),
            ]
            for group_name in ("AG_XAG_GROSS", "AG_XAG_NET"):
                for index, timestamp in enumerate(timestamps):
                    repository.insert_raw_quote(
                        group_name,
                        "domestic",
                        MarketQuote(
                            source_name="domestic",
                            symbol="nf_AG0",
                            label="AG Main",
                            ts=timestamp,
                            last=8000 + index * 10,
                            bid=None,
                            ask=None,
                            raw_payload="seed",
                        ),
                    )
                    repository.insert_raw_quote(
                        group_name,
                        "overseas",
                        MarketQuote(
                            source_name="overseas",
                            symbol="XAGUSDT",
                            label="Binance XAG",
                            ts=timestamp,
                            last=34 + index * 0.1,
                            bid=None,
                            ask=None,
                            raw_payload="seed",
                        ),
                    )
            for index, timestamp in enumerate(timestamps):
                repository.insert_fx_rate(
                    FXQuote(
                        source_name="fx",
                        pair="USD/CNY",
                        ts=timestamp,
                        rate=7.0 + index * 0.01,
                        raw_payload="seed",
                    )
                    )

            gross_history = service.get_history("AG_XAG_GROSS", limit=10, range_key="all")
            net_history = service.get_history("AG_XAG_NET", limit=10, range_key="all")

            self.assertEqual(len(gross_history), 3)
            self.assertEqual(len(net_history), 3)
            self.assertTrue(all(row["domestic_symbol"] == "nf_AG0" for row in gross_history))
            self.assertTrue(all(row["domestic_symbol"] == "nf_AG0" for row in net_history))
            self.assertGreater(gross_history[-1]["normalized_last"], net_history[-1]["normalized_last"])
            self.assertIsNotNone(gross_history[-1]["spread"])
            self.assertIsNotNone(net_history[-1]["spread"])

    def test_backfills_domestic_history_only_when_selected_source_supports_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "sina_domestic": {"kind": "sina_futures", "base_url": "https://hq.sinajs.cn"},
                        "shfe_domestic": {"kind": "shfe_delaymarket", "base_url": "https://www.shfe.com.cn"},
                        "overseas": {"kind": "binance_futures", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AG_XAG_GROSS",
                            "domestic_source": "sina_domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "domestic_candidates": [
                                {"source": "sina_domestic", "symbol": "nf_AG0", "label": "AG Main"},
                                {"source": "shfe_domestic", "symbol": "ag2604", "label": "AG2604"},
                            ],
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAGUSDT",
                            "overseas_label": "Binance XAG",
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "gross",
                        },
                        {
                            "group_name": "AG_XAG_NET",
                            "domestic_source": "sina_domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "domestic_candidates": [
                                {"source": "sina_domestic", "symbol": "nf_AG0", "label": "AG Main"},
                                {"source": "shfe_domestic", "symbol": "ag2604", "label": "AG2604"},
                            ],
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAGUSDT",
                            "overseas_label": "Binance XAG",
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "net",
                        },
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["sina_domestic"] = HistoryCapableDomesticAdapter()
            supported_report = service.backfill_domestic_history("AG_XAG_GROSS", range_key="all")
            gross_rows = repository.fetch_raw_quote_history("AG_XAG_GROSS", "domestic", symbol="nf_AG0", limit=10)
            net_rows = repository.fetch_raw_quote_history("AG_XAG_NET", "domestic", symbol="nf_AG0", limit=10)

            self.assertTrue(supported_report["supported"])
            self.assertEqual(supported_report["fetched_rows"], 2)
            self.assertEqual(supported_report["inserted_rows"], 4)
            self.assertEqual(len(gross_rows), 2)
            self.assertEqual(len(net_rows), 2)

    def test_backfills_domestic_history_from_dedicated_tqsdk_source_when_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "sina_domestic": {"kind": "sina_futures", "base_url": "https://hq.sinajs.cn"},
                        "tqsdk_domestic": {"kind": "tqsdk_main", "base_url": "wss://free-api.shinnytech.com/t/nfmd/front/mobile"},
                        "overseas": {"kind": "binance_futures", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AG_XAG_GROSS",
                            "domestic_source": "sina_domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "domestic_history_source": "tqsdk_domestic",
                            "domestic_product_code": "ag",
                            "domestic_candidates": [
                                {"source": "sina_domestic", "symbol": "nf_AG0", "label": "AG Main"},
                            ],
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAGUSDT",
                            "overseas_label": "Binance XAG",
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "gross",
                        },
                        {
                            "group_name": "AG_XAG_NET",
                            "domestic_source": "sina_domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "domestic_history_source": "tqsdk_domestic",
                            "domestic_product_code": "ag",
                            "domestic_candidates": [
                                {"source": "sina_domestic", "symbol": "nf_AG0", "label": "AG Main"},
                            ],
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAGUSDT",
                            "overseas_label": "Binance XAG",
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "net",
                        },
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["tqsdk_domestic"] = FakeTqSdkAdapter()

            supported_report = service.backfill_domestic_history("AG_XAG_GROSS", range_key="all")
            gross_rows = repository.fetch_raw_quote_history("AG_XAG_GROSS", "domestic", symbol="KQ.m@SHFE.ag", limit=10)
            net_rows = repository.fetch_raw_quote_history("AG_XAG_NET", "domestic", symbol="KQ.m@SHFE.ag", limit=10)

            self.assertTrue(supported_report["supported"])
            self.assertEqual(supported_report["domestic_source"], "tqsdk_domestic")
            self.assertEqual(supported_report["domestic_symbol"], "KQ.m@SHFE.ag")
            self.assertEqual(supported_report["fetched_rows"], 2)
            self.assertEqual(len(gross_rows), 2)
            self.assertEqual(len(net_rows), 2)

    def test_backfills_domestic_history_does_not_fall_back_when_dedicated_source_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "sina_domestic": {"kind": "sina_futures", "base_url": "https://hq.sinajs.cn"},
                        "tqsdk_domestic": {"kind": "tqsdk_main", "base_url": "wss://free-api.shinnytech.com/t/nfmd/front/mobile"},
                        "overseas": {"kind": "binance_futures", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AG_XAG_GROSS",
                            "domestic_source": "sina_domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "domestic_history_source": "tqsdk_domestic",
                            "domestic_product_code": "ag",
                            "domestic_candidates": [
                                {"source": "sina_domestic", "symbol": "nf_AG0", "label": "AG Main"},
                            ],
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAGUSDT",
                            "overseas_label": "Binance XAG",
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "gross",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["tqsdk_domestic"] = FakeTqSdkAdapter()
            original_fetch_history = service.adapters["tqsdk_domestic"].fetch_history
            service.adapters["tqsdk_domestic"].fetch_history = mock.Mock(side_effect=RuntimeError("tqsdk unavailable"))  # type: ignore[method-assign]
            service.adapters["sina_domestic"] = HistoryCapableDomesticAdapter()

            report = service.backfill_domestic_history("AG_XAG_GROSS", range_key="all")
            rows = repository.fetch_raw_quote_history("AG_XAG_GROSS", "domestic", symbol="nf_AG0", limit=10)

            self.assertFalse(report["supported"])
            self.assertEqual(report["domestic_source"], "tqsdk_domestic")
            self.assertEqual(report["domestic_symbol"], "KQ.m@SHFE.ag")
            self.assertIn("tqsdk unavailable", report["reason"])
            self.assertEqual(len(rows), 0)
            service.adapters["tqsdk_domestic"].fetch_history = original_fetch_history  # type: ignore[method-assign]

    def test_backfills_overseas_history_only_when_selected_source_supports_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "binance_futures": {"kind": "binance_futures", "base_url": "https://fapi.binance.com"},
                        "okx_swap": {"kind": "okx_swap", "base_url": "https://www.okx.com"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AG_XAG_GROSS",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "overseas_source": "binance_futures",
                            "overseas_symbol": "XAGUSDT",
                            "overseas_label": "Binance XAG",
                            "overseas_candidates": [
                                {"source": "binance_futures", "symbol": "XAGUSDT", "label": "Binance XAG"},
                                {"source": "okx_swap", "symbol": "XAG-USDT-SWAP", "label": "OKX XAG"},
                            ],
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "gross",
                        },
                        {
                            "group_name": "AG_XAG_NET",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AG0",
                            "domestic_label": "AG Main",
                            "overseas_source": "binance_futures",
                            "overseas_symbol": "XAGUSDT",
                            "overseas_label": "Binance XAG",
                            "overseas_candidates": [
                                {"source": "binance_futures", "symbol": "XAGUSDT", "label": "Binance XAG"},
                                {"source": "okx_swap", "symbol": "XAG-USDT-SWAP", "label": "OKX XAG"},
                            ],
                            "formula": "silver",
                            "domestic_unit": "CNY_PER_KG",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "net",
                        },
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["binance_futures"] = HistoryCapableOverseasAdapter("binance_futures")

            report = service.backfill_overseas_history("AG_XAG_GROSS", interval="60m", range_key="all")
            gross_rows = repository.fetch_raw_quote_history("AG_XAG_GROSS", "overseas", symbol="XAGUSDT", limit=10)
            net_rows = repository.fetch_raw_quote_history("AG_XAG_NET", "overseas", symbol="XAGUSDT", limit=10)

            self.assertTrue(report["supported"])
            self.assertEqual(report["fetched_rows"], 2)
            self.assertEqual(report["inserted_rows"], 4)
            self.assertEqual(len(gross_rows), 2)
            self.assertEqual(len(net_rows), 2)

    def test_history_range_filters_and_downsamples(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "overseas": {"kind": "binance_futures", "base_url": "http://local"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "overseas",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "gross",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)

            start = datetime.now(UTC) - timedelta(days=2)
            for index in range(577):
                timestamp = start + timedelta(minutes=index * 5)
                raw_last = 780.0 + index * 0.05
                normalized_last = 100.0 + index * 0.02
                overseas_last = 99.5 + index * 0.018
                repository.insert_normalized_domestic_quote(
                    "AU_XAU",
                    MarketQuote(
                        source_name="domestic",
                        symbol="nf_AU0",
                        label="AU Main",
                        ts=timestamp,
                        last=raw_last,
                        bid=None,
                        ask=None,
                        raw_payload="seed",
                    ),
                    fx_source="fx",
                    fx_rate=7.0,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    normalized_last=normalized_last,
                    normalized_bid=None,
                    normalized_ask=None,
                )
                repository.insert_raw_quote(
                    "AU_XAU",
                    "overseas",
                    MarketQuote(
                        source_name="overseas",
                        symbol="XAUUSDT",
                        label="Binance XAU",
                        ts=timestamp,
                        last=overseas_last,
                        bid=None,
                        ask=None,
                        raw_payload="seed",
                    ),
                )

            history_24h = service.get_history("AU_XAU", limit=240, range_key="24h")
            history_1y = service.get_history("AU_XAU", limit=900, range_key="1y")
            history_all = service.get_history("AU_XAU", limit=300, range_key="all")

            self.assertLessEqual(len(history_24h), 240)
            self.assertGreater(len(history_24h), 150)
            self.assertEqual(len(history_1y), 577)
            self.assertLessEqual(len(history_all), 300)
            self.assertGreater(len(history_all), 200)

            start_24h = datetime.fromisoformat(history_24h[0]["ts"])
            end_24h = datetime.fromisoformat(history_24h[-1]["ts"])
            self.assertLessEqual((end_24h - start_24h).total_seconds(), 24 * 3600 + 900)
            self.assertEqual(history_1y[0]["ts"], history_all[0]["ts"])
            self.assertEqual(history_1y[-1]["ts"], history_all[-1]["ts"])
            self.assertTrue(all(row["domestic_symbol"] == "nf_AU0" for row in history_all))

    def test_get_history_reads_local_rows_and_backfill_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "test",
                        "fx_source": "fx",
                        "sqlite_path": f"{tmp_dir}/monitor.db",
                    },
                    "sources": {
                        "domestic": {"kind": "mock_quote", "base_url": "http://local"},
                        "binance_futures": {"kind": "binance_futures", "base_url": "https://fapi.binance.com"},
                        "fx": {"kind": "mock_fx", "base_url": "http://local"},
                    },
                    "pairs": [
                        {
                            "group_name": "AU_XAU",
                            "domestic_source": "domestic",
                            "domestic_symbol": "nf_AU0",
                            "domestic_label": "AU Main",
                            "overseas_source": "binance_futures",
                            "overseas_symbol": "XAUUSDT",
                            "overseas_label": "Binance XAU",
                            "formula": "gold",
                            "domestic_unit": "CNY_PER_GRAM",
                            "target_unit": "USD_PER_OUNCE",
                            "tax_mode": "gross",
                        }
                    ],
                }
            )
            service = MonitorService(config, repository)
            service.adapters["binance_futures"] = HistoryCapableOverseasAdapter("binance_futures")

            timestamps = [
                datetime(2026, 3, 12, 1, 0, tzinfo=UTC),
                datetime(2026, 3, 12, 2, 0, tzinfo=UTC),
            ]
            for index, timestamp in enumerate(timestamps):
                repository.insert_normalized_domestic_quote(
                    "AU_XAU",
                    MarketQuote(
                        source_name="domestic",
                        symbol="nf_AU0",
                        label="AU Main",
                        ts=timestamp,
                        last=790.0 + index,
                        bid=None,
                        ask=None,
                        raw_payload="seed",
                    ),
                    fx_source="fx",
                    fx_rate=7.0,
                    formula="gold",
                    formula_version="v1",
                    tax_mode="gross",
                    target_unit="USD_PER_OUNCE",
                    normalized_last=111.2 + index,
                    normalized_bid=None,
                    normalized_ask=None,
                )

            history = service.get_history("AU_XAU", limit=50, range_key="30d")
            overseas_rows = repository.fetch_raw_quote_history("AU_XAU", "overseas", symbol="XAUUSDT", limit=10)

            self.assertEqual(len(history), 0)
            self.assertEqual(len(overseas_rows), 0)

            report = service.backfill_overseas_history("AU_XAU", interval="60m", range_key="30d")
            history = service.get_history("AU_XAU", limit=50, range_key="30d")
            overseas_rows = repository.fetch_raw_quote_history("AU_XAU", "overseas", symbol="XAUUSDT", limit=10)

            self.assertTrue(report["supported"])
            self.assertEqual(len(history), 2)
            self.assertEqual(len(overseas_rows), 2)
            self.assertEqual(history[-1]["overseas_symbol"], "XAUUSDT")
            self.assertIsNotNone(history[-1]["spread"])


if __name__ == "__main__":
    unittest.main()
