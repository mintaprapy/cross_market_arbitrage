import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from cross_market_monitor.application.monitor.quote_router import QuoteRouter
from cross_market_monitor.application.monitor.source_health import SourceHealthRecorder
from cross_market_monitor.domain.models import (
    AppConfig,
    MarketQuote,
    MonitorConfig,
    PairConfig,
    QuoteRouteConfig,
    SourceConfig,
    SourceHealth,
)


class _FakeRepository:
    def __init__(self, cached_quote: MarketQuote | None = None) -> None:
        self.cached_quote = cached_quote
        self.inserted_raw_quotes: list[tuple[str, str, MarketQuote, str]] = []
        self.health_updates: list[SourceHealth] = []
        self.load_calls: list[tuple[str, str, str, datetime]] = []

    def insert_raw_quote(self, group_name: str, leg_type: str, quote: MarketQuote, *, timezone_name: str) -> None:
        self.inserted_raw_quotes.append((group_name, leg_type, quote, timezone_name))

    def load_latest_raw_quote_before(self, group_name: str, leg_type: str, symbol: str, target_ts: datetime):
        self.load_calls.append((group_name, leg_type, symbol, target_ts))
        return self.cached_quote

    def upsert_source_health(self, health: SourceHealth) -> None:
        self.health_updates.append(health.model_copy(deep=True))


class _FakeAdapter:
    def __init__(self, quote: MarketQuote) -> None:
        self.quote = quote
        self.calls: list[tuple[str, str]] = []

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        self.calls.append((symbol, label))
        return self.quote


def _build_pair() -> PairConfig:
    return PairConfig(
        group_name="AU_XAU",
        domestic_source="tqsdk_domestic",
        domestic_symbol="KQ.m@SHFE.au",
        domestic_label="TqSdk SHFE AU Main",
        overseas_source="binance_futures",
        overseas_symbol="XAUUSDT",
        overseas_label="Binance XAUUSDT",
        formula="gold",
        domestic_unit="CNY_PER_GRAM",
        target_unit="USD_PER_OUNCE",
        domestic_product_code="au",
        trading_sessions_local=["09:00-10:15", "10:30-11:30", "13:30-15:00", "21:00-02:30"],
    )


def _build_context(repository: _FakeRepository, adapter: _FakeAdapter, pair: PairConfig):
    return SimpleNamespace(
        repository=repository,
        adapters={"tqsdk_domestic": adapter},
        config=MonitorConfig(
            app=AppConfig(timezone="Asia/Hong_Kong"),
            sources={
                "tqsdk_domestic": SourceConfig(
                    kind="tqsdk_main",
                    base_url="wss://free-api.shinnytech.com/t/nfmd/front/mobile",
                ),
            },
            pairs=[pair],
        ),
        source_health={
            "tqsdk_domestic": SourceHealth(
                source_name="tqsdk_domestic",
                kind="tqsdk_main",
            )
        },
        pair_map={pair.group_name: pair},
        local_tz=ZoneInfo("Asia/Hong_Kong"),
    )


class QuoteRouterTests(unittest.IsolatedAsyncioTestCase):
    async def test_skips_tqsdk_fetch_outside_fetch_window_and_reuses_cached_quote(self) -> None:
        pair = _build_pair()
        cached_quote = MarketQuote(
            source_name="tqsdk_domestic",
            symbol="KQ.m@SHFE.au",
            label="TqSdk SHFE AU Main",
            ts=datetime(2026, 3, 29, 18, 30, tzinfo=UTC),
            last=730.0,
        )
        repository = _FakeRepository(cached_quote=cached_quote)
        live_quote = MarketQuote(
            source_name="tqsdk_domestic",
            symbol="KQ.m@SHFE.au",
            label="TqSdk SHFE AU Main",
            ts=datetime(2026, 3, 30, 1, 0, tzinfo=UTC),
            last=731.0,
        )
        adapter = _FakeAdapter(live_quote)
        router = QuoteRouter(_build_context(repository, adapter, pair), SourceHealthRecorder(_build_context(repository, adapter, pair)))

        with patch(
            "cross_market_monitor.application.monitor.quote_router.utc_now",
            return_value=datetime(2026, 3, 30, 0, 40, tzinfo=UTC),
        ):
            selected, quotes, errors, detail = await router.fetch_leg_quote(
                "AU_XAU",
                "domestic",
                [QuoteRouteConfig(source="tqsdk_domestic", symbol="KQ.m@SHFE.au", label="TqSdk SHFE AU Main")],
            )

        self.assertEqual(adapter.calls, [])
        self.assertEqual(repository.inserted_raw_quotes, [])
        self.assertEqual(repository.load_calls[0][0:3], ("AU_XAU", "domestic", "KQ.m@SHFE.au"))
        self.assertEqual(selected, cached_quote)
        self.assertEqual(quotes, [cached_quote])
        self.assertEqual(errors, [])
        self.assertTrue(detail["attempts"][0]["skipped"])
        self.assertEqual(detail["attempts"][0]["skip_reason"], "outside_tqsdk_fetch_window")
        self.assertFalse(detail["attempts"][0]["persisted"])

    async def test_fetches_tqsdk_live_within_preopen_window(self) -> None:
        pair = _build_pair()
        repository = _FakeRepository()
        live_quote = MarketQuote(
            source_name="tqsdk_domestic",
            symbol="KQ.m@SHFE.au",
            label="TqSdk SHFE AU Main",
            ts=datetime(2026, 3, 30, 0, 56, tzinfo=UTC),
            last=731.0,
        )
        adapter = _FakeAdapter(live_quote)
        context = _build_context(repository, adapter, pair)
        router = QuoteRouter(context, SourceHealthRecorder(context))

        with patch(
            "cross_market_monitor.application.monitor.quote_router.utc_now",
            return_value=datetime(2026, 3, 30, 0, 56, tzinfo=UTC),
        ):
            selected, quotes, errors, detail = await router.fetch_leg_quote(
                "AU_XAU",
                "domestic",
                [QuoteRouteConfig(source="tqsdk_domestic", symbol="KQ.m@SHFE.au", label="TqSdk SHFE AU Main")],
            )

        self.assertEqual(adapter.calls, [("KQ.m@SHFE.au", "TqSdk SHFE AU Main")])
        self.assertEqual(len(repository.inserted_raw_quotes), 1)
        self.assertEqual(selected, live_quote)
        self.assertEqual(quotes, [live_quote])
        self.assertEqual(errors, [])
        self.assertFalse(detail["attempts"][0]["skipped"])
        self.assertIsNone(detail["attempts"][0]["skip_reason"])
        self.assertTrue(detail["attempts"][0]["persisted"])


if __name__ == "__main__":
    unittest.main()
