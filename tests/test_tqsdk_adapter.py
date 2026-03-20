import os
import unittest
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from cross_market_monitor.domain.models import SourceConfig
from cross_market_monitor.infrastructure.marketdata.tqsdk import TqSdkMainAdapter


class TqSdkMainAdapterTests(unittest.TestCase):
    def test_credentials_prefer_config_over_environment(self) -> None:
        adapter = TqSdkMainAdapter(
            "tqsdk_domestic",
            SourceConfig(
                kind="tqsdk_main",
                base_url="wss://example.invalid",
                params={
                    "auth_user": "config_user",
                    "auth_password": "config_password",
                    "auth_user_env": "TQSDK_USER",
                    "auth_password_env": "TQSDK_PASSWORD",
                },
            ),
        )

        with patch.dict(os.environ, {"TQSDK_USER": "env_user", "TQSDK_PASSWORD": "env_password"}, clear=True):
            self.assertEqual(adapter._credentials(), ("config_user", "config_password"))

    def test_credentials_fall_back_to_environment(self) -> None:
        adapter = TqSdkMainAdapter(
            "tqsdk_domestic",
            SourceConfig(
                kind="tqsdk_main",
                base_url="wss://example.invalid",
                params={
                    "auth_user_env": "ALT_TQSDK_USER",
                    "auth_password_env": "ALT_TQSDK_PASSWORD",
                },
            ),
        )

        with patch.dict(
            os.environ,
            {"ALT_TQSDK_USER": "env_user", "ALT_TQSDK_PASSWORD": "env_password"},
            clear=True,
        ):
            self.assertEqual(adapter._credentials(), ("env_user", "env_password"))

    def test_md_url_prefers_config_over_environment(self) -> None:
        adapter = TqSdkMainAdapter(
            "tqsdk_domestic",
            SourceConfig(
                kind="tqsdk_main",
                base_url="wss://base.invalid",
                params={
                    "md_url": "wss://config.invalid",
                    "md_url_env": "TQSDK_MD_URL",
                },
            ),
        )

        with patch.dict(os.environ, {"TQSDK_MD_URL": "wss://env.invalid"}, clear=True):
            self.assertEqual(adapter._md_url(), "wss://config.invalid")

    def test_create_api_retries_transient_login_errors(self) -> None:
        fake_api = object()
        adapter = TqSdkMainAdapter(
            "tqsdk_domestic",
            SourceConfig(
                kind="tqsdk_main",
                base_url="wss://example.invalid",
                params={
                    "auth_user": "config_user",
                    "auth_password": "config_password",
                    "login_retry_attempts": "3",
                    "retry_backoff_sec": "0.01",
                    "retry_max_backoff_sec": "0.02",
                },
            ),
        )

        with (
            patch("cross_market_monitor.infrastructure.marketdata.tqsdk.TqAuth", side_effect=lambda user, password: (user, password)),
            patch(
                "cross_market_monitor.infrastructure.marketdata.tqsdk.TqApi",
                side_effect=[
                    RuntimeError("Read timed out"),
                    RuntimeError("Connection reset by peer"),
                    fake_api,
                ],
            ) as mock_api,
            patch("cross_market_monitor.infrastructure.marketdata.tqsdk.time.sleep") as mock_sleep,
        ):
            self.assertIs(adapter._create_api(), fake_api)
            self.assertEqual(mock_api.call_count, 3)
            self.assertEqual(mock_sleep.call_count, 2)

    def test_fetch_history_retries_transient_errors(self) -> None:
        class FakeKlines:
            def iterrows(self):
                yield 0, {
                    "datetime": int(datetime(2026, 3, 19, 13, 0, tzinfo=UTC).timestamp() * 1_000_000_000),
                    "close": 100.5,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "volume": 1,
                    "open_oi": 1,
                    "close_oi": 1,
                    "symbol": "KQ.m@SHFE.au",
                    "duration": 1800,
                }

        class FakeApi:
            def __init__(self, should_fail: bool) -> None:
                self.should_fail = should_fail

            def get_kline_serial(self, symbol, duration_seconds, data_length=8000):
                if self.should_fail:
                    raise RuntimeError("Read timed out")
                return FakeKlines()

            def wait_update(self, deadline=None):
                return None

            def close(self):
                return None

        adapter = TqSdkMainAdapter(
            "tqsdk_domestic",
            SourceConfig(
                kind="tqsdk_main",
                base_url="wss://example.invalid",
                params={
                    "auth_user": "config_user",
                    "auth_password": "config_password",
                    "history_retry_attempts": "3",
                    "retry_backoff_sec": "0.01",
                    "retry_max_backoff_sec": "0.02",
                },
            ),
        )
        adapter._create_api_once = MagicMock(  # type: ignore[method-assign]
            side_effect=[FakeApi(True), FakeApi(True), FakeApi(False)]
        )

        with patch("cross_market_monitor.infrastructure.marketdata.tqsdk.time.sleep") as mock_sleep:
            rows = adapter.fetch_history("KQ.m@SHFE.au", "TqSdk AU", interval="30m")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].last, 100.5)
        self.assertEqual(adapter._create_api_once.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
