import asyncio
import sys
import unittest
from unittest import mock

from cross_market_monitor import main


class MainTests(unittest.TestCase):
    def test_run_once_disables_startup_history_by_default(self) -> None:
        service = mock.Mock()
        service.startup = mock.AsyncMock()
        service.poll_once = mock.AsyncMock()
        service.shutdown = mock.AsyncMock()
        service.get_snapshot.return_value = {"as_of": "2026-03-21T00:00:00+00:00", "snapshots": []}

        with (
            mock.patch("cross_market_monitor.main.build_service", return_value=service) as build_service,
            mock.patch("cross_market_monitor.main.print_console_table") as print_table,
        ):
            asyncio.run(main.run_once("config/monitor.yaml"))

        build_service.assert_called_once_with(
            "config/monitor.yaml",
            app_overrides={
                "startup_history_backfill_enabled": False,
                "tqsdk_startup_backfill_enabled": False,
            },
        )
        service.startup.assert_awaited_once_with()
        service.poll_once.assert_awaited_once_with()
        service.shutdown.assert_awaited_once_with()
        print_table.assert_called_once_with({"as_of": "2026-03-21T00:00:00+00:00", "snapshots": []})

    def test_run_worker_starts_with_background_history(self) -> None:
        service = mock.Mock()
        service.startup = mock.AsyncMock()
        service.run_forever = mock.AsyncMock()
        service.shutdown = mock.AsyncMock()

        with mock.patch("cross_market_monitor.main.build_service", return_value=service):
            asyncio.run(main.run_worker("config/monitor.yaml"))

        service.startup.assert_awaited_once_with(background_history=True)
        service.run_forever.assert_awaited_once_with()
        service.shutdown.assert_awaited_once_with()

    def test_serve_skips_spread_window_preload_for_fast_boot(self) -> None:
        service = mock.Mock()
        service.config.app.bind_host = "127.0.0.1"
        service.config.app.bind_port = 6080
        app = object()

        with (
            mock.patch.object(sys, "argv", ["cross-market-monitor", "--config", "config/monitor.yaml", "serve"]),
            mock.patch("cross_market_monitor.main.configure_logging", return_value="Asia/Shanghai"),
            mock.patch("cross_market_monitor.main.build_service", return_value=service) as build_service,
            mock.patch("cross_market_monitor.main.build_uvicorn_log_config", return_value={"version": 1}),
            mock.patch("cross_market_monitor.interfaces.api.app.create_app", return_value=app) as create_app,
            mock.patch("uvicorn.run") as uvicorn_run,
        ):
            main.main()

        build_service.assert_called_once_with(
            "config/monitor.yaml",
            preload_spread_windows=False,
        )
        create_app.assert_called_once_with(service, run_runtime=True, serve_dashboard=True)
        uvicorn_run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
