import asyncio
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


if __name__ == "__main__":
    unittest.main()
