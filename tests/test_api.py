import asyncio
import tempfile
import unittest
from unittest import mock

from cross_market_monitor.application.service import MonitorService
from cross_market_monitor.domain.models import MonitorConfig
from cross_market_monitor.infrastructure.repository import SQLiteRepository
from cross_market_monitor.interfaces.api.app import create_app
from starlette.routing import Mount, Route


class ApiTests(unittest.TestCase):
    def test_run_api_mode_serves_dashboard_assets_without_starting_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository = SQLiteRepository(f"{tmp_dir}/monitor.db")
            config = MonitorConfig.model_validate(
                {
                    "app": {
                        "name": "API Test",
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
            app = create_app(service, run_runtime=False)
            routes_by_path = {route.path: route for route in app.routes}

            self.assertIn("/", routes_by_path)
            self.assertIn("/api/health", routes_by_path)
            self.assertIn("/api/snapshot-summary", routes_by_path)
            self.assertIn("/dashboard", routes_by_path)

            index_route = routes_by_path["/"]
            health_route = routes_by_path["/api/health"]
            summary_route = routes_by_path["/api/snapshot-summary"]
            dashboard_mount = routes_by_path["/dashboard"]

            self.assertIsInstance(index_route, Route)
            self.assertIsInstance(health_route, Route)
            self.assertIsInstance(summary_route, Route)
            self.assertIsInstance(dashboard_mount, Mount)

            index_response = asyncio.run(index_route.endpoint())
            health_payload = asyncio.run(health_route.endpoint())
            summary_payload = asyncio.run(summary_route.endpoint())

            self.assertEqual(index_response.status_code, 200)
            self.assertIn("/dashboard/styles.css", index_response.body.decode("utf-8"))
            self.assertIn("/dashboard/app.js", index_response.body.decode("utf-8"))
            self.assertIn("API Test", index_response.body.decode("utf-8"))
            self.assertEqual(health_payload["pairs"][0]["status"], "waiting")
            self.assertEqual(summary_payload["snapshots"], [])

            css_path = dashboard_mount.app.directory / "styles.css"
            js_path = dashboard_mount.app.directory / "app.js"
            self.assertTrue(css_path.exists())
            self.assertTrue(js_path.exists())
            self.assertIn("--bg", css_path.read_text(encoding="utf-8"))
            self.assertIn("async function fetchJson", js_path.read_text(encoding="utf-8"))

    def test_runtime_mode_polls_once_before_background_start(self) -> None:
        service = mock.Mock()
        service.config.app.name = "API Test"
        service.config.app.poll_interval_sec = 10
        service.context.dashboard_pairs = ["visible-pair"]
        service.poll_once = mock.AsyncMock()
        runtime = mock.Mock()
        runtime.start = mock.AsyncMock()
        runtime.stop = mock.AsyncMock()

        with mock.patch("cross_market_monitor.interfaces.api.app.MonitorRuntime", return_value=runtime):
            app = create_app(service, run_runtime=True)

            async def run_case() -> None:
                async with app.router.lifespan_context(app):
                    pass

            asyncio.run(run_case())

        service.poll_once.assert_awaited_once_with(pairs=["visible-pair"])
        runtime.start.assert_awaited_once_with(background_startup=True, initial_delay_sec=10)
        runtime.stop.assert_awaited_once_with()


if __name__ == "__main__":
    unittest.main()
