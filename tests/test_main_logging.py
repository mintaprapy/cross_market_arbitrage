from __future__ import annotations

import logging
import unittest
from datetime import UTC, datetime

from cross_market_monitor.main import TimezoneFormatter, build_uvicorn_log_config


class TestMainLogging(unittest.TestCase):
    def test_timezone_formatter_uses_shanghai_time(self) -> None:
        formatter = TimezoneFormatter(
            "%(asctime)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            timezone_name="Asia/Shanghai",
        )
        record = logging.LogRecord(
            name="cross_market_monitor",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )
        record.created = datetime(2026, 3, 28, 9, 34, 10, tzinfo=UTC).timestamp()
        rendered = formatter.format(record)
        self.assertEqual(rendered, "2026-03-28 17:34:10 hello")

    def test_uvicorn_log_config_uses_timezone_formatter(self) -> None:
        log_config = build_uvicorn_log_config("Asia/Shanghai")
        self.assertIs(log_config["formatters"]["default"]["()"], TimezoneFormatter)
        self.assertIs(log_config["formatters"]["access"]["()"], TimezoneFormatter)
        self.assertEqual(log_config["formatters"]["default"]["timezone_name"], "Asia/Shanghai")
        self.assertEqual(log_config["formatters"]["access"]["timezone_name"], "Asia/Shanghai")


if __name__ == "__main__":
    unittest.main()
