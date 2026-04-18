from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from cross_market_monitor.application.monitor.summary_cache import SummaryCacheService


class SummaryCacheServiceTests(unittest.TestCase):
    def test_write_latest_summary_writes_json_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            export_dir = Path(tmp_dir) / "exports"

            class QueryStub:
                def get_snapshot_summary(self) -> dict:
                    return {
                        "as_of": "2026-04-18T00:00:00+00:00",
                        "health": {"poll_interval_sec": 10, "sources": []},
                        "snapshots": [],
                    }

            service = SummaryCacheService(QueryStub(), str(export_dir))
            path = service.write_latest_summary()

            self.assertEqual(path, export_dir / "summary" / "latest.json")
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["as_of"], "2026-04-18T00:00:00+00:00")
            self.assertEqual(payload["_snapshot_source"], "summary-cache")
            self.assertIn("_generated_at", payload)
