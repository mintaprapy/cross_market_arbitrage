from __future__ import annotations

import json
from pathlib import Path

from cross_market_monitor.application.common import utc_now


class SummaryCacheService:
    def __init__(self, query_service, export_dir: str) -> None:
        self.query_service = query_service
        self.export_dir = Path(export_dir)

    @property
    def summary_dir(self) -> Path:
        return self.export_dir / "summary"

    @property
    def latest_summary_path(self) -> Path:
        return self.summary_dir / "latest.json"

    def write_latest_summary(self) -> Path:
        payload = dict(self.query_service.get_snapshot_summary())
        payload["_snapshot_source"] = "summary-cache"
        payload["_generated_at"] = utc_now().isoformat()
        self.summary_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = self.latest_summary_path.with_suffix(".json.tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(self.latest_summary_path)
        return self.latest_summary_path
