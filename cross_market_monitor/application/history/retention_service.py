from __future__ import annotations

from datetime import timedelta

from cross_market_monitor.application.common import utc_now
from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.domain.models import JobRun


class RetentionService:
    def __init__(self, context: ServiceContext) -> None:
        self.context = context

    def maybe_run(self, *, force: bool = False) -> dict | None:
        if not self.context.config.app.retention_enabled:
            return None
        now = utc_now()
        if (
            not force
            and self.context.retention_last_run_at is not None
            and (now - self.context.retention_last_run_at).total_seconds() < self.context.config.app.retention_interval_sec
        ):
            return None
        return self.run_once(started_at=now)

    def run_once(self, *, started_at=None) -> dict:
        started = started_at or utc_now()
        self.context.repository.upsert_job_run(
            JobRun(
                job_name="retention",
                status="running",
                started_at=started,
                finished_at=None,
                updated_at=started,
                details={},
            )
        )
        try:
            deleted_rows: dict[str, int] = {}
            retention_targets = [
                ("raw_quotes", "ts", self.context.config.app.raw_quote_retention_days),
                ("fx_rates", "ts", self.context.config.app.fx_rate_retention_days),
                ("normalized_domestic_quotes", "ts", self.context.config.app.normalized_quote_retention_days),
                ("spread_snapshots", "ts", self.context.config.app.snapshot_retention_days),
                ("alert_events", "ts", self.context.config.app.alert_retention_days),
                ("notification_deliveries", "ts", self.context.config.app.delivery_retention_days),
            ]
            for table_name, ts_column, retention_days in retention_targets:
                if retention_days <= 0:
                    deleted_rows[table_name] = 0
                    continue
                cutoff_ts = (started - timedelta(days=retention_days)).isoformat()
                deleted_rows[table_name] = self.context.repository.delete_rows_before(table_name, ts_column, cutoff_ts)

            self.context.repository.rebuild_latest_snapshots()
            self.context.repository.checkpoint_wal()
            finished = utc_now()
            self.context.retention_last_run_at = finished
            report = {
                "deleted_rows": deleted_rows,
                "finished_at": finished.isoformat(),
            }
            self.context.repository.upsert_job_run(
                JobRun(
                    job_name="retention",
                    status="succeeded",
                    started_at=started,
                    finished_at=finished,
                    updated_at=finished,
                    details=report,
                )
            )
            return report
        except Exception as exc:
            failed_at = utc_now()
            self.context.repository.upsert_job_run(
                JobRun(
                    job_name="retention",
                    status="failed",
                    started_at=started,
                    finished_at=failed_at,
                    updated_at=failed_at,
                    details={"error": str(exc)},
                )
            )
            raise
