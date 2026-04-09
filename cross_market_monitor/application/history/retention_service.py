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
        enabled_group_names = [
            pair.group_name
            for pair in self.context.config.pairs
            if pair.enabled
        ]
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
            compacted_rows: dict[str, dict[str, int]] = {}
            realtime_days = max(int(self.context.config.app.timeseries_realtime_days), 0)
            daily_after_days = max(int(self.context.config.app.timeseries_daily_after_days), realtime_days + 1)
            intraday_bucket_seconds = max(int(self.context.config.app.timeseries_intraday_bucket_minutes), 1) * 60
            archive_bucket_seconds = max(int(self.context.config.app.timeseries_archive_bucket_days), 1) * 86400

            realtime_cutoff_ts = (started - timedelta(days=realtime_days)).isoformat()
            archive_cutoff_ts = (started - timedelta(days=daily_after_days)).isoformat()

            timeseries_tables = [
                "raw_quotes",
                "fx_rates",
                "normalized_domestic_quotes",
                "spread_snapshots",
            ]
            for table_name in timeseries_tables:
                daily_compacted = self.context.repository.compact_rows_by_bucket(
                    table_name,
                    bucket_seconds=archive_bucket_seconds,
                    end_ts=archive_cutoff_ts,
                )
                intraday_compacted = self.context.repository.compact_rows_by_bucket(
                    table_name,
                    bucket_seconds=intraday_bucket_seconds,
                    start_ts=archive_cutoff_ts,
                    end_ts=realtime_cutoff_ts,
                )
                compacted_rows[table_name] = {
                    "intraday_bucket_rows_deleted": intraday_compacted,
                    "archive_bucket_rows_deleted": daily_compacted,
                }

            deleted_rows: dict[str, int] = {}
            retention_targets = [
                ("alert_events", "ts", self.context.config.app.alert_retention_days),
                ("notification_deliveries", "ts", self.context.config.app.delivery_retention_days),
            ]
            for table_name, ts_column, retention_days in retention_targets:
                if retention_days <= 0:
                    deleted_rows[table_name] = 0
                    continue
                cutoff_ts = (started - timedelta(days=retention_days)).isoformat()
                deleted_rows[table_name] = self.context.repository.delete_rows_before(table_name, ts_column, cutoff_ts)

            self.context.repository.rebuild_latest_snapshots(enabled_group_names)
            self.context.repository.checkpoint_wal()
            finished = utc_now()
            self.context.retention_last_run_at = finished
            report = {
                "compacted_rows": compacted_rows,
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
