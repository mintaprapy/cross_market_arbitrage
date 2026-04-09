from __future__ import annotations

import json
from datetime import datetime

from cross_market_monitor.domain.models import FXQuote, JobRun, SourceHealth, SpreadSnapshot, WorkerRuntimeState


class SQLiteStateRepoMixin:
    def delete_latest_snapshots_for_groups(self, group_names: list[str]) -> None:
        if not group_names:
            return
        placeholders = ",".join("?" for _ in group_names)
        with self._lock, self._connect() as connection:
            connection.execute(
                f"DELETE FROM latest_snapshots WHERE group_name IN ({placeholders})",
                tuple(group_names),
            )

    def load_recent_spreads(self, group_name: str, limit: int) -> list[float]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT spread
                FROM spread_snapshots
                WHERE group_name = ? AND spread IS NOT NULL
                ORDER BY ts DESC
                LIMIT ?
                """,
                (group_name, limit),
            ).fetchall()
        return [row["spread"] for row in reversed(rows) if row["spread"] is not None]

    def load_recent_fx_rates(self, source_name: str, limit: int) -> list[float]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT rate
                FROM fx_rates
                WHERE source_name = ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (source_name, limit),
            ).fetchall()
        return [row["rate"] for row in reversed(rows) if row["rate"] is not None]

    def load_latest_fx_rate(self, source_name: str) -> FXQuote | None:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT ts, source_name, pair, rate, raw_payload
                FROM fx_rates
                WHERE source_name = ?
                ORDER BY ts DESC
                LIMIT 1
                """,
                (source_name,),
            ).fetchone()
        return self._row_to_fx_quote(row)

    def load_latest_fx_rate_any(self, source_names: list[str]) -> FXQuote | None:
        if not source_names:
            return None
        placeholders = ",".join("?" for _ in source_names)
        with self._lock, self._connect() as connection:
            row = connection.execute(
                f"""
                SELECT ts, source_name, pair, rate, raw_payload
                FROM fx_rates
                WHERE source_name IN ({placeholders})
                ORDER BY ts DESC
                LIMIT 1
                """,
                tuple(source_names),
            ).fetchone()
        return self._row_to_fx_quote(row)

    def load_latest_fx_rate_before_any(self, source_names: list[str], target_ts: datetime) -> FXQuote | None:
        if not source_names:
            return None
        target = self._timestamp_fields(target_ts, "UTC")[0]
        placeholders = ",".join("?" for _ in source_names)
        with self._lock, self._connect() as connection:
            row = connection.execute(
                f"""
                SELECT ts, source_name, pair, rate, raw_payload
                FROM fx_rates
                WHERE source_name IN ({placeholders}) AND ts <= ?
                ORDER BY ts DESC
                LIMIT 1
                """,
                (*tuple(source_names), target),
            ).fetchone()
        return self._row_to_fx_quote(row)

    def load_nearest_fx_rate(
        self,
        source_name: str,
        target_ts: datetime,
        *,
        max_delta_sec: float | None = None,
    ) -> FXQuote | None:
        target = self._timestamp_fields(target_ts, "UTC")[0]
        with self._lock, self._connect() as connection:
            before_row = connection.execute(
                """
                SELECT ts, source_name, pair, rate, raw_payload
                FROM fx_rates
                WHERE source_name = ? AND ts <= ?
                ORDER BY ts DESC
                LIMIT 1
                """,
                (source_name, target),
            ).fetchone()
            after_row = connection.execute(
                """
                SELECT ts, source_name, pair, rate, raw_payload
                FROM fx_rates
                WHERE source_name = ? AND ts >= ?
                ORDER BY ts ASC
                LIMIT 1
                """,
                (source_name, target),
            ).fetchone()

        candidates = [
            quote
            for quote in (self._row_to_fx_quote(before_row), self._row_to_fx_quote(after_row))
            if quote is not None
        ]
        if not candidates:
            return None
        selected = min(candidates, key=lambda quote: abs((quote.ts - target_ts).total_seconds()))
        if max_delta_sec is not None and abs((selected.ts - target_ts).total_seconds()) > max_delta_sec:
            return None
        return selected

    def load_nearest_fx_rate_any(
        self,
        source_names: list[str],
        target_ts: datetime,
        *,
        max_delta_sec: float | None = None,
    ) -> FXQuote | None:
        if not source_names:
            return None
        target = self._timestamp_fields(target_ts, "UTC")[0]
        placeholders = ",".join("?" for _ in source_names)
        params_prefix: tuple[object, ...] = tuple(source_names)
        with self._lock, self._connect() as connection:
            before_row = connection.execute(
                f"""
                SELECT ts, source_name, pair, rate, raw_payload
                FROM fx_rates
                WHERE source_name IN ({placeholders}) AND ts <= ?
                ORDER BY ts DESC
                LIMIT 1
                """,
                (*params_prefix, target),
            ).fetchone()
            after_row = connection.execute(
                f"""
                SELECT ts, source_name, pair, rate, raw_payload
                FROM fx_rates
                WHERE source_name IN ({placeholders}) AND ts >= ?
                ORDER BY ts ASC
                LIMIT 1
                """,
                (*params_prefix, target),
            ).fetchone()

        candidates = [
            quote
            for quote in (self._row_to_fx_quote(before_row), self._row_to_fx_quote(after_row))
            if quote is not None
        ]
        if not candidates:
            return None
        selected = min(candidates, key=lambda quote: abs((quote.ts - target_ts).total_seconds()))
        if max_delta_sec is not None and abs((selected.ts - target_ts).total_seconds()) > max_delta_sec:
            return None
        return selected

    def load_latest_raw_quote_before(
        self,
        group_name: str,
        leg_type: str,
        symbol: str,
        target_ts: datetime,
    ):
        target = self._timestamp_fields(target_ts, "UTC")[0]
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT ts, source_name, symbol, label, last_px, bid_px, ask_px, raw_payload
                FROM raw_quotes
                WHERE group_name = ? AND leg_type = ? AND symbol = ? AND ts <= ?
                ORDER BY ts DESC, id DESC
                LIMIT 1
                """,
                (group_name, leg_type, symbol, target),
            ).fetchone()
        return self._row_to_market_quote(row)

    def load_latest_snapshots(self) -> list[SpreadSnapshot]:
        with self._lock, self._connect() as connection:
            latest_rows = connection.execute(
                """
                SELECT s.*
                FROM latest_snapshots AS ls
                INNER JOIN spread_snapshots AS s
                    ON s.id = ls.snapshot_id
                ORDER BY s.group_name ASC
                """
            ).fetchall()
            if latest_rows:
                rows = latest_rows
            else:
                rows = connection.execute(
                    """
                    SELECT s.*
                    FROM spread_snapshots AS s
                    INNER JOIN (
                        SELECT group_name, MAX(ts) AS max_ts
                        FROM spread_snapshots
                        GROUP BY group_name
                    ) AS latest
                        ON s.group_name = latest.group_name
                       AND s.ts = latest.max_ts
                    ORDER BY s.group_name ASC, s.id DESC
                    """
                ).fetchall()
        decoded = self._decode_json_rows(rows, {"errors", "route_detail"})
        latest_by_group: dict[str, SpreadSnapshot] = {}
        for row in decoded:
            snapshot = SpreadSnapshot.model_validate(
                {
                    **row,
                    "ts": self._parse_timestamp(row["ts"]),
                    "ts_local": self._parse_timestamp(row["ts_local"]) if row.get("ts_local") else None,
                }
            )
            latest_by_group.setdefault(snapshot.group_name, snapshot)
        return [latest_by_group[group_name] for group_name in sorted(latest_by_group)]

    def load_route_preferences(self) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT group_name, leg_type, symbol, updated_at
                FROM route_preferences
                ORDER BY group_name ASC, leg_type ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_route_preference(self, group_name: str, leg_type: str, symbol: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO route_preferences (group_name, leg_type, symbol, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(group_name, leg_type) DO UPDATE SET
                    symbol = excluded.symbol,
                    updated_at = excluded.updated_at
                """,
                (
                    group_name,
                    leg_type,
                    symbol,
                    self._timestamp_fields_string()[0],
                ),
            )

    def delete_route_preference(self, group_name: str, leg_type: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                DELETE FROM route_preferences
                WHERE group_name = ? AND leg_type = ?
                """,
                (group_name, leg_type),
            )

    def upsert_runtime_state(self, state: WorkerRuntimeState) -> None:
        payload = state.model_dump(mode="json")
        updated_at = payload["last_heartbeat_at"] or payload["last_poll_finished_at"] or payload["started_at"]
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO runtime_state (state_name, payload, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(state_name) DO UPDATE SET
                    payload = excluded.payload,
                    updated_at = excluded.updated_at
                """,
                (
                    state.state_name,
                    json.dumps(payload, ensure_ascii=False),
                    updated_at,
                ),
            )

    def load_runtime_state(self, state_name: str = "worker") -> WorkerRuntimeState | None:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT payload
                FROM runtime_state
                WHERE state_name = ?
                """,
                (state_name,),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(row["payload"])
        return WorkerRuntimeState.model_validate(payload)

    def upsert_source_health(self, health: SourceHealth) -> None:
        payload = health.model_dump(mode="json")
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO source_health_state (
                    source_name, kind, success_count, failure_count, last_success_at, last_failure_at,
                    last_error, last_symbol, last_latency_ms, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_name) DO UPDATE SET
                    kind = excluded.kind,
                    success_count = excluded.success_count,
                    failure_count = excluded.failure_count,
                    last_success_at = excluded.last_success_at,
                    last_failure_at = excluded.last_failure_at,
                    last_error = excluded.last_error,
                    last_symbol = excluded.last_symbol,
                    last_latency_ms = excluded.last_latency_ms,
                    updated_at = excluded.updated_at
                """,
                (
                    health.source_name,
                    health.kind,
                    health.success_count,
                    health.failure_count,
                    payload["last_success_at"],
                    payload["last_failure_at"],
                    health.last_error,
                    health.last_symbol,
                    health.last_latency_ms,
                    payload["updated_at"],
                ),
            )

    def load_source_health_state(self) -> list[SourceHealth]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT source_name, kind, success_count, failure_count, last_success_at, last_failure_at,
                       last_error, last_symbol, last_latency_ms, updated_at
                FROM source_health_state
                ORDER BY source_name ASC
                """
            ).fetchall()
        return [SourceHealth.model_validate(dict(row)) for row in rows]

    def upsert_job_run(self, job_run: JobRun) -> None:
        payload = job_run.model_dump(mode="json")
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO job_runs (job_name, status, started_at, finished_at, details, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_name) DO UPDATE SET
                    status = excluded.status,
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    details = excluded.details,
                    updated_at = excluded.updated_at
                """,
                (
                    job_run.job_name,
                    job_run.status,
                    payload["started_at"],
                    payload["finished_at"],
                    json.dumps(job_run.details, ensure_ascii=False),
                    payload["updated_at"],
                ),
            )

    def load_job_runs(self) -> list[JobRun]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT job_name, status, started_at, finished_at, details, updated_at
                FROM job_runs
                ORDER BY job_name ASC
                """
            ).fetchall()
        decoded = self._decode_json_rows(rows, {"details"})
        return [JobRun.model_validate(dict(row)) for row in decoded]

    def delete_rows_before(self, table_name: str, ts_column: str, cutoff_ts: str) -> int:
        allowed_tables = {
            "raw_quotes": "ts",
            "fx_rates": "ts",
            "normalized_domestic_quotes": "ts",
            "spread_snapshots": "ts",
            "alert_events": "ts",
            "notification_deliveries": "ts",
        }
        if allowed_tables.get(table_name) != ts_column:
            raise ValueError(f"Unsupported retention target: {table_name}.{ts_column}")
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                f"DELETE FROM {table_name} WHERE {ts_column} < ?",
                (cutoff_ts,),
            )
        return int(cursor.rowcount)

    def compact_rows_by_bucket(
        self,
        table_name: str,
        *,
        bucket_seconds: int,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> int:
        table_specs = {
            "raw_quotes": ("group_name", "leg_type", "source_name", "symbol"),
            "fx_rates": ("source_name", "pair"),
            "normalized_domestic_quotes": ("group_name", "source_name", "symbol", "tax_mode", "target_unit"),
            "spread_snapshots": ("group_name", "domestic_symbol", "overseas_symbol", "tax_mode"),
        }
        partition_columns = table_specs.get(table_name)
        if partition_columns is None:
            raise ValueError(f"Unsupported compaction target: {table_name}")

        bucket_seconds = max(int(bucket_seconds), 1)
        clauses: list[str] = []
        params: list[object] = []
        if start_ts is not None:
            clauses.append("ts >= ?")
            params.append(start_ts)
        if end_ts is not None:
            clauses.append("ts < ?")
            params.append(end_ts)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        partition_sql = ", ".join(
            [*partition_columns, f"CAST(strftime('%s', ts) AS INTEGER) / {bucket_seconds}"]
        )

        query = f"""
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_sql}
                        ORDER BY ts DESC, id DESC
                    ) AS row_number
                FROM {table_name}
                {where_sql}
            )
            DELETE FROM {table_name}
            WHERE id IN (
                SELECT id
                FROM ranked
                WHERE row_number > 1
            )
        """
        with self._lock, self._connect() as connection:
            before_changes = connection.total_changes
            connection.execute(query, tuple(params))
            after_changes = connection.total_changes
        return int(after_changes - before_changes)

    def checkpoint_wal(self) -> None:
        with self._lock, self._connect() as connection:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def rebuild_latest_snapshots(self, group_names: list[str] | None = None) -> None:
        with self._lock, self._connect() as connection:
            connection.execute("DELETE FROM latest_snapshots")
            if group_names:
                placeholders = ",".join("?" for _ in group_names)
                connection.execute(
                    f"""
                INSERT INTO latest_snapshots (group_name, snapshot_id, ts)
                SELECT s.group_name, s.id, s.ts
                FROM spread_snapshots AS s
                INNER JOIN (
                    SELECT group_name, MAX(id) AS latest_id
                    FROM spread_snapshots
                    WHERE group_name IN ({placeholders})
                    GROUP BY group_name
                ) AS latest
                    ON latest.group_name = s.group_name
                   AND latest.latest_id = s.id
                """,
                    tuple(group_names),
                )
                return
            connection.execute(
                """
                INSERT INTO latest_snapshots (group_name, snapshot_id, ts)
                SELECT s.group_name, s.id, s.ts
                FROM spread_snapshots AS s
                INNER JOIN (
                    SELECT group_name, MAX(id) AS latest_id
                    FROM spread_snapshots
                    GROUP BY group_name
                ) AS latest
                    ON latest.group_name = s.group_name
                   AND latest.latest_id = s.id
                """
            )
