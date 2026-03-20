from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - optional dependency
    pa = None
    pq = None


class SQLiteQueryRepoMixin:
    def fetch_raw_quote_history(
        self,
        group_name: str,
        leg_type: str,
        *,
        symbol: str | None = None,
        limit: int | None = 300,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> list[dict]:
        clauses = ["group_name = ?", "leg_type = ?"]
        params: list[object] = [group_name, leg_type]
        if symbol is not None:
            clauses.append("symbol = ?")
            params.append(symbol)
        if start_ts is not None:
            clauses.append("ts >= ?")
            params.append(start_ts)
        if end_ts is not None:
            clauses.append("ts <= ?")
            params.append(end_ts)

        query = f"""
            SELECT
                id,
                ts,
                ts_utc,
                ts_local,
                source_name,
                symbol,
                label,
                last_px,
                bid_px,
                ask_px
            FROM raw_quotes
            WHERE {" AND ".join(clauses)}
        """
        if limit is None:
            query += "\nORDER BY ts ASC"
            query_params: tuple[object, ...] = tuple(params)
        else:
            query += "\nORDER BY id DESC\nLIMIT ?"
            query_params = (*params, limit)

        with self._lock, self._connect() as connection:
            rows = connection.execute(query, query_params).fetchall()
        results = [dict(row) for row in rows]
        deduped_by_ts: dict[str, dict] = {}
        for row in results:
            key = str(row["ts"])
            previous = deduped_by_ts.get(key)
            if previous is None or int(row["id"]) > int(previous["id"]):
                deduped_by_ts[key] = row
        results = sorted(deduped_by_ts.values(), key=lambda row: (row["ts"], row["id"]))
        return results if limit is None else results

    def fetch_normalized_domestic_history(
        self,
        group_name: str,
        *,
        symbol: str | None = None,
        limit: int | None = 300,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> list[dict]:
        clauses = ["group_name = ?"]
        params: list[object] = [group_name]
        if symbol is not None:
            clauses.append("symbol = ?")
            params.append(symbol)
        if start_ts is not None:
            clauses.append("ts >= ?")
            params.append(start_ts)
        if end_ts is not None:
            clauses.append("ts <= ?")
            params.append(end_ts)

        query = f"""
            SELECT
                id,
                ts,
                ts_utc,
                ts_local,
                source_name,
                symbol,
                label,
                fx_source,
                fx_rate,
                formula,
                formula_version,
                tax_mode,
                target_unit,
                raw_last,
                raw_bid,
                raw_ask,
                normalized_last,
                normalized_bid,
                normalized_ask
            FROM normalized_domestic_quotes
            WHERE {" AND ".join(clauses)}
        """
        if limit is None:
            query += "\nORDER BY ts ASC"
            query_params: tuple[object, ...] = tuple(params)
        else:
            query += "\nORDER BY id DESC\nLIMIT ?"
            query_params = (*params, limit)

        with self._lock, self._connect() as connection:
            rows = connection.execute(query, query_params).fetchall()
        results = [dict(row) for row in rows]
        return results if limit is None else list(reversed(results))

    def fetch_fx_history(
        self,
        source_name: str,
        *,
        limit: int | None = 300,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> list[dict]:
        clauses = ["source_name = ?"]
        params: list[object] = [source_name]
        if start_ts is not None:
            clauses.append("ts >= ?")
            params.append(start_ts)
        if end_ts is not None:
            clauses.append("ts <= ?")
            params.append(end_ts)

        query = f"""
            SELECT id, ts, ts_utc, ts_local, source_name, pair, rate
            FROM fx_rates
            WHERE {" AND ".join(clauses)}
        """
        if limit is None:
            query += "\nORDER BY ts ASC"
            query_params: tuple[object, ...] = tuple(params)
        else:
            query += "\nORDER BY id DESC\nLIMIT ?"
            query_params = (*params, limit)

        with self._lock, self._connect() as connection:
            rows = connection.execute(query, query_params).fetchall()
        results = [dict(row) for row in rows]
        return results if limit is None else list(reversed(results))

    def fetch_history(
        self,
        group_name: str,
        limit: int | None,
        *,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> list[dict]:
        where, params = self._build_filters(group_name=group_name, start_ts=start_ts, end_ts=end_ts)
        query = f"""
            SELECT
                ts,
                ts_utc,
                ts_local,
                domestic_symbol,
                overseas_symbol,
                spread,
                spread_pct,
                zscore,
                domestic_last_raw,
                normalized_last,
                overseas_last,
                status,
                signal_state,
                pause_reason
            FROM spread_snapshots
            {where}
        """
        if limit is None:
            query += "\nORDER BY ts ASC"
            query_params: tuple[object, ...] = tuple(params)
        else:
            query += "\nORDER BY ts DESC\nLIMIT ?"
            query_params = (*params, limit)

        with self._lock, self._connect() as connection:
            rows = connection.execute(query, query_params).fetchall()
        results = [dict(row) for row in rows]
        return results if limit is None else list(reversed(results))

    def fetch_alerts(self, limit: int = 100) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT ts, ts_utc, ts_local, group_name, category, severity, message, metadata
                FROM alert_events
                ORDER BY ts DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return self._decode_json_rows(rows, {"metadata"})

    def fetch_notification_deliveries(self, limit: int = 100) -> list[dict]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT ts, ts_utc, ts_local, notifier_name, group_name, category, severity, success, response_message, payload
                FROM notification_deliveries
                ORDER BY ts DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        results = self._decode_json_rows(rows, {"payload"})
        for row in results:
            row["success"] = bool(row["success"])
        return results

    def fetch_snapshots(
        self,
        group_name: str | None = None,
        limit: int = 300,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> list[dict]:
        where, params = self._build_filters(group_name=group_name, start_ts=start_ts, end_ts=end_ts)
        query = f"""
            SELECT *
            FROM spread_snapshots
            {where}
            ORDER BY ts DESC
            LIMIT ?
        """
        params.append(limit)
        with self._lock, self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return self._decode_json_rows(reversed(rows), {"errors", "route_detail"})

    def export_dataset_to_csv(
        self,
        dataset: str,
        output_path: str,
        *,
        group_name: str | None = None,
        limit: int = 5000,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> int:
        rows = self._fetch_dataset_rows(dataset, group_name=group_name, limit=limit, start_ts=start_ts, end_ts=end_ts)
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("w", newline="", encoding="utf-8") as handle:
            if not rows:
                handle.write("")
                return 0
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        return len(rows)

    def export_dataset_to_parquet(
        self,
        dataset: str,
        output_path: str,
        *,
        group_name: str | None = None,
        limit: int = 5000,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> int:
        if pa is None or pq is None:
            raise RuntimeError("pyarrow is required for Parquet export but is not installed")
        rows = self._fetch_dataset_rows(dataset, group_name=group_name, limit=limit, start_ts=start_ts, end_ts=end_ts)
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        parquet_rows = [self._coerce_parquet_row(row) for row in rows]
        table = pa.Table.from_pylist(parquet_rows)
        pq.write_table(table, destination)
        return len(rows)

    def _fetch_dataset_rows(
        self,
        dataset: str,
        *,
        group_name: str | None = None,
        limit: int = 5000,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> list[dict]:
        table = {
            "snapshots": "spread_snapshots",
            "alerts": "alert_events",
            "raw_quotes": "raw_quotes",
            "fx_rates": "fx_rates",
            "notification_deliveries": "notification_deliveries",
        }.get(dataset)
        if table is None:
            raise ValueError(f"Unsupported dataset: {dataset}")

        where, params = self._build_filters(
            group_name=group_name if table != "fx_rates" else None,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        query = f"SELECT * FROM {table} {where} ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        with self._lock, self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        json_columns = {
            "spread_snapshots": {"errors", "route_detail"},
            "alert_events": {"metadata"},
            "notification_deliveries": {"payload"},
        }.get(table, set())
        return self._decode_json_rows(reversed(rows), json_columns)

    def _build_filters(
        self,
        *,
        group_name: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> tuple[str, list]:
        conditions: list[str] = []
        params: list = []
        if group_name:
            conditions.append("group_name = ?")
            params.append(group_name)
        if start_ts:
            conditions.append("ts >= ?")
            params.append(start_ts)
        if end_ts:
            conditions.append("ts <= ?")
            params.append(end_ts)
        if not conditions:
            return "", params
        return "WHERE " + " AND ".join(conditions), params

    def _decode_json_rows(self, rows, json_columns: set[str]) -> list[dict]:
        results = [dict(row) for row in rows]
        for row in results:
            for column in json_columns:
                if column in row and row[column]:
                    row[column] = json.loads(row[column])
        return results

    def _coerce_parquet_row(self, row: dict) -> dict:
        result: dict = {}
        for key, value in row.items():
            if isinstance(value, (dict, list)):
                result[key] = json.dumps(value, ensure_ascii=False)
            else:
                result[key] = value
        return result
