#!/usr/bin/env python3
"""Record one SQLite growth sample and compare it to the previous sample."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path

from db_metrics_common import DEFAULT_CONFIG, ROOT, collect_db_snapshot, human_bytes, resolve_db_path


SAMPLE_FIELDS = (
    "generated_at",
    "db_path",
    "db_file_bytes",
    "wal_file_bytes",
    "shm_file_bytes",
    "total_disk_bytes",
    "page_size",
    "page_count",
    "freelist_count",
    "logical_db_bytes",
    "raw_quotes_rows",
    "fx_rates_rows",
    "normalized_domestic_quotes_rows",
    "spread_snapshots_rows",
    "alert_events_rows",
    "notification_deliveries_rows",
    "latest_snapshots_rows",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to monitor config. Default: %(default)s",
    )
    parser.add_argument(
        "--db-path",
        help="Optional explicit SQLite path. Overrides config.",
    )
    parser.add_argument(
        "--samples-path",
        type=Path,
        default=ROOT / "logs" / "db_growth" / "samples.csv",
        help="CSV file that stores historical samples. Default: %(default)s",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON summary.",
    )
    return parser.parse_args()


def flatten_snapshot(snapshot: dict) -> dict[str, str | int]:
    table_rows = snapshot.get("table_rows", {})
    return {
        "generated_at": snapshot["generated_at"],
        "db_path": snapshot["db_path"],
        "db_file_bytes": int(snapshot.get("db_file_bytes", 0)),
        "wal_file_bytes": int(snapshot.get("wal_file_bytes", 0)),
        "shm_file_bytes": int(snapshot.get("shm_file_bytes", 0)),
        "total_disk_bytes": int(snapshot.get("total_disk_bytes", 0)),
        "page_size": int(snapshot.get("page_size", 0)),
        "page_count": int(snapshot.get("page_count", 0)),
        "freelist_count": int(snapshot.get("freelist_count", 0)),
        "logical_db_bytes": int(snapshot.get("logical_db_bytes", 0)),
        "raw_quotes_rows": int(table_rows.get("raw_quotes", 0)),
        "fx_rates_rows": int(table_rows.get("fx_rates", 0)),
        "normalized_domestic_quotes_rows": int(table_rows.get("normalized_domestic_quotes", 0)),
        "spread_snapshots_rows": int(table_rows.get("spread_snapshots", 0)),
        "alert_events_rows": int(table_rows.get("alert_events", 0)),
        "notification_deliveries_rows": int(table_rows.get("notification_deliveries", 0)),
        "latest_snapshots_rows": int(table_rows.get("latest_snapshots", 0)),
    }


def read_last_sample(samples_path: Path) -> dict[str, str] | None:
    if not samples_path.exists():
        return None
    with samples_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return rows[-1] if rows else None


def append_sample(samples_path: Path, sample: dict[str, str | int]) -> None:
    samples_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = samples_path.exists()
    with samples_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SAMPLE_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(sample)


def build_delta(previous: dict[str, str] | None, current: dict[str, str | int]) -> dict:
    if previous is None:
        return {"has_previous": False}

    previous_ts = datetime.fromisoformat(previous["generated_at"])
    current_ts = datetime.fromisoformat(str(current["generated_at"]))
    elapsed_seconds = (current_ts - previous_ts).total_seconds()
    elapsed_hours = elapsed_seconds / 3600 if elapsed_seconds > 0 else 0
    elapsed_days = elapsed_seconds / 86400 if elapsed_seconds > 0 else 0

    def delta(field: str) -> int:
        return int(current[field]) - int(previous[field])

    total_disk_delta = delta("total_disk_bytes")
    result = {
        "has_previous": True,
        "elapsed_seconds": elapsed_seconds,
        "elapsed_hours": elapsed_hours,
        "elapsed_days": elapsed_days,
        "total_disk_delta_bytes": total_disk_delta,
        "total_disk_delta_per_hour": (total_disk_delta / elapsed_hours) if elapsed_hours > 0 else None,
        "total_disk_delta_per_day": (total_disk_delta / elapsed_days) if elapsed_days > 0 else None,
        "row_deltas": {
            "raw_quotes": delta("raw_quotes_rows"),
            "fx_rates": delta("fx_rates_rows"),
            "normalized_domestic_quotes": delta("normalized_domestic_quotes_rows"),
            "spread_snapshots": delta("spread_snapshots_rows"),
            "alert_events": delta("alert_events_rows"),
            "notification_deliveries": delta("notification_deliveries_rows"),
        },
    }
    if elapsed_days > 0:
        result["row_deltas_per_day"] = {
            name: value / elapsed_days for name, value in result["row_deltas"].items()
        }
    else:
        result["row_deltas_per_day"] = {}
    return result


def print_text(current: dict[str, str | int], delta: dict) -> None:
    print(f"sample_at: {current['generated_at']}")
    print(f"db_path: {current['db_path']}")
    print(
        "disk_size:"
        f" total={human_bytes(int(current['total_disk_bytes']))}"
        f" db={human_bytes(int(current['db_file_bytes']))}"
        f" wal={human_bytes(int(current['wal_file_bytes']))}"
        f" shm={human_bytes(int(current['shm_file_bytes']))}"
    )
    print(
        "row_counts:"
        f" raw={current['raw_quotes_rows']}"
        f" normalized={current['normalized_domestic_quotes_rows']}"
        f" snapshots={current['spread_snapshots_rows']}"
        f" fx={current['fx_rates_rows']}"
    )

    if not delta["has_previous"]:
        print("delta: no previous sample, baseline recorded")
        return

    print(
        "delta:"
        f" elapsed={delta['elapsed_hours']:.2f}h"
        f" total={human_bytes(int(delta['total_disk_delta_bytes']))}"
        f" total_per_day={human_bytes(int(delta['total_disk_delta_per_day'])) if delta['total_disk_delta_per_day'] is not None else 'n/a'}"
    )
    print("row_deltas:")
    for name, value in delta["row_deltas"].items():
        per_day = delta["row_deltas_per_day"].get(name)
        if per_day is None:
            print(f"  {name}: {value}")
            continue
        print(f"  {name}: {value} ({per_day:.1f}/day)")


def main() -> int:
    args = parse_args()
    db_path = resolve_db_path(args.config.resolve(), args.db_path)
    snapshot = collect_db_snapshot(db_path)
    current = flatten_snapshot(snapshot)
    last_sample = read_last_sample(args.samples_path.resolve())
    delta = build_delta(last_sample, current)
    append_sample(args.samples_path.resolve(), current)

    if args.json:
        print(json.dumps({"current": current, "delta": delta}, ensure_ascii=False, indent=2))
    else:
        print_text(current, delta)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
