#!/usr/bin/env python3
"""Print current SQLite size and core table statistics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from db_metrics_common import DEFAULT_CONFIG, collect_db_snapshot, human_bytes, resolve_db_path


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
        "--json",
        action="store_true",
        help="Emit JSON instead of human-readable text.",
    )
    return parser.parse_args()


def print_text(snapshot: dict) -> None:
    print(f"generated_at: {snapshot['generated_at']}")
    print(f"db_path: {snapshot['db_path']}")
    print(f"exists: {snapshot['exists']}")
    if not snapshot["exists"]:
        return

    print(
        "disk_size:"
        f" db={human_bytes(snapshot['db_file_bytes'])}"
        f" wal={human_bytes(snapshot['wal_file_bytes'])}"
        f" shm={human_bytes(snapshot['shm_file_bytes'])}"
        f" total={human_bytes(snapshot['total_disk_bytes'])}"
    )
    print(
        "sqlite_pages:"
        f" page_size={snapshot.get('page_size', 0)}"
        f" page_count={snapshot.get('page_count', 0)}"
        f" freelist={snapshot.get('freelist_count', 0)}"
        f" logical={human_bytes(snapshot.get('logical_db_bytes', 0))}"
    )
    print("table_rows:")
    for table, count in snapshot["table_rows"].items():
        print(f"  {table}: {count}")
    if snapshot["table_bytes"]:
        print("table_bytes:")
        for table, size in sorted(snapshot["table_bytes"].items(), key=lambda item: item[1], reverse=True):
            print(f"  {table}: {human_bytes(size)}")


def main() -> int:
    args = parse_args()
    db_path = resolve_db_path(args.config.resolve(), args.db_path)
    snapshot = collect_db_snapshot(db_path)
    if args.json:
        print(json.dumps(snapshot, ensure_ascii=False, indent=2))
    else:
        print_text(snapshot)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
