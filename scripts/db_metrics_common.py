#!/usr/bin/env python3
"""Shared helpers for SQLite size and growth diagnostics."""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cross_market_monitor.infrastructure.config_loader import load_config


DEFAULT_CONFIG = ROOT / "config" / "monitor.yaml"
CORE_TABLES = (
    "raw_quotes",
    "fx_rates",
    "normalized_domestic_quotes",
    "spread_snapshots",
    "alert_events",
    "notification_deliveries",
    "latest_snapshots",
)


def human_bytes(value: int) -> str:
    sign = "-" if value < 0 else ""
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(abs(value))
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    if unit == "B":
        return f"{sign}{int(size)} {unit}"
    return f"{sign}{size:.2f} {unit}"


def resolve_db_path(config_path: Path, db_path: str | None) -> Path:
    if db_path:
        candidate = Path(db_path)
        return candidate if candidate.is_absolute() else (ROOT / candidate).resolve()
    config = load_config(str(config_path))
    candidate = Path(config.app.sqlite_path)
    return candidate if candidate.is_absolute() else (ROOT / candidate).resolve()


def file_sizes(db_path: Path) -> dict[str, int]:
    files = {
        "db_file_bytes": db_path.stat().st_size if db_path.exists() else 0,
        "wal_file_bytes": 0,
        "shm_file_bytes": 0,
    }
    wal_path = db_path.with_name(f"{db_path.name}-wal")
    shm_path = db_path.with_name(f"{db_path.name}-shm")
    if wal_path.exists():
        files["wal_file_bytes"] = wal_path.stat().st_size
    if shm_path.exists():
        files["shm_file_bytes"] = shm_path.stat().st_size
    files["total_disk_bytes"] = (
        files["db_file_bytes"] + files["wal_file_bytes"] + files["shm_file_bytes"]
    )
    return files


def collect_db_snapshot(db_path: Path) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "db_path": str(db_path),
        "exists": db_path.exists(),
    }
    snapshot.update(file_sizes(db_path))
    snapshot["table_rows"] = {}
    snapshot["table_bytes"] = {}

    if not db_path.exists():
        return snapshot

    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        snapshot["page_size"] = page_size
        snapshot["page_count"] = page_count
        snapshot["freelist_count"] = freelist_count
        snapshot["logical_db_bytes"] = page_size * page_count

        existing_tables = {
            str(row["name"])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        for table in CORE_TABLES:
            if table not in existing_tables:
                snapshot["table_rows"][table] = 0
                continue
            snapshot["table_rows"][table] = int(
                connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )

        try:
            rows = connection.execute(
                """
                SELECT name, SUM(pgsize) AS bytes
                FROM dbstat
                GROUP BY name
                ORDER BY bytes DESC
                """
            ).fetchall()
            snapshot["table_bytes"] = {
                str(row["name"]): int(row["bytes"])
                for row in rows
                if row["name"] in CORE_TABLES
            }
            snapshot["index_bytes"] = {
                str(row["name"]): int(row["bytes"])
                for row in rows
                if str(row["name"]).startswith("idx_") or str(row["name"]).startswith("sqlite_autoindex")
            }
        except sqlite3.DatabaseError:
            snapshot["table_bytes"] = {}
            snapshot["index_bytes"] = {}

    return snapshot
