#!/usr/bin/env python3
"""Export a runtime diagnostic bundle for the cross-market monitor."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import tarfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cross_market_monitor.infrastructure.config_loader import load_config


DEFAULT_CONFIG = ROOT / "config" / "monitor.yaml"
DEFAULT_OUTPUT_ROOT = ROOT / "exports"
DEFAULT_HOURS = 12
DEFAULT_API_BASE_URL = "http://127.0.0.1:6080"
DEFAULT_SERVICES = ("cross-market-monitor",)
DEFAULT_EXTRA_SERVICES = ("nginx", "systemd-resolved")
ERROR_LIKE_PATTERNS = (
    re.compile(r"\berror\b", re.IGNORECASE),
    re.compile(r"traceback", re.IGNORECASE),
    re.compile(r"\bexception\b", re.IGNORECASE),
    re.compile(r"\b(?:timed out|timeout|failed|refused|reset by peer)\b", re.IGNORECASE),
    re.compile(r"\b(?:5\d{2}|429|403)\b"),
)


@dataclass
class CommandResult:
    cmd: list[str]
    returncode: int
    stdout_path: str
    stderr_path: str


def now_local() -> datetime:
    return datetime.now().astimezone()


def sanitize_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip()) or "unknown"


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, payload: Any) -> None:
    write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))


def run_command(cmd: list[str], output_dir: Path, stem: str) -> CommandResult:
    stdout_path = output_dir / f"{stem}.out.txt"
    stderr_path = output_dir / f"{stem}.err.txt"
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        write_text(stdout_path, proc.stdout)
        write_text(stderr_path, proc.stderr)
        returncode = proc.returncode
    except FileNotFoundError as exc:
        write_text(stdout_path, "")
        write_text(stderr_path, f"{exc}\n")
        returncode = 127
    return CommandResult(
        cmd=cmd,
        returncode=returncode,
        stdout_path=stdout_path.name,
        stderr_path=stderr_path.name,
    )


def collect_system_info(output_dir: Path, config_path: Path, config) -> None:
    payload = {
        "generated_at": now_local().isoformat(),
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": sys.version,
        "cwd": os.getcwd(),
        "repo_root": str(ROOT),
        "config_path": str(config_path),
        "sqlite_path": str(config.app.sqlite_path),
        "export_dir": str(config.app.export_dir),
        "api_bind": f"{config.app.bind_host}:{config.app.bind_port}",
        "fx_source": config.app.fx_source,
        "fx_backup_sources": list(config.app.fx_backup_sources),
    }
    write_json(output_dir / "environment.json", payload)


def collect_service_artifacts(service: str, hours: int, output_dir: Path) -> dict[str, Any]:
    stem = sanitize_name(service)
    results = {
        "status": run_command(["systemctl", "status", service, "--no-pager"], output_dir, f"{stem}.systemctl_status"),
        "is_active": run_command(["systemctl", "is-active", service], output_dir, f"{stem}.systemctl_is_active"),
        "show": run_command(["systemctl", "show", service], output_dir, f"{stem}.systemctl_show"),
        "cat": run_command(["systemctl", "cat", service], output_dir, f"{stem}.systemctl_cat"),
        "journal": run_command(
            ["journalctl", "-u", service, "--since", f"{hours} hours ago", "--no-pager"],
            output_dir,
            f"{stem}.journal_last_{hours}h",
        ),
    }
    return {key: asdict(value) for key, value in results.items()}


def summarize_journal(journal_path: Path, output_dir: Path, stem: str) -> dict[str, Any]:
    if not journal_path.exists():
        summary = {"exists": False}
        write_json(output_dir / f"{stem}.summary.json", summary)
        return summary

    lines = journal_path.read_text(encoding="utf-8", errors="replace").splitlines()
    relevant = [line for line in lines if any(pattern.search(line) for pattern in ERROR_LIKE_PATTERNS)]
    top_messages = Counter(_normalize_journal_line(line) for line in relevant).most_common(20)
    summary = {
        "exists": True,
        "line_count": len(lines),
        "error_like_count": len(relevant),
        "top_messages": top_messages,
    }
    write_json(output_dir / f"{stem}.summary.json", summary)
    write_text(output_dir / f"{stem}.relevant.txt", "\n".join(relevant) + ("\n" if relevant else ""))
    return summary


def _normalize_journal_line(line: str) -> str:
    line = re.sub(r"^\w{3}\s+\d+\s+\d+:\d+:\d+\s+[^ ]+\s+", "", line)
    line = re.sub(r"\[[0-9]+\]", "[]", line)
    return line.strip()


def copy_optional_file(src: Path, dst: Path) -> None:
    if src.exists():
        shutil.copy2(src, dst)


def iter_import_paths(config_path: Path) -> list[Path]:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    imports = list(raw.get("imports") or []) + list(raw.get("optional_imports") or [])
    paths: list[Path] = []
    for item in imports:
        if not isinstance(item, str):
            continue
        import_path = Path(item)
        if not import_path.is_absolute():
            import_path = (config_path.parent / import_path).resolve()
        if import_path.exists():
            paths.append(import_path)
    return paths


def collect_repo_artifacts(output_dir: Path, config_path: Path, config) -> None:
    copy_optional_file(config_path, output_dir / "monitor.yaml")
    for imported_path in iter_import_paths(config_path):
        copy_optional_file(imported_path, output_dir / imported_path.name)
    for example_path in sorted((ROOT / "config").glob("monitor*.example.yaml")):
        copy_optional_file(example_path, output_dir / example_path.name)
    if config.app.domestic_trading_calendar_path:
        copy_optional_file(Path(config.app.domestic_trading_calendar_path), output_dir / Path(config.app.domestic_trading_calendar_path).name)
    copy_optional_file(ROOT / "systemd" / "cross-market-monitor.service", output_dir / "cross-market-monitor.repo.service")
    copy_optional_file(Path("/etc/systemd/system/cross-market-monitor.service"), output_dir / "cross-market-monitor.installed.service")
    copy_optional_file(Path("/etc/systemd/system/cross-market-monitor-worker.service"), output_dir / "cross-market-monitor-worker.installed.service")
    copy_optional_file(Path("/etc/systemd/system/cross-market-monitor-api.service"), output_dir / "cross-market-monitor-api.installed.service")
    copy_optional_file(Path("/etc/nginx/sites-available/cross-market-monitor"), output_dir / "cross-market-monitor.installed.nginx.conf")

    results = {
        "git_status": run_command(["git", "status", "--short"], output_dir, "repo.git_status"),
        "git_head": run_command(["git", "rev-parse", "HEAD"], output_dir, "repo.git_head"),
        "git_branch": run_command(["git", "branch", "--show-current"], output_dir, "repo.git_branch"),
        "ps_python": run_command(["ps", "-ef"], output_dir, "runtime.ps_ef"),
        "disk_usage": run_command(["df", "-h"], output_dir, "runtime.df_h"),
    }
    write_json(output_dir / "repo_commands.json", {key: asdict(value) for key, value in results.items()})

    data_dir = ROOT / "data"
    if data_dir.exists():
        listing = []
        for path in sorted(data_dir.iterdir()):
            stat = path.stat()
            listing.append(
                {
                    "name": path.name,
                    "size": stat.st_size,
                    "mtime": datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(),
                }
            )
        write_json(output_dir / "data_listing.json", listing)


def collect_nginx_artifacts(output_dir: Path, tail_lines: int = 4000) -> dict[str, Any]:
    results = {
        "nginx.error_log": run_command(["tail", "-n", str(tail_lines), "/var/log/nginx/error.log"], output_dir, "nginx.error_log"),
        "nginx.access_log": run_command(["tail", "-n", str(tail_lines), "/var/log/nginx/access.log"], output_dir, "nginx.access_log"),
    }
    return {key: asdict(value) for key, value in results.items()}


def backup_database(db_path: Path, output_dir: Path) -> Path | None:
    if not db_path.exists():
        return None
    snapshot_path = output_dir / f"{db_path.name}.snapshot"
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as src, sqlite3.connect(snapshot_path) as dst:
        src.backup(dst)
    return snapshot_path


def analyze_database(db_path: Path, output_dir: Path) -> dict[str, Any]:
    if not db_path.exists():
        summary = {"exists": False, "db_path": str(db_path)}
        write_json(output_dir / "db_summary.json", summary)
        return summary

    summary: dict[str, Any] = {
        "exists": True,
        "db_path": str(db_path),
        "generated_at": now_local().isoformat(),
        "tables": [],
    }

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        tables = [
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
        ]
        for table in tables:
            columns = [str(row["name"]) for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]
            row_count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            item: dict[str, Any] = {
                "table": table,
                "row_count": row_count,
                "columns": columns,
            }
            if "ts" in columns:
                min_ts, max_ts = conn.execute(f'SELECT MIN(ts), MAX(ts) FROM "{table}"').fetchone()
                item["min_ts"] = min_ts
                item["max_ts"] = max_ts
            if "updated_at" in columns:
                item["updated_at_max"] = conn.execute(f'SELECT MAX(updated_at) FROM "{table}"').fetchone()[0]
            summary["tables"].append(item)

        if "source_health_state" in tables:
            summary["source_health_state"] = [
                dict(row)
                for row in conn.execute(
                    "SELECT source_name, kind, success_count, failure_count, last_success_at, last_failure_at, last_error, last_symbol, last_latency_ms FROM source_health_state ORDER BY source_name"
                ).fetchall()
            ]
        if "runtime_state" in tables:
            summary["runtime_state"] = [
                {
                    "state_name": row["state_name"],
                    "updated_at": row["updated_at"],
                    "payload_preview": str(row["payload"])[:500],
                }
                for row in conn.execute("SELECT state_name, payload, updated_at FROM runtime_state ORDER BY state_name").fetchall()
            ]
        if "job_runs" in tables:
            summary["job_runs"] = [dict(row) for row in conn.execute("SELECT * FROM job_runs ORDER BY job_name").fetchall()]

    write_json(output_dir / "db_summary.json", summary)
    lines = [
        f"db_path = {db_path}",
        "",
        "table\trow_count\tmin_ts\tmax_ts\tupdated_at_max",
    ]
    for item in summary["tables"]:
        lines.append(
            "\t".join(
                [
                    item["table"],
                    str(item["row_count"]),
                    str(item.get("min_ts") or ""),
                    str(item.get("max_ts") or ""),
                    str(item.get("updated_at_max") or ""),
                ]
            )
        )
    write_text(output_dir / "db_summary.txt", "\n".join(lines) + "\n")
    return summary


def fetch_api_artifacts(api_base_url: str, output_dir: Path) -> dict[str, Any]:
    endpoints = {
        "health": "/api/health",
        "snapshot_summary": "/api/snapshot-summary",
        "job_runs": "/api/job-runs",
    }
    summary: dict[str, Any] = {"api_base_url": api_base_url, "results": {}}
    for stem, path in endpoints.items():
        url = f"{api_base_url.rstrip('/')}{path}"
        try:
            with urlopen(url, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
            write_json(output_dir / f"api.{stem}.json", payload)
            summary["results"][stem] = {"ok": True, "url": url}
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            write_text(output_dir / f"api.{stem}.error.txt", f"{exc}\n")
            summary["results"][stem] = {"ok": False, "url": url, "error": str(exc)}
    write_json(output_dir / "api_summary.json", summary)
    return summary


def build_report(
    output_dir: Path,
    *,
    services: list[str],
    hours: int,
    archive_name: str,
    service_summaries: dict[str, dict[str, Any]],
    journal_summaries: dict[str, dict[str, Any]],
    db_summary: dict[str, Any],
    api_summary: dict[str, Any],
) -> None:
    lines = [
        "# Cross Market Runtime Diagnostic Report",
        "",
        f"- generated_at: `{now_local().isoformat()}`",
        f"- window_hours: `{hours}`",
        f"- archive: `{archive_name}`",
        "",
        "## Services",
        "",
    ]
    for service in services:
        meta = service_summaries[service]
        journal = journal_summaries[service]
        lines.append(
            f"- `{service}`: status_rc=`{meta['status']['returncode']}`, is_active_rc=`{meta['is_active']['returncode']}`, journal_rc=`{meta['journal']['returncode']}`"
        )
        if journal.get("exists"):
            lines.append(
                f"  journal: lines=`{journal['line_count']}`, error_like=`{journal['error_like_count']}`"
            )
            for message, count in journal.get("top_messages", [])[:5]:
                lines.append(f"  top: `{count}` x {message}")
    lines.extend(["", "## API", ""])
    for stem, result in api_summary.get("results", {}).items():
        lines.append(f"- `{stem}`: ok=`{result.get('ok')}` url=`{result.get('url')}`")
        if result.get("error"):
            lines.append(f"  error: {result['error']}")

    lines.extend(["", "## Database", ""])
    if not db_summary.get("exists"):
        lines.append(f"- missing: `{db_summary.get('db_path')}`")
    else:
        for item in db_summary.get("tables", []):
            lines.append(
                f"- `{item['table']}`: rows=`{item['row_count']}` max_ts=`{item.get('max_ts') or '--'}`"
            )

    write_text(output_dir / "REPORT.md", "\n".join(lines) + "\n")


def make_archive(output_root: Path, bundle_dir: Path) -> Path:
    archive_path = output_root / f"{bundle_dir.name}.tar.gz"
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(bundle_dir, arcname=bundle_dir.name)
    return archive_path


def parse_services(extra_services: str) -> list[str]:
    items = list(DEFAULT_SERVICES)
    for raw in extra_services.split(","):
        value = raw.strip()
        if value and value not in items:
            items.append(value)
    return items


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export runtime diagnostics for cross-market monitor",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to monitor.yaml")
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="Journal lookback window in hours")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Directory to store bundles")
    parser.add_argument("--api-base-url", default=DEFAULT_API_BASE_URL, help="Base API URL, e.g. http://127.0.0.1:6080")
    parser.add_argument("--extra-services", default=",".join(DEFAULT_EXTRA_SERVICES), help="Comma separated extra services to include")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    stamp = now_local().strftime("%Y%m%d_%H%M%S")
    bundle_dir = output_root / f"runtime_diagnostics_{stamp}"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    services = parse_services(args.extra_services)
    collect_system_info(bundle_dir, config_path, config)
    collect_repo_artifacts(bundle_dir, config_path, config)

    service_summaries: dict[str, dict[str, Any]] = {}
    journal_summaries: dict[str, dict[str, Any]] = {}
    for service in services:
        service_summaries[service] = collect_service_artifacts(service, args.hours, bundle_dir)
        journal_file = bundle_dir / service_summaries[service]["journal"]["stdout_path"]
        journal_summaries[service] = summarize_journal(journal_file, bundle_dir, f"{sanitize_name(service)}.journal")

    nginx_summary = collect_nginx_artifacts(bundle_dir)
    write_json(bundle_dir / "nginx_summary.json", nginx_summary)

    db_snapshot = backup_database(Path(config.app.sqlite_path), bundle_dir)
    db_summary = analyze_database(db_snapshot or Path(config.app.sqlite_path), bundle_dir)
    api_summary = fetch_api_artifacts(args.api_base_url, bundle_dir)

    build_report(
        bundle_dir,
        services=services,
        hours=args.hours,
        archive_name=f"{bundle_dir.name}.tar.gz",
        service_summaries=service_summaries,
        journal_summaries=journal_summaries,
        db_summary=db_summary,
        api_summary=api_summary,
    )
    archive_path = make_archive(output_root, bundle_dir)

    print(f"bundle_dir={bundle_dir}")
    print(f"archive={archive_path}")


if __name__ == "__main__":
    main()
