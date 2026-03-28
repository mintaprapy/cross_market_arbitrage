from __future__ import annotations

import argparse
import json
import shutil
import statistics
import tarfile
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = ROOT / "data" / "tqsdk_connectivity"
DEFAULT_OUTPUT_ROOT = ROOT / "exports"
DEFAULT_DAYS = 7
DEFAULT_MIN_CONNECT_SUCCESS_RATE = 0.99
DEFAULT_MIN_IN_SESSION_FRESH_RATE = 0.99
DEFAULT_EXCLUDE_WINDOWS = ("19:00-19:30",)
DEFAULT_EXCLUDE_TIMEZONE = "Asia/Hong_Kong"
PRODUCT_CODES = ("au", "ag", "cu", "bc", "sc")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _latency_summary(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"median_ms": None, "p95_ms": None, "max_ms": None}
    ordered = sorted(values)
    p95_index = max(int(len(ordered) * 0.95) - 1, 0)
    return {
        "median_ms": round(statistics.median(ordered), 2),
        "p95_ms": round(ordered[p95_index], 2),
        "max_ms": round(max(ordered), 2),
    }


def _parse_window_spec(value: str) -> tuple[time, time]:
    text = value.strip()
    if "-" not in text:
        raise ValueError(f"Invalid window spec: {value}")
    start_text, end_text = [item.strip() for item in text.split("-", 1)]
    start = time.fromisoformat(start_text)
    end = time.fromisoformat(end_text)
    return start, end


def _report_overlaps_window(report: dict[str, Any], *, window_spec: str, timezone_name: str) -> bool:
    started_at = _parse_datetime(report.get("started_at")) or report.get("_report_ts")
    ended_at = _parse_datetime(report.get("ended_at")) or report.get("_report_ts")
    if not isinstance(started_at, datetime) or not isinstance(ended_at, datetime):
        return False
    if ended_at < started_at:
        started_at, ended_at = ended_at, started_at

    local_zone = ZoneInfo(timezone_name)
    start_local = started_at.astimezone(local_zone)
    end_local = ended_at.astimezone(local_zone)
    window_start, window_end = _parse_window_spec(window_spec)

    day = start_local.date()
    while day <= end_local.date():
        local_window_start = datetime.combine(day, window_start, tzinfo=local_zone)
        local_window_end = datetime.combine(day, window_end, tzinfo=local_zone)
        if local_window_end <= local_window_start:
            local_window_end += timedelta(days=1)
        if start_local < local_window_end and end_local > local_window_start:
            return True
        day += timedelta(days=1)
    return False


def load_recent_reports(input_dir: Path, *, days: int, now: datetime | None = None) -> list[dict[str, Any]]:
    current = now or _utc_now()
    cutoff = current - timedelta(days=days)
    reports: list[dict[str, Any]] = []

    for path in sorted(input_dir.glob("tqsdk_connectivity_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

        report_ts = _parse_datetime(payload.get("ended_at")) or _parse_datetime(payload.get("started_at"))
        if report_ts is None:
            report_ts = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        if report_ts < cutoff:
            continue

        payload["_path"] = str(path)
        payload["_report_ts"] = report_ts
        reports.append(payload)
    return reports


def split_excluded_reports(
    reports: list[dict[str, Any]],
    *,
    exclude_windows: list[str],
    exclude_timezone: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not exclude_windows:
        return reports, []

    included: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for report in reports:
        matched_window = next(
            (
                window_spec
                for window_spec in exclude_windows
                if _report_overlaps_window(report, window_spec=window_spec, timezone_name=exclude_timezone)
            ),
            None,
        )
        if matched_window is None:
            included.append(report)
            continue
        cloned = dict(report)
        cloned["_excluded_window"] = matched_window
        excluded.append(cloned)
    return included, excluded


def aggregate_reports(
    reports: list[dict[str, Any]],
    *,
    days: int,
    min_connect_success_rate: float,
    min_in_session_fresh_rate: float,
    max_refresh_latency_median_ms: float | None,
    exclude_windows: list[str],
    exclude_timezone: str,
    excluded_reports: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    timestamps = [item["_report_ts"] for item in reports if isinstance(item.get("_report_ts"), datetime)]
    connect_successes = sum(1 for item in reports if item.get("connect_success"))
    connect_success_rate = (connect_successes / len(reports)) if reports else None
    refresh_medians = [
        float(item["refresh_latency"]["median_ms"])
        for item in reports
        if item.get("refresh_latency", {}).get("median_ms") is not None
    ]
    refresh_update_ratios = [
        float(item["refresh_update_ratio"])
        for item in reports
        if item.get("refresh_update_ratio") is not None
    ]

    symbol_summary: dict[str, dict[str, Any]] = {}
    breaches: list[str] = []

    if reports and connect_success_rate is not None and connect_success_rate < min_connect_success_rate:
        breaches.append(
            f"connect_success_rate={connect_success_rate:.2%} < threshold {min_connect_success_rate:.2%}"
        )

    refresh_latency = _latency_summary(refresh_medians)
    if (
        reports
        and max_refresh_latency_median_ms is not None
        and refresh_latency["median_ms"] is not None
        and float(refresh_latency["median_ms"]) > max_refresh_latency_median_ms
    ):
        breaches.append(
            "refresh_latency_median_ms="
            f"{float(refresh_latency['median_ms']):.2f} > threshold {max_refresh_latency_median_ms:.2f}"
        )

    for code in PRODUCT_CODES:
        symbol_rows = [item.get("symbols", {}).get(code) for item in reports]
        symbol_rows = [item for item in symbol_rows if isinstance(item, dict)]
        if not symbol_rows:
            continue

        success_rates = [float(item["success_rate"]) for item in symbol_rows if item.get("success_rate") is not None]
        fresh_rates = [
            float(item["in_session_fresh_rate"])
            for item in symbol_rows
            if item.get("in_session_fresh_rate") is not None
        ]
        max_age_in_session = [
            float(item["max_age_in_session_sec"])
            for item in symbol_rows
            if item.get("max_age_in_session_sec") is not None
        ]
        stale_total = sum(int(item.get("stale_in_session_count", 0)) for item in symbol_rows)
        in_session_cycles_total = sum(int(item.get("in_session_cycles", 0)) for item in symbol_rows)
        out_of_session_cycles_total = sum(int(item.get("out_of_session_cycles", 0)) for item in symbol_rows)
        resolved_symbols = sorted(
            {
                str(symbol)
                for item in symbol_rows
                for symbol in item.get("resolved_symbols", []) or []
                if str(symbol).strip()
            }
        )

        avg_fresh_rate = statistics.mean(fresh_rates) if fresh_rates else None
        min_fresh_rate = min(fresh_rates) if fresh_rates else None
        if avg_fresh_rate is not None and avg_fresh_rate < min_in_session_fresh_rate:
            breaches.append(
                f"{code}.avg_in_session_fresh_rate={avg_fresh_rate:.2%} < threshold {min_in_session_fresh_rate:.2%}"
            )

        symbol_summary[code] = {
            "report_count": len(symbol_rows),
            "avg_success_rate": round(statistics.mean(success_rates), 4) if success_rates else None,
            "avg_in_session_fresh_rate": round(avg_fresh_rate, 4) if avg_fresh_rate is not None else None,
            "min_in_session_fresh_rate": round(min_fresh_rate, 4) if min_fresh_rate is not None else None,
            "total_stale_in_session": stale_total,
            "in_session_cycles_total": in_session_cycles_total,
            "out_of_session_cycles_total": out_of_session_cycles_total,
            "max_age_in_session_sec": round(max(max_age_in_session), 3) if max_age_in_session else None,
            "resolved_symbols": resolved_symbols,
        }

    connection_failures = [
        {
            "path": item.get("_path"),
            "report_ts": item.get("_report_ts"),
            "setup_error": item.get("setup_error"),
            "connect_attempts": item.get("connect_attempts", [])[-3:],
        }
        for item in reports
        if not item.get("connect_success")
    ]

    summary = {
        "generated_at": _utc_now(),
        "window_days": days,
        "report_count": len(reports),
        "period_start": min(timestamps) if timestamps else None,
        "period_end": max(timestamps) if timestamps else None,
        "source_reports": [str(item.get("_path")) for item in reports],
        "thresholds": {
            "min_connect_success_rate": min_connect_success_rate,
            "min_in_session_fresh_rate": min_in_session_fresh_rate,
            "max_refresh_latency_median_ms": max_refresh_latency_median_ms,
        },
        "excluded_windows": list(exclude_windows),
        "excluded_timezone": exclude_timezone,
        "excluded_report_count": len(excluded_reports or []),
        "excluded_reports": [
            {
                "path": item.get("_path"),
                "report_ts": item.get("_report_ts"),
                "excluded_window": item.get("_excluded_window"),
            }
            for item in (excluded_reports or [])
        ][:20],
        "overall": {
            "connect_success_count": connect_successes,
            "connect_failure_count": len(reports) - connect_successes,
            "connect_success_rate": round(connect_success_rate, 4) if connect_success_rate is not None else None,
            "refresh_latency": refresh_latency,
            "avg_refresh_update_ratio": round(statistics.mean(refresh_update_ratios), 4)
            if refresh_update_ratios
            else None,
        },
        "symbols": symbol_summary,
        "connection_failures": connection_failures[:10],
        "breaches": breaches,
        "is_stable": bool(reports) and not breaches,
    }
    return summary


def render_report(summary: dict[str, Any]) -> str:
    overall = summary["overall"]
    lines = [
        "# TqSdk Weekly Stability Report",
        "",
        f"- generated_at: `{summary['generated_at']}`",
        f"- window_days: `{summary['window_days']}`",
        f"- report_count: `{summary['report_count']}`",
        f"- excluded_report_count: `{summary.get('excluded_report_count', 0)}`",
        f"- period_start: `{summary.get('period_start') or '--'}`",
        f"- period_end: `{summary.get('period_end') or '--'}`",
        f"- is_stable: `{summary['is_stable']}`",
        "",
        "## Overall",
        "",
        f"- connect_success: `{overall['connect_success_count']}/{summary['report_count']}`",
        f"- connect_success_rate: `{overall.get('connect_success_rate')}`",
        f"- avg_refresh_update_ratio: `{overall.get('avg_refresh_update_ratio')}`",
        f"- refresh_latency_median_ms: `{overall['refresh_latency'].get('median_ms')}`",
        f"- refresh_latency_p95_ms: `{overall['refresh_latency'].get('p95_ms')}`",
        f"- refresh_latency_max_ms: `{overall['refresh_latency'].get('max_ms')}`",
        "",
        "## Thresholds",
        "",
        f"- min_connect_success_rate: `{summary['thresholds']['min_connect_success_rate']}`",
        f"- min_in_session_fresh_rate: `{summary['thresholds']['min_in_session_fresh_rate']}`",
        f"- max_refresh_latency_median_ms: `{summary['thresholds']['max_refresh_latency_median_ms']}`",
        f"- excluded_windows: `{summary.get('excluded_windows') or []}`",
        f"- excluded_timezone: `{summary.get('excluded_timezone') or '--'}`",
        "",
        "## Per Product",
        "",
        "| Product | Reports | Avg In-Session Fresh | Min In-Session Fresh | Total Stale In-Session | Max Age In-Session (s) | Resolved Symbols |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]

    for code in PRODUCT_CODES:
        item = summary["symbols"].get(code)
        if not item:
            continue
        lines.append(
            "| "
            + " | ".join(
                [
                    code.upper(),
                    str(item["report_count"]),
                    str(item.get("avg_in_session_fresh_rate") or "--"),
                    str(item.get("min_in_session_fresh_rate") or "--"),
                    str(item.get("total_stale_in_session") or 0),
                    str(item.get("max_age_in_session_sec") or "--"),
                    ", ".join(item.get("resolved_symbols", [])) or "--",
                ]
            )
            + " |"
        )

    lines.extend(["", "## Breaches", ""])
    if summary["breaches"]:
        for breach in summary["breaches"]:
            lines.append(f"- {breach}")
    else:
        lines.append("- none")

    lines.extend(["", "## Excluded Reports", ""])
    if summary.get("excluded_reports"):
        for item in summary["excluded_reports"]:
            lines.append(
                f"- `{item.get('report_ts')}` `{item.get('path')}` excluded_window=`{item.get('excluded_window')}`"
            )
    else:
        lines.append("- none")

    lines.extend(["", "## Connection Failures", ""])
    if summary["connection_failures"]:
        for failure in summary["connection_failures"]:
            lines.append(
                f"- `{failure.get('report_ts')}` `{failure.get('path')}` setup_error=`{failure.get('setup_error')}`"
            )
    else:
        lines.append("- none")

    return "\n".join(lines) + "\n"


def export_bundle(
    summary: dict[str, Any],
    *,
    reports: list[dict[str, Any]],
    output_root: Path,
) -> tuple[Path, Path]:
    output_root.mkdir(parents=True, exist_ok=True)
    stamp = _utc_now().strftime("%Y%m%d_%H%M%S")
    bundle_dir = output_root / f"tqsdk_weekly_stability_{stamp}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    source_dir = bundle_dir / "source_reports"
    source_dir.mkdir(parents=True, exist_ok=True)

    (bundle_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=_json_default) + "\n",
        encoding="utf-8",
    )
    (bundle_dir / "REPORT.md").write_text(render_report(summary), encoding="utf-8")

    for item in reports:
        path_text = item.get("_path")
        if not path_text:
            continue
        src = Path(path_text)
        if src.exists():
            shutil.copy2(src, source_dir / src.name)

    archive_path = output_root / f"{bundle_dir.name}.tar.gz"
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(bundle_dir, arcname=bundle_dir.name)
    return bundle_dir, archive_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export a 7-day TqSdk stability summary bundle")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR), help="Directory containing tqsdk_connectivity_*.json")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Directory to store exported bundles")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Rolling window size in days")
    parser.add_argument(
        "--exclude-window",
        action="append",
        default=list(DEFAULT_EXCLUDE_WINDOWS),
        help="Local time window to exclude from stability scoring, e.g. 19:00-19:30",
    )
    parser.add_argument(
        "--exclude-timezone",
        default=DEFAULT_EXCLUDE_TIMEZONE,
        help="Timezone used to interpret excluded windows",
    )
    parser.add_argument(
        "--min-connect-success-rate",
        type=float,
        default=DEFAULT_MIN_CONNECT_SUCCESS_RATE,
        help="Threshold for overall connection success rate",
    )
    parser.add_argument(
        "--min-in-session-fresh-rate",
        type=float,
        default=DEFAULT_MIN_IN_SESSION_FRESH_RATE,
        help="Threshold for per-product in-session fresh rate",
    )
    parser.add_argument(
        "--max-refresh-latency-median-ms",
        type=float,
        default=None,
        help="Optional threshold for overall refresh latency median; omit to ignore this metric",
    )
    return parser


def run_report(args: argparse.Namespace) -> int:
    input_dir = Path(args.input_dir).resolve()
    output_root = Path(args.output_root).resolve()
    reports = load_recent_reports(input_dir, days=args.days)
    included_reports, excluded_reports = split_excluded_reports(
        reports,
        exclude_windows=list(args.exclude_window or []),
        exclude_timezone=str(args.exclude_timezone),
    )
    summary = aggregate_reports(
        included_reports,
        days=args.days,
        min_connect_success_rate=args.min_connect_success_rate,
        min_in_session_fresh_rate=args.min_in_session_fresh_rate,
        max_refresh_latency_median_ms=args.max_refresh_latency_median_ms,
        exclude_windows=list(args.exclude_window or []),
        exclude_timezone=str(args.exclude_timezone),
        excluded_reports=excluded_reports,
    )
    bundle_dir, archive_path = export_bundle(summary, reports=included_reports, output_root=output_root)

    print(f"bundle_dir={bundle_dir}")
    print(f"archive={archive_path}")
    print(f"summary_json={bundle_dir / 'summary.json'}")
    print(f"report_md={bundle_dir / 'REPORT.md'}")
    print(f"is_stable={summary['is_stable']}")
    print(f"report_count={summary['report_count']}")
    print(f"excluded_report_count={summary.get('excluded_report_count', 0)}")
    if summary["overall"]["connect_success_rate"] is not None:
        print(f"connect_success_rate={summary['overall']['connect_success_rate']:.2%}")
    return 0 if summary["is_stable"] else 1


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(run_report(args))


if __name__ == "__main__":
    main()
