from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path

import uvicorn

from cross_market_monitor.application.service import MonitorService
from cross_market_monitor.infrastructure.config_loader import load_config
from cross_market_monitor.infrastructure.repository import SQLiteRepository
from cross_market_monitor.interfaces.dashboard import create_app

DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "config" / "monitor.yaml"


def load_runtime(config_path: str):
    config = load_config(config_path)
    repository = SQLiteRepository(config.app.sqlite_path)
    return config, repository


def build_service(config_path: str) -> MonitorService:
    config, repository = load_runtime(config_path)
    return MonitorService(config, repository)


def print_console_table(snapshot_payload: dict) -> None:
    print()
    print(f"As of: {snapshot_payload['as_of']}")
    print("-" * 130)
    print(
        f"{'GROUP':<16} {'STATUS':<8} {'NORM':>12} {'OVS':>12} {'SPREAD':>12} {'SPREAD%':>10} {'ZSCORE':>10} {'EXEC D->O':>12} {'EXEC O->D':>12}"
    )
    print("-" * 130)
    for item in snapshot_payload["snapshots"]:
        print(
            f"{item['group_name']:<16} {item['status']:<8} "
            f"{_fmt(item['normalized_last']):>12} {_fmt(item['overseas_last']):>12} "
            f"{_fmt(item['spread']):>12} {_fmt_pct(item['spread_pct']):>10} "
            f"{_fmt(item['zscore']):>10} {_fmt(item['executable_buy_domestic_sell_overseas']):>12} "
            f"{_fmt(item['executable_buy_overseas_sell_domestic']):>12}"
        )
    print("-" * 130)


def _fmt(value: float | None) -> str:
    if value is None:
        return "--"
    return f"{value:.4f}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "--"
    return f"{value * 100:.2f}%"


async def run_console(config_path: str, cycles: int) -> None:
    service = build_service(config_path)
    for _ in range(cycles):
        await service.poll_once()
        print_console_table(service.get_snapshot())
        if cycles > 1:
            await asyncio.sleep(service.config.app.poll_interval_sec)


def export_csv(
    config_path: str,
    dataset: str,
    output: str | None,
    group_name: str | None,
    limit: int,
    start_ts: str | None,
    end_ts: str | None,
) -> None:
    config, repository = load_runtime(config_path)
    output_path = output or str(
        Path(config.app.export_dir).resolve() / f"{dataset}_{group_name or 'all'}.csv"
    )
    count = repository.export_dataset_to_csv(
        dataset,
        output_path,
        group_name=group_name,
        limit=limit,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    print(f"Exported {count} rows to {output_path}")


def print_replay_report(report: dict) -> None:
    print()
    print(f"Replay Summary: {report['group_name']}")
    print("-" * 72)
    print(f"Samples: {report['sample_count']}")
    print(f"Window: {report['start_ts']} -> {report['end_ts']}")
    print(
        f"Status counts: ok={report['ok_count']} partial={report['partial_count']} stale={report['stale_count']} error={report['error_count']}"
    )
    print(
        f"Latest spread={_fmt(report['latest_spread'])} spread_pct={_fmt_pct(report['latest_spread_pct'])} zscore={_fmt(report['latest_zscore'])}"
    )
    print(
        f"Spread mean/std={_fmt(report['spread_mean'])}/{_fmt(report['spread_std'])} min={_fmt(report['spread_min'])} max={_fmt(report['spread_max'])}"
    )
    print(
        f"Breaches: spread_pct={report['spread_pct_breach_count']} zscore={report['zscore_breach_count']}"
    )
    print(
        f"Convergence ratio={_fmt(report['convergence_ratio'])} divergence ratio={_fmt(report['divergence_ratio'])}"
    )
    print("Top highlights:")
    for item in report["top_highlights"]:
        print(
            f"  {item['ts']} {item['metric']} score={_fmt(item['score'])} spread={_fmt(item['spread'])} spread_pct={_fmt_pct(item['spread_pct'])} zscore={_fmt(item['zscore'])}"
        )
    print("Signal entries:")
    for item in report["signal_entries"]:
        value = item["value"] * 100 if item["trigger"] == "spread_pct" else item["value"]
        threshold = item["threshold"] * 100 if item["trigger"] == "spread_pct" else item["threshold"]
        suffix = "%" if item["trigger"] == "spread_pct" else ""
        print(
            f"  {item['ts']} {item['trigger']} value={value:.2f}{suffix} threshold={threshold:.2f}{suffix} direction={item['direction']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Cross-market commodity spread monitor")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to YAML config")
    subparsers = parser.add_subparsers(dest="command", required=True)

    console_parser = subparsers.add_parser("console", help="Run polling cycles and print table output")
    console_parser.add_argument("--cycles", type=int, default=1, help="Number of cycles to run")

    subparsers.add_parser("run-once", help="Run a single polling cycle")

    serve_parser = subparsers.add_parser("serve", help="Run FastAPI dashboard")
    serve_parser.add_argument("--host", default=None, help="Bind host")
    serve_parser.add_argument("--port", type=int, default=None, help="Bind port")

    export_parser = subparsers.add_parser("export-csv", help="Export stored data into CSV")
    export_parser.add_argument(
        "--dataset",
        required=True,
        choices=["snapshots", "alerts", "raw_quotes", "fx_rates", "notification_deliveries"],
    )
    export_parser.add_argument("--output", default=None, help="Output CSV path")
    export_parser.add_argument("--group-name", default=None, help="Optional group filter")
    export_parser.add_argument("--limit", type=int, default=5000, help="Maximum rows to export")
    export_parser.add_argument("--start-ts", default=None, help="Inclusive ISO timestamp lower bound")
    export_parser.add_argument("--end-ts", default=None, help="Inclusive ISO timestamp upper bound")

    replay_parser = subparsers.add_parser("replay", help="Analyze historical spread snapshots")
    replay_parser.add_argument("--group-name", required=True, help="Group to analyze")
    replay_parser.add_argument("--limit", type=int, default=1000, help="Maximum rows to analyze")
    replay_parser.add_argument("--start-ts", default=None, help="Inclusive ISO timestamp lower bound")
    replay_parser.add_argument("--end-ts", default=None, help="Inclusive ISO timestamp upper bound")
    replay_parser.add_argument("--format", choices=["text", "json"], default="text")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    if args.command == "console":
        asyncio.run(run_console(args.config, args.cycles))
        return

    if args.command == "run-once":
        asyncio.run(run_console(args.config, 1))
        return

    if args.command == "export-csv":
        export_csv(
            args.config,
            args.dataset,
            args.output,
            args.group_name,
            args.limit,
            args.start_ts,
            args.end_ts,
        )
        return

    if args.command == "replay":
        service = build_service(args.config)
        report = service.replay_summary(
            args.group_name,
            limit=args.limit,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
        )
        if args.format == "json":
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            print_replay_report(report)
        return

    service = build_service(args.config)
    app = create_app(service)
    host = args.host or service.config.app.bind_host
    port = args.port or service.config.app.bind_port
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
