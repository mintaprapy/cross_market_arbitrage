from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path

from cross_market_monitor.infrastructure.config_loader import load_config
from cross_market_monitor.infrastructure.repository import SQLiteRepository

DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "config" / "monitor.yaml"


def load_runtime(config_path: str):
    config = load_config(config_path)
    repository = SQLiteRepository(config.app.sqlite_path)
    return config, repository


def build_service(config_path: str):
    from cross_market_monitor.application.service import MonitorService

    config, repository = load_runtime(config_path)
    return MonitorService(config, repository)


def print_console_table(snapshot_payload: dict) -> None:
    print()
    print(f"As of: {snapshot_payload['as_of']}")
    print("-" * 128)
    print(
        f"{'GROUP':<18} {'STATUS':<8} {'SIG':<8} {'NORM':>12} {'OVS':>12} {'SPREAD':>12} {'SPREAD%':>10} {'ZSCORE':>10} {'FX JUMP':>10}"
    )
    print("-" * 128)
    for item in snapshot_payload["snapshots"]:
        print(
            f"{item['group_name']:<18} {item['status']:<8} {item['signal_state']:<8} "
            f"{_fmt(item['normalized_last']):>12} {_fmt(item['overseas_last']):>12} "
            f"{_fmt(item['spread']):>12} {_fmt_pct(item['spread_pct']):>10} "
            f"{_fmt(item['zscore']):>10} {_fmt_pct(item['fx_jump_pct']):>10}"
        )
    print("-" * 128)


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
    await service.startup()
    try:
        for _ in range(cycles):
            await service.poll_once()
            print_console_table(service.get_snapshot())
            if cycles > 1:
                await asyncio.sleep(service.config.app.poll_interval_sec)
    finally:
        await service.shutdown()


async def run_worker(config_path: str) -> None:
    service = build_service(config_path)
    await service.startup()
    try:
        await service.run_forever()
    finally:
        await service.shutdown()


def export_dataset(
    config_path: str,
    dataset: str,
    fmt: str,
    output: str | None,
    group_name: str | None,
    limit: int,
    start_ts: str | None,
    end_ts: str | None,
) -> None:
    config, repository = load_runtime(config_path)
    suffix = "parquet" if fmt == "parquet" else "csv"
    output_path = output or str(Path(config.app.export_dir).resolve() / f"{dataset}_{group_name or 'all'}.{suffix}")
    if fmt == "parquet":
        count = repository.export_dataset_to_parquet(
            dataset,
            output_path,
            group_name=group_name,
            limit=limit,
            start_ts=start_ts,
            end_ts=end_ts,
        )
    else:
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
        f"Status counts: ok={report['ok_count']} partial={report['partial_count']} stale_or_paused={report['stale_count']} error={report['error_count']}"
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
    print(
        f"Hedge beta/intercept={_fmt(report['hedge_ratio_ols'])}/{_fmt(report['hedge_intercept'])} realized daily vol={_fmt_pct(report['realized_daily_vol_pct'])}"
    )
    print(
        f"Suggested scale={_fmt(report['recommended_position_scale'])} avg round-trip cost={_fmt(report['average_round_trip_cost'])} avg net edge={_fmt(report['average_net_edge_after_cost'])} profitable_after_cost={report['profitable_after_cost_count']}"
    )
    print("Top highlights:")
    for item in report["top_highlights"]:
        print(
            f"  {item['ts']} {item['metric']} score={_fmt(item['score'])} spread={_fmt(item['spread'])} spread_pct={_fmt_pct(item['spread_pct'])} zscore={_fmt(item['zscore'])}"
        )
    print("Signal entries:")
    for item in report["signal_entries"]:
        trigger = item["trigger"]
        value = item["value"] * 100 if trigger == "spread_pct" else item["value"]
        threshold = item["threshold"] * 100 if trigger == "spread_pct" else item["threshold"]
        suffix = "%" if trigger == "spread_pct" else ""
        print(
            f"  {item['ts']} {trigger} value={value:.2f}{suffix} threshold={threshold:.2f}{suffix} direction={item['direction']}"
        )


def print_domestic_backfill_report(report: dict) -> None:
    print()
    print(f"Domestic Backfill: {report['group_name']}")
    print("-" * 72)
    print(
        f"Source={report['domestic_source']} Symbol={report['domestic_symbol']} Interval={report['interval']} Range={report['range_key']}"
    )
    if not report.get("supported", False):
        print(f"Supported: no")
        print(f"Reason: {report.get('reason', '--')}")
        return
    print(
        f"Requested window: {report.get('requested_start_ts') or '--'} -> {report.get('requested_end_ts') or 'latest'}"
    )
    print(
        f"Available window: {report.get('available_start_ts') or '--'} -> {report.get('available_end_ts') or '--'}"
    )
    print(
        f"Fetched={report.get('fetched_rows', 0)} Inserted={report.get('inserted_rows', 0)} Skipped={report.get('skipped_rows', 0)}"
    )
    print("Per group:")
    for item in report.get("per_group", []):
        print(
            f"  {item['group_name']}: inserted={item['inserted_rows']} skipped={item['skipped_rows']}"
        )


def print_overseas_backfill_report(report: dict) -> None:
    print()
    print(f"Overseas Backfill: {report['group_name']}")
    print("-" * 72)
    print(
        f"Source={report['overseas_source']} Symbol={report['overseas_symbol']} Interval={report['interval']} Range={report['range_key']}"
    )
    if not report.get("supported", False):
        print("Supported: no")
        print(f"Reason: {report.get('reason', '--')}")
        return
    print(
        f"Requested window: {report.get('requested_start_ts') or '--'} -> {report.get('requested_end_ts') or 'latest'}"
    )
    print(
        f"Available window: {report.get('available_start_ts') or '--'} -> {report.get('available_end_ts') or '--'}"
    )
    print(
        f"Fetched={report.get('fetched_rows', 0)} Inserted={report.get('inserted_rows', 0)} Skipped={report.get('skipped_rows', 0)}"
    )
    print("Per group:")
    for item in report.get("per_group", []):
        print(
            f"  {item['group_name']}: inserted={item['inserted_rows']} skipped={item['skipped_rows']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Cross-market commodity spread monitor")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to YAML config")
    subparsers = parser.add_subparsers(dest="command", required=True)

    console_parser = subparsers.add_parser("console", help="Run polling cycles and print table output")
    console_parser.add_argument("--cycles", type=int, default=1, help="Number of cycles to run")

    subparsers.add_parser("run-once", help="Run a single polling cycle")

    subparsers.add_parser("run-worker", help="Run the monitor worker loop without starting the API")

    serve_parser = subparsers.add_parser("serve", help="Run FastAPI dashboard")
    serve_parser.add_argument("--host", default=None, help="Bind host")
    serve_parser.add_argument("--port", type=int, default=None, help="Bind port")

    api_parser = subparsers.add_parser("run-api", help="Run FastAPI dashboard and API")
    api_parser.add_argument("--host", default=None, help="Bind host")
    api_parser.add_argument("--port", type=int, default=None, help="Bind port")

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

    export_parquet_parser = subparsers.add_parser("export-parquet", help="Export stored data into Parquet")
    export_parquet_parser.add_argument(
        "--dataset",
        required=True,
        choices=["snapshots", "alerts", "raw_quotes", "fx_rates", "notification_deliveries"],
    )
    export_parquet_parser.add_argument("--output", default=None, help="Output Parquet path")
    export_parquet_parser.add_argument("--group-name", default=None, help="Optional group filter")
    export_parquet_parser.add_argument("--limit", type=int, default=5000, help="Maximum rows to export")
    export_parquet_parser.add_argument("--start-ts", default=None, help="Inclusive ISO timestamp lower bound")
    export_parquet_parser.add_argument("--end-ts", default=None, help="Inclusive ISO timestamp upper bound")

    replay_parser = subparsers.add_parser("replay", help="Analyze historical spread snapshots")
    replay_parser.add_argument("--group-name", required=True, help="Group to analyze")
    replay_parser.add_argument("--limit", type=int, default=1000, help="Maximum rows to analyze")
    replay_parser.add_argument("--start-ts", default=None, help="Inclusive ISO timestamp lower bound")
    replay_parser.add_argument("--end-ts", default=None, help="Inclusive ISO timestamp upper bound")
    replay_parser.add_argument("--format", choices=["text", "json"], default="text")

    domestic_backfill_parser = subparsers.add_parser(
        "backfill-domestic",
        help="Backfill domestic history from the currently selected domestic source when the adapter supports it",
    )
    domestic_backfill_parser.add_argument("--group-name", required=True, help="Group to backfill")
    domestic_backfill_parser.add_argument("--interval", choices=["5m", "15m", "30m", "60m", "1d"], default="5m")
    domestic_backfill_parser.add_argument(
        "--range-key",
        choices=["24h", "7d", "30d", "90d", "1y", "all"],
        default="30d",
        help="Named lookback window when start/end are not provided",
    )
    domestic_backfill_parser.add_argument("--start-ts", default=None, help="Inclusive ISO timestamp lower bound")
    domestic_backfill_parser.add_argument("--end-ts", default=None, help="Inclusive ISO timestamp upper bound")
    domestic_backfill_parser.add_argument("--format", choices=["text", "json"], default="text")

    overseas_backfill_parser = subparsers.add_parser(
        "backfill-overseas",
        help="Backfill overseas history from the currently selected overseas source when the adapter supports it",
    )
    overseas_backfill_parser.add_argument("--group-name", required=True, help="Group to backfill")
    overseas_backfill_parser.add_argument(
        "--interval",
        choices=["1m", "5m", "15m", "30m", "60m", "4h", "1d"],
        default="60m",
    )
    overseas_backfill_parser.add_argument(
        "--range-key",
        choices=["24h", "7d", "30d", "90d", "1y", "all"],
        default="30d",
        help="Named lookback window when start/end are not provided",
    )
    overseas_backfill_parser.add_argument("--start-ts", default=None, help="Inclusive ISO timestamp lower bound")
    overseas_backfill_parser.add_argument("--end-ts", default=None, help="Inclusive ISO timestamp upper bound")
    overseas_backfill_parser.add_argument("--format", choices=["text", "json"], default="text")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    if args.command == "console":
        _run_async(run_console(args.config, args.cycles))
        return

    if args.command == "run-once":
        _run_async(run_console(args.config, 1))
        return

    if args.command == "run-worker":
        _run_async(run_worker(args.config))
        return

    if args.command == "export-csv":
        export_dataset(
            args.config,
            args.dataset,
            "csv",
            args.output,
            args.group_name,
            args.limit,
            args.start_ts,
            args.end_ts,
        )
        return

    if args.command == "export-parquet":
        export_dataset(
            args.config,
            args.dataset,
            "parquet",
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

    if args.command == "backfill-domestic":
        service = build_service(args.config)
        report = service.backfill_domestic_history(
            args.group_name,
            interval=args.interval,
            range_key=args.range_key,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
        )
        if args.format == "json":
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            print_domestic_backfill_report(report)
        return

    if args.command == "backfill-overseas":
        service = build_service(args.config)
        report = service.backfill_overseas_history(
            args.group_name,
            interval=args.interval,
            range_key=args.range_key,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
        )
        if args.format == "json":
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            print_overseas_backfill_report(report)
        return

    from cross_market_monitor.interfaces.api.app import create_app
    import uvicorn

    service = build_service(args.config)
    app = create_app(service, run_runtime=args.command != "run-api")
    host = args.host or service.config.app.bind_host
    port = args.port or service.config.app.bind_port
    uvicorn.run(app, host=host, port=port, log_level="info")


def _run_async(coro) -> None:
    try:
        asyncio.run(coro)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
