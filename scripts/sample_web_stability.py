#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


DEFAULT_ENDPOINTS = (
    "/",
    "/dashboard/app.js",
    "/dashboard/styles.css",
    "/api/health",
    "/api/snapshot-summary",
)


@dataclass(slots=True)
class EndpointResult:
    path: str
    ok: bool
    status_code: int | None
    latency_ms: float
    bytes: int
    error: str | None = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sample dashboard and API stability on a fixed interval.")
    parser.add_argument("--base-url", default="http://127.0.0.1:6080", help="Base URL for the local service.")
    parser.add_argument("--interval-sec", type=int, default=60, help="Sampling interval in seconds.")
    parser.add_argument("--cycles", type=int, default=0, help="Optional fixed number of sampling cycles.")
    parser.add_argument(
        "--duration-minutes",
        type=int,
        default=0,
        help="Optional fixed runtime in minutes. 0 means run until interrupted.",
    )
    parser.add_argument("--group-name", default="AU_XAU", help="Group used for the card endpoint probe.")
    parser.add_argument("--range-key", default="24h", help="Range key used for the card endpoint probe.")
    parser.add_argument("--timeout-sec", type=int, default=10, help="Per-request timeout.")
    parser.add_argument("--log-file", default=None, help="Optional JSONL output path.")
    return parser


def resolve_log_file(root_dir: Path, explicit_path: str | None) -> Path:
    if explicit_path:
        return Path(explicit_path).expanduser().resolve()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return root_dir / "logs" / f"web_stability_{timestamp}.jsonl"


def fetch(url: str, timeout_sec: int) -> tuple[int, bytes]:
    request = Request(
        url,
        headers={
            "User-Agent": "cross-market-monitor-web-sampler/1.0",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )
    with urlopen(request, timeout=timeout_sec) as response:
        body = response.read()
        return getattr(response, "status", 200), body


def probe_endpoint(base_url: str, path: str, timeout_sec: int) -> EndpointResult:
    url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    started = time.perf_counter()
    try:
        status_code, body = fetch(url, timeout_sec)
        latency_ms = (time.perf_counter() - started) * 1000
        return EndpointResult(
            path=path,
            ok=200 <= status_code < 300,
            status_code=status_code,
            latency_ms=round(latency_ms, 2),
            bytes=len(body),
        )
    except HTTPError as exc:
        latency_ms = (time.perf_counter() - started) * 1000
        return EndpointResult(
            path=path,
            ok=False,
            status_code=exc.code,
            latency_ms=round(latency_ms, 2),
            bytes=0,
            error=f"HTTP {exc.code}",
        )
    except URLError as exc:
        latency_ms = (time.perf_counter() - started) * 1000
        return EndpointResult(
            path=path,
            ok=False,
            status_code=None,
            latency_ms=round(latency_ms, 2),
            bytes=0,
            error=str(exc.reason),
        )
    except Exception as exc:  # pragma: no cover - runtime guard
        latency_ms = (time.perf_counter() - started) * 1000
        return EndpointResult(
            path=path,
            ok=False,
            status_code=None,
            latency_ms=round(latency_ms, 2),
            bytes=0,
            error=repr(exc),
        )


def read_json(base_url: str, path: str, timeout_sec: int) -> dict[str, Any] | None:
    url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    try:
        _, body = fetch(url, timeout_sec)
    except Exception:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except Exception:
        return None


def build_cycle_record(base_url: str, args: argparse.Namespace) -> dict[str, Any]:
    card_path = f"/api/card?group_name={args.group_name}&range_key={args.range_key}"
    endpoints = [*DEFAULT_ENDPOINTS, card_path]
    endpoint_results = [probe_endpoint(base_url, path, args.timeout_sec) for path in endpoints]
    health = read_json(base_url, "/api/health", args.timeout_sec)
    summary = read_json(base_url, "/api/snapshot-summary", args.timeout_sec)
    failures = [item.path for item in endpoint_results if not item.ok]
    return {
        "ts": datetime.now().astimezone().isoformat(),
        "base_url": base_url,
        "overall_ok": not failures,
        "failed_paths": failures,
        "endpoints": [asdict(item) for item in endpoint_results],
        "health": {
            "total_cycles": health.get("total_cycles") if isinstance(health, dict) else None,
            "last_poll_finished_at": health.get("last_poll_finished_at") if isinstance(health, dict) else None,
            "latest_fx_source": health.get("latest_fx_source") if isinstance(health, dict) else None,
            "fx_is_live": health.get("fx_is_live") if isinstance(health, dict) else None,
            "pair_statuses": health.get("pairs") if isinstance(health, dict) else None,
        },
        "summary": {
            "as_of": summary.get("as_of") if isinstance(summary, dict) else None,
            "snapshot_count": len(summary.get("snapshots", [])) if isinstance(summary, dict) else None,
        },
    }


def print_cycle(record: dict[str, Any]) -> None:
    health = record["health"]
    failures = ",".join(record["failed_paths"]) if record["failed_paths"] else "-"
    print(
        f"[{record['ts']}] ok={record['overall_ok']} "
        f"total_cycles={health['total_cycles']} "
        f"last_poll_finished_at={health['last_poll_finished_at']} "
        f"fx={health['latest_fx_source']} "
        f"failed={failures}",
        flush=True,
    )


def main() -> int:
    args = build_parser().parse_args()
    root_dir = Path(__file__).resolve().parents[1]
    log_file = resolve_log_file(root_dir, args.log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    end_monotonic = None
    if args.duration_minutes > 0:
        end_monotonic = time.monotonic() + args.duration_minutes * 60

    cycles_completed = 0
    try:
        with log_file.open("a", encoding="utf-8") as handle:
            while True:
                cycle_started = time.monotonic()
                record = build_cycle_record(args.base_url, args)
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                handle.flush()
                print_cycle(record)
                cycles_completed += 1

                if args.cycles and cycles_completed >= args.cycles:
                    break
                if end_monotonic is not None and time.monotonic() >= end_monotonic:
                    break

                elapsed = time.monotonic() - cycle_started
                sleep_sec = max(0, args.interval_sec - elapsed)
                time.sleep(sleep_sec)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return 130

    print(f"log saved to: {log_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
