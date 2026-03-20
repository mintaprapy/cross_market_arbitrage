from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any

from tqsdk import TqApi, TqAuth

from cross_market_monitor.application.service import _build_adapter
from cross_market_monitor.infrastructure.config_loader import load_config
from cross_market_monitor.infrastructure.http_client import HttpClient

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "monitor.yaml"
DEFAULT_TQ_MD_URL = "wss://free-api.shinnytech.com/t/nfmd/front/mobile"
DEFAULT_DURATION_HOURS = 5.0
DEFAULT_INTERVAL_SEC = 10.0
HKT = timezone(timedelta(hours=8))

TQ_SYMBOLS = {
    "au": "KQ.m@SHFE.au",
    "ag": "KQ.m@SHFE.ag",
    "cu": "KQ.m@SHFE.cu",
    "sc": "KQ.m@INE.sc",
    "bc": "KQ.m@INE.bc",
}


def utc_now() -> datetime:
    return datetime.now(UTC)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default) + "\n", encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, default=_json_default) + "\n")


def _safe_age_sec(quote_ts: datetime | None, *, now: datetime) -> float | None:
    if quote_ts is None:
        return None
    return round(max((now - quote_ts).total_seconds(), 0.0), 3)


def _parse_tqsdk_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if " " in text and "+" not in text:
        text = text.replace(" ", "T") + "+08:00"
    try:
        return datetime.fromisoformat(text).astimezone(UTC)
    except ValueError:
        return None


def _latency_stats(latencies: list[float]) -> dict[str, float | None]:
    if not latencies:
        return {"median_ms": None, "p95_ms": None, "max_ms": None}
    ordered = sorted(latencies)
    p95_index = max(int(len(ordered) * 0.95) - 1, 0)
    return {
        "median_ms": round(median(ordered), 2),
        "p95_ms": round(ordered[p95_index], 2),
        "max_ms": round(max(ordered), 2),
    }


def _age_stats(ages: list[float]) -> dict[str, float | None]:
    if not ages:
        return {"median_age_sec": None, "max_age_sec": None}
    return {
        "median_age_sec": round(median(ages), 3),
        "max_age_sec": round(max(ages), 3),
    }


def _build_probe_pairs(config) -> list[dict[str, Any]]:
    seen_products: set[str] = set()
    pairs: list[dict[str, Any]] = []
    for pair in config.pairs:
        product_code = (pair.domestic_product_code or "").lower()
        if not product_code or product_code in seen_products or product_code not in TQ_SYMBOLS:
            continue
        seen_products.add(product_code)
        pairs.append(
            {
                "group_name": pair.group_name,
                "product_code": product_code,
                "current_main_source": pair.domestic_source,
                "current_main_symbol": pair.domestic_symbol,
                "current_main_label": pair.domestic_label,
                "tqsdk_symbol": TQ_SYMBOLS[product_code],
            }
        )
    return pairs


@dataclass
class ProbeContext:
    run_dir: Path
    events_path: Path
    status_path: Path
    summary_json_path: Path
    summary_md_path: Path


class TqSdkProbe:
    def __init__(self, user: str, password: str, md_url: str) -> None:
        self.user = user
        self.password = password
        self.md_url = md_url
        self.api: TqApi | None = None
        self.quotes: dict[str, Any] = {}

    def connect(self, symbols: list[str], *, initial_deadline_sec: float = 15.0) -> None:
        self.close()
        self.api = TqApi(
            auth=TqAuth(self.user, self.password),
            disable_print=True,
            _stock=True,
            _md_url=self.md_url,
        )
        self.quotes = {symbol: self.api.get_quote(symbol) for symbol in symbols}
        if initial_deadline_sec > 0:
            self.api.wait_update(deadline=time.time() + initial_deadline_sec)

    def refresh(self, deadline_sec: float = 2.0) -> tuple[float, bool]:
        if self.api is None:
            raise RuntimeError("TqSdk probe is not connected")
        started = time.perf_counter()
        updated = self.api.wait_update(deadline=time.time() + deadline_sec)
        return (time.perf_counter() - started) * 1000, bool(updated)

    def probe_quote(
        self,
        symbol: str,
        *,
        cycle_ts: datetime,
        refresh_latency_ms: float,
        refresh_updated: bool,
    ) -> dict[str, Any]:
        quote = self.quotes[symbol]
        quote_ts = _parse_tqsdk_time(getattr(quote, "datetime", None))
        last_price = getattr(quote, "last_price", None)
        success = last_price is not None and quote_ts is not None
        return {
            "source": "tqsdk_main",
            "product_code": symbol.split("@", 1)[1].split(".")[-1],
            "requested_symbol": symbol,
            "resolved_symbol": getattr(quote, "underlying_symbol", None),
            "success": success,
            "latency_ms": round(refresh_latency_ms, 2),
            "cycle_ts": cycle_ts,
            "quote_ts": quote_ts,
            "age_sec": _safe_age_sec(quote_ts, now=cycle_ts),
            "price": float(last_price) if last_price is not None else None,
            "refresh_updated": refresh_updated,
            "error_type": None,
            "error": None if success else "missing_quote_fields",
        }

    def close(self) -> None:
        if self.api is not None:
            try:
                self.api.close()
            except Exception:
                pass
        self.api = None
        self.quotes = {}


def _select_shfe_contract(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            float(item.get("open_interest") or 0.0),
            float(item.get("volume") or 0.0),
            str(item.get("symbol") or ""),
        ),
    )


def _current_main_event(pair: dict[str, Any], adapter, *, cycle_ts: datetime) -> dict[str, Any]:
    started = time.perf_counter()
    quote = adapter.fetch_quote(pair["current_main_symbol"], pair["current_main_label"])
    latency_ms = (time.perf_counter() - started) * 1000
    return {
        "source": "current_main",
        "product_code": pair["product_code"],
        "requested_symbol": pair["current_main_symbol"],
        "resolved_symbol": quote.symbol,
        "success": True,
        "latency_ms": round(latency_ms, 2),
        "cycle_ts": cycle_ts,
        "quote_ts": quote.ts,
        "age_sec": _safe_age_sec(quote.ts, now=cycle_ts),
        "price": quote.last,
        "error_type": None,
        "error": None,
    }


def _current_contract_event(pair: dict[str, Any], adapter, *, cycle_ts: datetime) -> dict[str, Any]:
    started = time.perf_counter()
    candidates = adapter.list_contracts(pair["product_code"], limit=12)
    selected = _select_shfe_contract(candidates)
    if selected is None:
        raise ValueError(f"No SHFE/INE contracts available for {pair['product_code']}")
    quote = adapter.fetch_quote(selected["symbol"], selected["label"])
    latency_ms = (time.perf_counter() - started) * 1000
    return {
        "source": "current_contract",
        "product_code": pair["product_code"],
        "requested_symbol": pair["product_code"],
        "resolved_symbol": quote.symbol,
        "success": True,
        "latency_ms": round(latency_ms, 2),
        "cycle_ts": cycle_ts,
        "quote_ts": quote.ts,
        "age_sec": _safe_age_sec(quote.ts, now=cycle_ts),
        "price": quote.last,
        "error_type": None,
        "error": None,
    }


def _failure_event(
    *,
    source: str,
    product_code: str,
    requested_symbol: str,
    resolved_symbol: str | None,
    cycle_ts: datetime,
    started: float,
    exc: Exception,
) -> dict[str, Any]:
    return {
        "source": source,
        "product_code": product_code,
        "requested_symbol": requested_symbol,
        "resolved_symbol": resolved_symbol,
        "success": False,
        "latency_ms": round((time.perf_counter() - started) * 1000, 2),
        "cycle_ts": cycle_ts,
        "quote_ts": None,
        "age_sec": None,
        "price": None,
        "error_type": type(exc).__name__,
        "error": str(exc),
    }


def _build_summary(events_path: Path, *, started_at: datetime, expected_end_at: datetime | None = None) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    if events_path.exists():
        with events_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                events.append(json.loads(line))

    summary: dict[str, Any] = {
        "started_at": started_at,
        "ended_at": utc_now(),
        "expected_end_at": expected_end_at,
        "event_count": len(events),
        "overall": [],
        "per_symbol": [],
    }

    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_symbol: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in events:
        by_source[str(item["source"])].append(item)
        by_symbol[(str(item["source"]), str(item["product_code"]))].append(item)

    for source_name, source_events in sorted(by_source.items()):
        successes = [item for item in source_events if item["success"]]
        failures = [item for item in source_events if not item["success"]]
        latencies = [float(item["latency_ms"]) for item in source_events if item.get("latency_ms") is not None]
        ages = [float(item["age_sec"]) for item in successes if item.get("age_sec") is not None]
        errors = Counter(str(item["error_type"]) for item in failures if item.get("error_type"))
        summary["overall"].append(
            {
                "source": source_name,
                "attempts": len(source_events),
                "successes": len(successes),
                "failures": len(failures),
                "success_rate": round(len(successes) / len(source_events), 4) if source_events else None,
                **_latency_stats(latencies),
                **_age_stats(ages),
                "top_errors": errors.most_common(5),
            }
        )

    for (source_name, product_code), source_events in sorted(by_symbol.items()):
        successes = [item for item in source_events if item["success"]]
        failures = [item for item in source_events if not item["success"]]
        latencies = [float(item["latency_ms"]) for item in source_events if item.get("latency_ms") is not None]
        ages = [float(item["age_sec"]) for item in successes if item.get("age_sec") is not None]
        errors = Counter(str(item["error_type"]) for item in failures if item.get("error_type"))
        latest_success = successes[-1] if successes else None
        summary["per_symbol"].append(
            {
                "source": source_name,
                "product_code": product_code,
                "attempts": len(source_events),
                "successes": len(successes),
                "failures": len(failures),
                "success_rate": round(len(successes) / len(source_events), 4) if source_events else None,
                **_latency_stats(latencies),
                **_age_stats(ages),
                "latest_price": latest_success.get("price") if latest_success else None,
                "latest_requested_symbol": latest_success.get("requested_symbol") if latest_success else None,
                "latest_resolved_symbol": latest_success.get("resolved_symbol") if latest_success else None,
                "latest_quote_ts": latest_success.get("quote_ts") if latest_success else None,
                "top_errors": errors.most_common(5),
            }
        )
    return summary


def _summary_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Stability Probe Summary",
        "",
        f"- Started: {summary['started_at']}",
        f"- Ended: {summary['ended_at']}",
        f"- Expected End: {summary.get('expected_end_at') or '--'}",
        f"- Event Count: {summary['event_count']}",
        "",
        "## Overall",
        "",
        "| Source | Attempts | Successes | Failures | Success Rate | Median Latency (ms) | P95 Latency (ms) | Max Age (s) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in summary["overall"]:
        success_rate = "--" if item["success_rate"] is None else f"{item['success_rate'] * 100:.2f}%"
        lines.append(
            f"| {item['source']} | {item['attempts']} | {item['successes']} | {item['failures']} | "
            f"{success_rate} | "
            f"{item['median_ms'] if item['median_ms'] is not None else '--'} | "
            f"{item['p95_ms'] if item['p95_ms'] is not None else '--'} | "
            f"{item['max_age_sec'] if item['max_age_sec'] is not None else '--'} |"
        )
        if item["top_errors"]:
            lines.append("")
            lines.append(f"Top errors for `{item['source']}`: {item['top_errors']}")
            lines.append("")

    lines.extend(
        [
            "## Per Symbol",
            "",
            "| Source | Product | Success Rate | Median Latency (ms) | Max Age (s) | Latest Symbol | Latest Price |",
            "|---|---|---:|---:|---:|---|---:|",
        ]
    )
    for item in summary["per_symbol"]:
        latest_symbol = item["latest_resolved_symbol"] or item["latest_requested_symbol"] or "--"
        latest_price = item["latest_price"]
        success_rate = "--" if item["success_rate"] is None else f"{item['success_rate'] * 100:.2f}%"
        latest_price_text = "--" if latest_price is None else f"{latest_price:.4f}"
        lines.append(
            f"| {item['source']} | {item['product_code']} | "
            f"{success_rate} | "
            f"{item['median_ms'] if item['median_ms'] is not None else '--'} | "
            f"{item['max_age_sec'] if item['max_age_sec'] is not None else '--'} | "
            f"{latest_symbol} | "
            f"{latest_price_text} |"
        )
        if item["top_errors"]:
            lines.append("")
            lines.append(f"Errors for `{item['source']}:{item['product_code']}`: {item['top_errors']}")
            lines.append("")

    return "\n".join(lines) + "\n"


def _latest_run_dir(base_dir: Path) -> Path:
    candidates = [item for item in base_dir.iterdir() if item.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No run directories found under {base_dir}")
    return sorted(candidates)[-1]


def run_probe(args: argparse.Namespace) -> int:
    user = os.environ.get("TQSDK_USER")
    password = os.environ.get("TQSDK_PASSWORD")
    if not user or not password:
        print("Missing TQSDK_USER or TQSDK_PASSWORD in environment.", file=sys.stderr)
        return 2

    config = load_config(args.config)
    probe_pairs = _build_probe_pairs(config)
    if not probe_pairs:
        print("No probe pairs found in config.", file=sys.stderr)
        return 2

    base_dir = _ensure_dir(Path(args.output_dir))
    started_at = utc_now()
    run_id = started_at.astimezone(HKT).strftime("%Y%m%d_%H%M%S")
    run_dir = _ensure_dir(base_dir / run_id)
    context = ProbeContext(
        run_dir=run_dir,
        events_path=run_dir / "events.jsonl",
        status_path=run_dir / "status.json",
        summary_json_path=run_dir / "summary.json",
        summary_md_path=run_dir / "summary.md",
    )

    sina_client = HttpClient(
        timeout_sec=config.app.http_timeout_sec,
        verify_ssl=config.sources["sina_domestic"].verify_ssl,
    )
    shfe_client = HttpClient(
        timeout_sec=config.app.http_timeout_sec,
        verify_ssl=config.sources["shfe_domestic"].verify_ssl,
    )
    current_main_adapter = _build_adapter("sina_domestic", config.sources["sina_domestic"], sina_client)
    current_contract_adapter = _build_adapter("shfe_domestic", config.sources["shfe_domestic"], shfe_client)

    tq_probe = TqSdkProbe(user, password, os.environ.get("TQSDK_MD_URL", DEFAULT_TQ_MD_URL))
    tq_symbols = [item["tqsdk_symbol"] for item in probe_pairs]

    duration = timedelta(hours=float(args.duration_hours))
    expected_end_at = started_at + duration
    _write_json(
        context.status_path,
        {
            "run_id": run_id,
            "started_at": started_at,
            "expected_end_at": expected_end_at,
            "status": "starting",
            "pairs": probe_pairs,
            "events_path": str(context.events_path),
            "summary_json_path": str(context.summary_json_path),
            "summary_md_path": str(context.summary_md_path),
        },
    )

    cycles = 0
    end_at_monotonic = time.monotonic() + duration.total_seconds()

    while time.monotonic() < end_at_monotonic:
        cycle_started_perf = time.perf_counter()
        cycle_ts = utc_now()
        cycles += 1

        for pair in probe_pairs:
            started = time.perf_counter()
            try:
                event = _current_main_event(pair, current_main_adapter, cycle_ts=cycle_ts)
            except Exception as exc:
                event = _failure_event(
                    source="current_main",
                    product_code=pair["product_code"],
                    requested_symbol=pair["current_main_symbol"],
                    resolved_symbol=None,
                    cycle_ts=cycle_ts,
                    started=started,
                    exc=exc,
                )
            _append_jsonl(context.events_path, event)

            started = time.perf_counter()
            try:
                event = _current_contract_event(pair, current_contract_adapter, cycle_ts=cycle_ts)
            except Exception as exc:
                event = _failure_event(
                    source="current_contract",
                    product_code=pair["product_code"],
                    requested_symbol=pair["product_code"],
                    resolved_symbol=None,
                    cycle_ts=cycle_ts,
                    started=started,
                    exc=exc,
                )
            _append_jsonl(context.events_path, event)

        tq_refresh_started = time.perf_counter()
        try:
            if tq_probe.api is None:
                tq_probe.connect(tq_symbols, initial_deadline_sec=max(args.interval_sec, 15.0))
            tq_refresh_started = time.perf_counter()
            refresh_latency_ms, refresh_updated = tq_probe.refresh(deadline_sec=max(args.interval_sec * 0.75, 2.0))
            for pair in probe_pairs:
                event = tq_probe.probe_quote(
                    pair["tqsdk_symbol"],
                    cycle_ts=cycle_ts,
                    refresh_latency_ms=refresh_latency_ms,
                    refresh_updated=refresh_updated,
                )
                event["product_code"] = pair["product_code"]
                _append_jsonl(context.events_path, event)
        except Exception as exc:
            tq_probe.close()
            for pair in probe_pairs:
                _append_jsonl(
                    context.events_path,
                    _failure_event(
                        source="tqsdk_main",
                        product_code=pair["product_code"],
                        requested_symbol=pair["tqsdk_symbol"],
                        resolved_symbol=None,
                        cycle_ts=cycle_ts,
                        started=tq_refresh_started,
                        exc=exc,
                    ),
                )

        if cycles == 1 or cycles % max(int(300 / max(args.interval_sec, 1)), 1) == 0:
            summary = _build_summary(context.events_path, started_at=started_at, expected_end_at=expected_end_at)
            _write_json(context.summary_json_path, summary)
            context.summary_md_path.write_text(_summary_markdown(summary), encoding="utf-8")
            _write_json(
                context.status_path,
                {
                    "run_id": run_id,
                    "started_at": started_at,
                    "expected_end_at": expected_end_at,
                    "last_cycle_at": cycle_ts,
                    "cycles_completed": cycles,
                    "status": "running",
                    "summary_json_path": str(context.summary_json_path),
                    "summary_md_path": str(context.summary_md_path),
                },
            )

        sleep_for = max(args.interval_sec - (time.perf_counter() - cycle_started_perf), 0.0)
        if sleep_for > 0:
            time.sleep(sleep_for)

    summary = _build_summary(context.events_path, started_at=started_at, expected_end_at=expected_end_at)
    _write_json(context.summary_json_path, summary)
    context.summary_md_path.write_text(_summary_markdown(summary), encoding="utf-8")
    _write_json(
        context.status_path,
        {
            "run_id": run_id,
            "started_at": started_at,
            "expected_end_at": expected_end_at,
            "completed_at": utc_now(),
            "cycles_completed": cycles,
            "status": "completed",
            "summary_json_path": str(context.summary_json_path),
            "summary_md_path": str(context.summary_md_path),
        },
    )
    tq_probe.close()
    print(str(context.run_dir))
    return 0


def report_probe(args: argparse.Namespace) -> int:
    base_dir = Path(args.output_dir)
    run_dir = Path(args.run_dir) if args.run_dir else _latest_run_dir(base_dir)
    status_path = run_dir / "status.json"
    events_path = run_dir / "events.jsonl"
    if not status_path.exists() or not events_path.exists():
        print(f"Missing status.json or events.jsonl in {run_dir}", file=sys.stderr)
        return 2
    status = json.loads(status_path.read_text(encoding="utf-8"))
    started_at = datetime.fromisoformat(status["started_at"])
    expected_end_at = (
        datetime.fromisoformat(status["expected_end_at"]) if status.get("expected_end_at") else None
    )
    summary = _build_summary(events_path, started_at=started_at, expected_end_at=expected_end_at)
    summary_json_path = run_dir / "summary.json"
    summary_md_path = run_dir / "summary.md"
    _write_json(summary_json_path, summary)
    summary_md_path.write_text(_summary_markdown(summary), encoding="utf-8")
    if args.format == "json":
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=_json_default))
    else:
        print(summary_md_path.read_text(encoding="utf-8"))
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Realtime stability probe for current domestic sources vs TqSdk")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to monitor YAML config")
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parents[2] / "data" / "stability"),
        help="Directory for run outputs",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the probe loop")
    run_parser.add_argument("--duration-hours", type=float, default=DEFAULT_DURATION_HOURS, help="Total probe duration in hours")
    run_parser.add_argument("--interval-sec", type=float, default=DEFAULT_INTERVAL_SEC, help="Seconds between probe cycles")

    report_parser = subparsers.add_parser("report", help="Render a summary for a completed or running probe")
    report_parser.add_argument("--run-dir", default=None, help="Specific run directory; defaults to latest")
    report_parser.add_argument("--format", choices=["text", "json"], default="text")

    args = parser.parse_args()
    if args.command == "run":
        raise SystemExit(run_probe(args))
    raise SystemExit(report_probe(args))


if __name__ == "__main__":
    main()
