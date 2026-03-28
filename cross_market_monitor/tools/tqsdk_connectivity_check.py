from __future__ import annotations

import argparse
import json
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from tqsdk import TqApi, TqAuth

from cross_market_monitor.application.common import active_trading_session_window
from cross_market_monitor.infrastructure.config_loader import load_config

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "monitor.yaml"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "tqsdk_connectivity"
DEFAULT_MD_URL = "wss://free-api.shinnytech.com/t/nfmd/front/mobile"
DEFAULT_DURATION_SEC = 300
DEFAULT_INTERVAL_SEC = 5.0
DEFAULT_CONNECT_TIMEOUT_SEC = 20.0

TQ_SYMBOLS = {
    "au": "KQ.m@SHFE.au",
    "ag": "KQ.m@SHFE.ag",
    "cu": "KQ.m@SHFE.cu",
    "bc": "KQ.m@INE.bc",
    "sc": "KQ.m@INE.sc",
    "al": "KQ.m@SHFE.al",
    "b": "KQ.m@DCE.b",
    "cf": "KQ.m@CZCE.CF",
    "sr": "KQ.m@CZCE.SR",
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(value)
    return str(value)


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


def _safe_age_sec(quote_ts: datetime | None, *, now: datetime) -> float | None:
    if quote_ts is None:
        return None
    return round(max((now - quote_ts).total_seconds(), 0.0), 3)


def _effective_age_sec(
    quote_ts: datetime | None,
    *,
    now: datetime,
    session_start_utc: datetime | None,
) -> float | None:
    if quote_ts is None:
        return None
    reference_ts = quote_ts
    if session_start_utc is not None and reference_ts < session_start_utc:
        reference_ts = session_start_utc
    return _safe_age_sec(reference_ts, now=now)


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


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


@dataclass
class SymbolStats:
    requested_symbol: str
    trading_sessions_local: list[str]
    stale_seconds: int
    timezone_name: str
    non_trading_dates_local: list[str]
    weekends_closed: bool
    attempts: int = 0
    success: int = 0
    fail: int = 0
    ages: list[float] | None = None
    prices: list[float] | None = None
    resolved_symbols: set[str] | None = None
    errors: dict[str, int] | None = None
    in_session_cycles: int = 0
    out_of_session_cycles: int = 0
    stale_in_session_count: int = 0
    in_session_ages: list[float] | None = None
    out_of_session_ages: list[float] | None = None

    def __post_init__(self) -> None:
        if self.ages is None:
            self.ages = []
        if self.prices is None:
            self.prices = []
        if self.resolved_symbols is None:
            self.resolved_symbols = set()
        if self.errors is None:
            self.errors = defaultdict(int)
        if self.in_session_ages is None:
            self.in_session_ages = []
        if self.out_of_session_ages is None:
            self.out_of_session_ages = []

    def active_session_window(self, cycle_ts: datetime) -> tuple[datetime, datetime] | None:
        local_dt = cycle_ts.astimezone(ZoneInfo(self.timezone_name))
        return active_trading_session_window(
            local_dt,
            self.trading_sessions_local,
            non_trading_dates=self.non_trading_dates_local,
            weekends_closed=self.weekends_closed,
        )

    def as_dict(self) -> dict[str, Any]:
        success_rate = round(self.success / self.attempts, 4) if self.attempts else None
        in_session_success = self.in_session_cycles - self.stale_in_session_count
        in_session_fresh_rate = (
            round(in_session_success / self.in_session_cycles, 4) if self.in_session_cycles else None
        )
        return {
            "requested_symbol": self.requested_symbol,
            "trading_sessions_local": self.trading_sessions_local,
            "stale_seconds": self.stale_seconds,
            "attempts": self.attempts,
            "success": self.success,
            "fail": self.fail,
            "success_rate": success_rate,
            "median_age_sec": round(statistics.median(self.ages), 3) if self.ages else None,
            "max_age_sec": round(max(self.ages), 3) if self.ages else None,
            "in_session_cycles": self.in_session_cycles,
            "out_of_session_cycles": self.out_of_session_cycles,
            "stale_in_session_count": self.stale_in_session_count,
            "in_session_fresh_rate": in_session_fresh_rate,
            "median_age_in_session_sec": round(statistics.median(self.in_session_ages), 3) if self.in_session_ages else None,
            "max_age_in_session_sec": round(max(self.in_session_ages), 3) if self.in_session_ages else None,
            "median_age_out_of_session_sec": round(statistics.median(self.out_of_session_ages), 3) if self.out_of_session_ages else None,
            "max_age_out_of_session_sec": round(max(self.out_of_session_ages), 3) if self.out_of_session_ages else None,
            "latest_price": self.prices[-1] if self.prices else None,
            "resolved_symbols": sorted(self.resolved_symbols),
            "error_counts": dict(self.errors),
        }


def _resolve_tqsdk_credentials(config_path: Path) -> tuple[str, str, str]:
    config = load_config(config_path)
    source = config.sources.get("tqsdk_domestic")
    params = dict(source.params or {}) if source else {}

    user = params.get("auth_user") or ""
    password = params.get("auth_password") or ""
    md_url = params.get("md_url") or DEFAULT_MD_URL

    if not user or not password:
        raise SystemExit(
            "Missing TqSdk credentials. Fill config/local.yaml under "
            "sources.tqsdk_domestic.params.auth_user/auth_password."
        )
    return str(user), str(password), str(md_url)


def _resolve_probe_specs(config_path: Path, selected: list[str] | None) -> tuple[str, list[str], bool, dict[str, dict[str, Any]]]:
    config = load_config(config_path)
    timezone_name = config.app.timezone
    non_trading_dates_local = [item.isoformat() for item in config.app.domestic_non_trading_dates_local]
    weekends_closed = config.app.domestic_weekends_closed
    product_codes = []
    seen: set[str] = set()
    specs: dict[str, dict[str, Any]] = {}
    for pair in config.pairs:
        product_code = (pair.domestic_product_code or "").lower().strip()
        if not product_code or product_code in seen or product_code not in TQ_SYMBOLS:
            continue
        seen.add(product_code)
        product_codes.append(product_code)
        specs[product_code] = {
            "requested_symbol": TQ_SYMBOLS[product_code],
            "trading_sessions_local": list(pair.trading_sessions_local),
            "stale_seconds": pair.thresholds.stale_seconds,
        }

    if selected:
        normalized = [item.strip().lower() for item in selected if item.strip()]
        invalid = [item for item in normalized if item not in TQ_SYMBOLS]
        if invalid:
            raise SystemExit(f"Unsupported product codes: {', '.join(invalid)}")
        product_codes = [item for item in normalized if item in specs]

    if not product_codes:
        raise SystemExit("No TqSdk-compatible domestic product codes found in config.")

    selected_specs = {code: specs[code] for code in product_codes}
    return timezone_name, non_trading_dates_local, weekends_closed, selected_specs


def run_check(args: argparse.Namespace) -> int:
    config_path = Path(args.config).resolve()
    user, password, md_url = _resolve_tqsdk_credentials(config_path)
    timezone_name, non_trading_dates_local, weekends_closed, specs = _resolve_probe_specs(config_path, args.products)
    symbols = {code: spec["requested_symbol"] for code, spec in specs.items()}
    output_dir = _ensure_dir(Path(args.output_dir).resolve())

    run_started_at = _utc_now()
    run_id = run_started_at.strftime("%Y%m%d_%H%M%S")
    api: TqApi | None = None
    quotes: dict[str, Any] = {}
    connect_attempts: list[dict[str, Any]] = []
    stats = {
        code: SymbolStats(
            requested_symbol=spec["requested_symbol"],
            trading_sessions_local=spec["trading_sessions_local"],
            stale_seconds=spec["stale_seconds"],
            timezone_name=timezone_name,
            non_trading_dates_local=non_trading_dates_local,
            weekends_closed=weekends_closed,
        )
        for code, spec in specs.items()
    }
    refresh_latencies: list[float] = []
    refresh_updates = 0
    samples: list[dict[str, Any]] = []
    setup_error: dict[str, Any] | None = None

    for attempt in range(1, args.connect_attempts + 1):
        started = time.perf_counter()
        attempt_row: dict[str, Any] = {
            "attempt": attempt,
            "started_at": _utc_now(),
            "md_url": md_url,
        }
        try:
            api = TqApi(
                auth=TqAuth(user, password),
                disable_print=True,
                _stock=True,
                _md_url=md_url,
            )
            quotes = {code: api.get_quote(symbol) for code, symbol in symbols.items()}
            api.wait_update(deadline=time.time() + args.connect_timeout_sec)
            ready = any(
                getattr(quote, "last_price", None) is not None and getattr(quote, "datetime", None) not in (None, "")
                for quote in quotes.values()
            )
            attempt_row["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 2)
            attempt_row["ok"] = ready
            if ready:
                connect_attempts.append(attempt_row)
                break
            attempt_row["error_type"] = "NotReady"
            attempt_row["error"] = "Connected but no quotes became ready before timeout"
        except Exception as exc:
            attempt_row["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 2)
            attempt_row["ok"] = False
            attempt_row["error_type"] = type(exc).__name__
            attempt_row["error"] = str(exc)
            if api is not None:
                try:
                    api.close()
                except Exception:
                    pass
                api = None
                quotes = {}
        connect_attempts.append(attempt_row)
        time.sleep(args.reconnect_sleep_sec)

    if api is None or not quotes:
        setup_error = connect_attempts[-1] if connect_attempts else {"error": "Failed to initialize TqSdk"}
    else:
        start_monotonic = time.monotonic()
        next_tick = start_monotonic
        while time.monotonic() - start_monotonic < args.duration_sec:
            cycle_started = time.perf_counter()
            updated = False
            try:
                updated = bool(api.wait_update(deadline=time.time() + max(args.interval_sec * 0.75, 2.0)))
            except Exception as exc:
                setup_error = {
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "phase": "polling",
                    "at": _utc_now(),
                }
                break

            refresh_latency = (time.perf_counter() - cycle_started) * 1000
            refresh_latencies.append(refresh_latency)
            if updated:
                refresh_updates += 1

            cycle_ts = _utc_now()
            for code, quote in quotes.items():
                stat = stats[code]
                stat.attempts += 1
                last_price = getattr(quote, "last_price", None)
                quote_ts = _parse_tqsdk_time(getattr(quote, "datetime", None))
                resolved_symbol = getattr(quote, "underlying_symbol", None)
                if resolved_symbol:
                    stat.resolved_symbols.add(str(resolved_symbol))
                session_window = stat.active_session_window(cycle_ts)
                in_session = session_window is not None
                session_start_utc = session_window[0].astimezone(UTC) if session_window is not None else None
                if in_session:
                    stat.in_session_cycles += 1
                else:
                    stat.out_of_session_cycles += 1

                if last_price is not None and quote_ts is not None:
                    raw_age_sec = _safe_age_sec(quote_ts, now=cycle_ts)
                    age_sec = _effective_age_sec(quote_ts, now=cycle_ts, session_start_utc=session_start_utc)
                    stat.success += 1
                    if age_sec is not None:
                        stat.ages.append(age_sec)
                        if in_session:
                            stat.in_session_ages.append(age_sec)
                            if age_sec > stat.stale_seconds:
                                stat.stale_in_session_count += 1
                        else:
                            stat.out_of_session_ages.append(age_sec)
                    stat.prices.append(float(last_price))
                    samples.append(
                        {
                            "product_code": code,
                            "requested_symbol": stat.requested_symbol,
                            "resolved_symbol": resolved_symbol,
                            "price": float(last_price),
                            "quote_ts": quote_ts,
                            "raw_age_sec": raw_age_sec,
                            "age_sec": age_sec,
                            "in_trading_session": in_session,
                            "stale_in_session": bool(in_session and age_sec is not None and age_sec > stat.stale_seconds),
                            "refresh_latency_ms": round(refresh_latency, 2),
                            "refresh_updated": updated,
                            "cycle_ts": cycle_ts,
                        }
                    )
                else:
                    stat.fail += 1
                    if in_session:
                        session_age_sec = _safe_age_sec(session_start_utc, now=cycle_ts)
                        if session_age_sec is None or session_age_sec > stat.stale_seconds:
                            stat.stale_in_session_count += 1
                    if last_price is None and quote_ts is None:
                        stat.errors["missing_price_and_datetime"] += 1
                    elif last_price is None:
                        stat.errors["missing_price"] += 1
                    else:
                        stat.errors["missing_datetime"] += 1

            next_tick += args.interval_sec
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)

    if api is not None:
        try:
            api.close()
        except Exception:
            pass

    summary = {
        "started_at": run_started_at,
        "ended_at": _utc_now(),
        "config_path": str(config_path),
        "timezone": timezone_name,
        "domestic_non_trading_dates_local": non_trading_dates_local,
        "domestic_weekends_closed": weekends_closed,
        "duration_sec": args.duration_sec,
        "interval_sec": args.interval_sec,
        "connect_timeout_sec": args.connect_timeout_sec,
        "md_url": md_url,
        "connect_attempts": connect_attempts,
        "connect_success": setup_error is None,
        "setup_error": setup_error,
        "refresh_cycles": len(refresh_latencies),
        "refresh_updated_cycles": refresh_updates,
        "refresh_update_ratio": round(refresh_updates / len(refresh_latencies), 4) if refresh_latencies else None,
        "refresh_latency": _latency_summary(refresh_latencies),
        "symbols": {code: stat.as_dict() for code, stat in stats.items()},
    }

    output_path = output_dir / f"tqsdk_connectivity_{run_id}.json"
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=_json_default) + "\n", encoding="utf-8")

    print(json.dumps({"output": str(output_path), "summary": summary}, ensure_ascii=False, default=_json_default))
    return 0 if summary["connect_success"] else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Server-side TqSdk connectivity check for domestic night-session data")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to monitor.yaml")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for JSON reports")
    parser.add_argument("--duration-sec", type=int, default=DEFAULT_DURATION_SEC, help="Sampling duration after successful connect")
    parser.add_argument("--interval-sec", type=float, default=DEFAULT_INTERVAL_SEC, help="Seconds between samples")
    parser.add_argument("--connect-timeout-sec", type=float, default=DEFAULT_CONNECT_TIMEOUT_SEC, help="Per-attempt connect timeout")
    parser.add_argument("--connect-attempts", type=int, default=3, help="Number of connect retries before failing")
    parser.add_argument("--reconnect-sleep-sec", type=float, default=3.0, help="Sleep between connect retries")
    parser.add_argument("--products", nargs="*", default=None, help="Optional subset of products: au ag cu bc sc")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(run_check(args))


if __name__ == "__main__":
    main()
