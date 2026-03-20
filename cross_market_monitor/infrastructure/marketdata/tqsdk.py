from __future__ import annotations

import json
import os
import threading
import time
from datetime import UTC, datetime
from typing import Any, Callable

from cross_market_monitor.domain.models import MarketQuote, SourceConfig
from cross_market_monitor.infrastructure.http_client import HttpClient

try:  # pragma: no cover - import availability depends on runtime environment
    from tqsdk import TqApi, TqAuth
except Exception:  # pragma: no cover - keep module importable without tqsdk installed
    TqApi = None
    TqAuth = None

TQ_MAIN_SYMBOLS = {
    "au": "KQ.m@SHFE.au",
    "ag": "KQ.m@SHFE.ag",
    "cu": "KQ.m@SHFE.cu",
    "sc": "KQ.m@INE.sc",
    "bc": "KQ.m@INE.bc",
}

TQ_INTERVAL_SECONDS = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "60m": 3600,
    "1d": 86400,
}

NON_RETRYABLE_TQ_ERRORS = (
    "用户权限认证失败",
    "INVALID_CREDENTIALS",
    "Missing TQSDK credentials",
    "tqsdk is not installed",
    "Unsupported TqSdk interval",
)


def tqsdk_main_symbol_for_product(product_code: str | None) -> str | None:
    if not product_code:
        return None
    return TQ_MAIN_SYMBOLS.get(product_code.lower())


class TqSdkMainAdapter:
    def __init__(self, source_name: str, source_config: SourceConfig, http_client: HttpClient | None = None) -> None:
        self.source_name = source_name
        self.source_config = source_config
        self.http_client = http_client

    def is_configured(self) -> bool:
        user, password = self._credentials()
        return bool(user and password and TqApi and TqAuth)

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        api = self._create_api()
        try:
            quote = api.get_quote(symbol)
            api.wait_update(deadline=time.time() + 8)
            return self._quote_from_live(symbol, label, quote)
        finally:
            _safe_close_api(api)

    def fetch_history(
        self,
        symbol: str,
        label: str,
        *,
        interval: str = "30m",
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
    ) -> list[MarketQuote]:
        duration_seconds = TQ_INTERVAL_SECONDS.get(interval)
        if duration_seconds is None:
            raise ValueError(f"Unsupported TqSdk interval: {interval}")

        def load_history_once() -> list[MarketQuote]:
            api = self._create_api(use_retry=False)
            try:
                klines = api.get_kline_serial(symbol, duration_seconds, data_length=8000)
                for _ in range(10):
                    api.wait_update(deadline=time.time() + 1)

                quotes: list[MarketQuote] = []
                for _, row in klines.iterrows():
                    ts = _parse_kline_ts(row.get("datetime"))
                    if ts is None:
                        continue
                    if start_ts is not None and ts < start_ts:
                        continue
                    if end_ts is not None and ts > end_ts:
                        continue
                    close_px = _float_or_none(row.get("close"))
                    if close_px is None:
                        continue
                    quotes.append(
                        MarketQuote(
                            source_name=self.source_name,
                            symbol=symbol,
                            label=label,
                            ts=ts,
                            last=close_px,
                            bid=None,
                            ask=None,
                            raw_payload=json.dumps(
                                {
                                    "row_symbol": row.get("symbol"),
                                    "duration": row.get("duration"),
                                    "open": _float_or_none(row.get("open")),
                                    "high": _float_or_none(row.get("high")),
                                    "low": _float_or_none(row.get("low")),
                                    "close": close_px,
                                    "volume": _float_or_none(row.get("volume")),
                                    "open_oi": _float_or_none(row.get("open_oi")),
                                    "close_oi": _float_or_none(row.get("close_oi")),
                                },
                                ensure_ascii=False,
                            ),
                        )
                    )
                return quotes
            finally:
                _safe_close_api(api)

        return self._call_with_retry(
            "history fetch",
            load_history_once,
            attempts_key="history_retry_attempts",
            default_attempts=3,
        )

    def create_live_api(self) -> Any:
        return self._create_api()

    def build_live_quote(self, symbol: str, label: str, quote: Any) -> MarketQuote:
        return self._quote_from_live(symbol, label, quote)

    def _create_api(self, *, use_retry: bool = True) -> Any:
        if use_retry:
            return self._call_with_retry(
                "login",
                self._create_api_once,
                attempts_key="login_retry_attempts",
                default_attempts=3,
            )
        return self._create_api_once()

    def _create_api_once(self) -> Any:
        if TqApi is None or TqAuth is None:
            raise RuntimeError("tqsdk is not installed in the current Python environment")

        user, password = self._credentials()
        if not user or not password:
            raise RuntimeError("Missing TQSDK credentials in config or environment")

        return TqApi(
            auth=TqAuth(user, password),
            disable_print=True,
            _stock=True,
            _md_url=self._md_url(),
        )

    def _credentials(self) -> tuple[str | None, str | None]:
        user = self.source_config.params.get("auth_user")
        password = self.source_config.params.get("auth_password")
        user_env = self.source_config.params.get("auth_user_env", "TQSDK_USER")
        password_env = self.source_config.params.get("auth_password_env", "TQSDK_PASSWORD")
        return user or os.environ.get(user_env), password or os.environ.get(password_env)

    def _md_url(self) -> str:
        md_url_env = self.source_config.params.get("md_url_env", "TQSDK_MD_URL")
        return os.environ.get(md_url_env) or self.source_config.base_url

    def _call_with_retry(
        self,
        action_name: str,
        operation: Callable[[], Any],
        *,
        attempts_key: str,
        default_attempts: int,
    ) -> Any:
        attempts = self._int_param(attempts_key, default_attempts)
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return operation()
            except Exception as exc:
                last_error = exc
                if attempt >= attempts or not self._is_retryable_error(exc):
                    raise
                time.sleep(self._retry_delay_sec(attempt))
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"TqSdk {action_name} failed without raising an exception")

    def _retry_delay_sec(self, attempt: int) -> float:
        base_delay = self._float_param("retry_backoff_sec", 1.5)
        max_delay = self._float_param("retry_max_backoff_sec", max(base_delay, 8.0))
        return min(base_delay * (2 ** max(attempt - 1, 0)), max_delay)

    def _is_retryable_error(self, exc: Exception) -> bool:
        message = str(exc)
        return not any(token in message for token in NON_RETRYABLE_TQ_ERRORS)

    def _int_param(self, key: str, default: int) -> int:
        raw = self.source_config.params.get(key)
        if raw is None:
            return default
        try:
            return max(int(raw), 1)
        except (TypeError, ValueError):
            return default

    def _float_param(self, key: str, default: float) -> float:
        raw = self.source_config.params.get(key)
        if raw is None:
            return default
        try:
            return max(float(raw), 0.0)
        except (TypeError, ValueError):
            return default

    def _quote_from_live(self, symbol: str, label: str, quote: Any) -> MarketQuote:
        ts = _parse_quote_ts(getattr(quote, "datetime", None))
        last_px = _float_or_none(getattr(quote, "last_price", None))
        if ts is None or last_px is None:
            raise RuntimeError(f"TqSdk returned incomplete quote for {symbol}")
        return MarketQuote(
            source_name=self.source_name,
            symbol=symbol,
            label=label,
            ts=ts,
            last=last_px,
            bid=_float_or_none(getattr(quote, "bid_price1", None)),
            ask=_float_or_none(getattr(quote, "ask_price1", None)),
            raw_payload=json.dumps(
                {
                    "underlying_symbol": getattr(quote, "underlying_symbol", None),
                    "datetime": getattr(quote, "datetime", None),
                },
                ensure_ascii=False,
            ),
        )


class TqSdkShadowRunner:
    def __init__(
        self,
        adapter: TqSdkMainAdapter,
        specs: list[dict[str, Any]],
        interval_sec: int,
        on_quote: Callable[[dict[str, Any], MarketQuote], None],
        on_success: Callable[[str, str, float], None],
        on_failure: Callable[[str, str, float, str], None],
    ) -> None:
        self.adapter = adapter
        self.specs = specs
        self.interval_sec = max(interval_sec, 1)
        self.on_quote = on_quote
        self.on_success = on_success
        self.on_failure = on_failure

    def run(self, stop_event: threading.Event) -> None:
        if not self.specs:
            return

        api = None
        try:
            api = self.adapter.create_live_api()
            quotes = {
                spec["symbol"]: api.get_quote(spec["symbol"])
                for spec in self.specs
            }
            api.wait_update(deadline=time.time() + min(max(self.interval_sec, 2), 15))
            next_sample_at = time.monotonic()
            while not stop_event.is_set():
                now = time.monotonic()
                remaining = next_sample_at - now
                started = time.perf_counter()
                try:
                    if remaining > 0:
                        api.wait_update(deadline=time.time() + remaining)
                        continue
                    api.wait_update(deadline=time.time() + min(self.interval_sec, 2))
                    latency_ms = (time.perf_counter() - started) * 1000
                    for spec in self.specs:
                        quote = self.adapter.build_live_quote(spec["symbol"], spec["label"], quotes[spec["symbol"]])
                        self.on_quote(spec, quote)
                        self.on_success(self.adapter.source_name, spec["symbol"], latency_ms)
                    next_sample_at = time.monotonic() + self.interval_sec
                except Exception as exc:
                    latency_ms = (time.perf_counter() - started) * 1000
                    for spec in self.specs:
                        self.on_failure(self.adapter.source_name, spec["symbol"], latency_ms, str(exc))
                    next_sample_at = time.monotonic() + self.interval_sec
        finally:
            _safe_close_api(api)


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    parsed = float(value)
    return parsed if parsed > 0 else None


def _parse_quote_ts(value: Any) -> datetime | None:
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


def _parse_kline_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    raw = float(value)
    if raw <= 0:
        return None
    return datetime.fromtimestamp(raw / 1_000_000_000, tz=UTC)


def _safe_close_api(api: Any) -> None:
    if api is None:
        return
    try:
        api.close()
    except Exception:
        pass
