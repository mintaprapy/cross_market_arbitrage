from __future__ import annotations

import json
import re
from urllib.parse import urlsplit, urlunsplit
from datetime import UTC, datetime, timedelta

from cross_market_monitor.domain.models import FXQuote, MarketQuote, SourceConfig
from cross_market_monitor.infrastructure.http_client import HttpClient

PRICE_CANDIDATES = [8, 7, 6, 2, 3]
FX_RATE_CANDIDATES = [8, 1, 2]


def parse_sina_futures_payload(source_name: str, symbol: str, label: str, payload: str) -> MarketQuote:
    match = re.search(r'="([^"]*)"', payload)
    if not match:
        raise ValueError(f"Unexpected Sina payload for {symbol}")

    parts = match.group(1).split(",")
    if not parts or all(part == "" for part in parts):
        raise ValueError(f"Empty Sina payload for {symbol}")

    values = [float(parts[index]) for index in PRICE_CANDIDATES if index < len(parts) and _is_positive(parts[index])]
    if not values:
        raise ValueError(f"Could not parse latest price for {symbol}")

    bid = _float_or_none(parts, 6)
    ask = _float_or_none(parts, 7)
    last = values[0]

    trade_date = parts[17] if len(parts) > 17 and parts[17] else datetime.now(UTC).date().isoformat()
    hhmmss = parts[1] if len(parts) > 1 else ""
    timestamp = _parse_sina_datetime(trade_date, hhmmss)

    return MarketQuote(
        source_name=source_name,
        symbol=symbol,
        label=label,
        ts=timestamp,
        last=last,
        bid=bid,
        ask=ask,
        raw_payload=payload.strip(),
    )


def _is_positive(value: str) -> bool:
    try:
        return float(value) > 0
    except ValueError:
        return False


def _float_or_none(parts: list[str], index: int) -> float | None:
    if index >= len(parts):
        return None
    try:
        value = float(parts[index])
    except ValueError:
        return None
    return value if value > 0 else None


def _parse_sina_datetime(trade_date: str, hhmmss: str) -> datetime:
    cleaned = hhmmss.strip()
    if len(cleaned) == 8 and cleaned.count(":") == 2:
        return datetime.fromisoformat(f"{trade_date}T{cleaned}+08:00").astimezone(UTC)
    if len(cleaned) == 6 and cleaned.isdigit():
        return datetime.fromisoformat(
            f"{trade_date}T{cleaned[0:2]}:{cleaned[2:4]}:{cleaned[4:6]}+08:00"
        ).astimezone(UTC)
    return datetime.now(UTC)


class SinaFuturesAdapter:
    def __init__(self, source_name: str, source_config: SourceConfig, http_client: HttpClient) -> None:
        self.source_name = source_name
        self.source_config = source_config
        self.http_client = http_client

    def fetch_quote(self, symbol: str, label: str) -> MarketQuote:
        payload = _fetch_sina_text(
            self.http_client,
            self.source_config,
            f"{self.source_config.base_url}/list={symbol}",
        )
        return parse_sina_futures_payload(self.source_name, symbol, label, payload)

    def fetch_history(
        self,
        symbol: str,
        label: str,
        *,
        interval: str = "5m",
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
    ) -> list[MarketQuote]:
        history_symbol = _history_symbol(symbol)
        endpoint = _history_endpoint(interval)
        payload = self.http_client.get_text(
            f"{_history_base_url(self.source_config)}/futures/api/json.php/{endpoint}?symbol={history_symbol}",
            headers=self.source_config.headers,
        )
        return parse_sina_history_payload(
            self.source_name,
            symbol,
            label,
            payload,
            interval=interval,
            start_ts=start_ts,
            end_ts=end_ts,
        )


def parse_sina_fx_payload(source_name: str, pair: str, payload: str) -> FXQuote:
    match = re.search(r'="([^"]*)"', payload)
    if not match:
        raise ValueError(f"Unexpected Sina FX payload for {pair}")

    parts = match.group(1).split(",")
    if not parts or all(part == "" for part in parts):
        raise ValueError(f"Empty Sina FX payload for {pair}")

    rate = None
    for index in FX_RATE_CANDIDATES:
        candidate = _float_or_none(parts, index)
        if candidate is not None:
            rate = candidate
            break
    if rate is None:
        raise ValueError(f"Could not parse latest FX rate for {pair}")

    trade_date = next((part for part in reversed(parts) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", part.strip())), None)
    if trade_date is None:
        trade_date = datetime.now(UTC).date().isoformat()
    timestamp = _parse_sina_datetime(trade_date, parts[0] if parts else "")

    return FXQuote(
        source_name=source_name,
        pair=pair,
        ts=timestamp,
        rate=rate,
        raw_payload=payload.strip(),
    )


class SinaFxAdapter:
    def __init__(self, source_name: str, source_config: SourceConfig, http_client: HttpClient) -> None:
        self.source_name = source_name
        self.source_config = source_config
        self.http_client = http_client

    def fetch_rate(self, base: str, quote: str) -> FXQuote:
        pair = f"{base}/{quote}"
        symbol = self.source_config.params.get("symbol") or f"fx_s{base}{quote}".lower()
        payload = _fetch_sina_text(
            self.http_client,
            self.source_config,
            f"{self.source_config.base_url}/list={symbol}",
        )
        parsed = parse_sina_fx_payload(self.source_name, pair, payload)
        return parsed.model_copy(update={"ts": datetime.now(UTC)})


def _fetch_sina_text(http_client: HttpClient, source_config: SourceConfig, url: str) -> str:
    headers = _sina_request_headers(source_config)
    last_error: Exception | None = None
    for candidate_url in _sina_candidate_urls(url):
        try:
            return http_client.get_text(candidate_url, headers=headers)
        except Exception as exc:  # pragma: no cover - exercised via adapter fallback test
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Unable to fetch Sina payload from {url}")


def _sina_request_headers(source_config: SourceConfig) -> dict[str, str]:
    headers = {
        "Referer": "https://finance.sina.com.cn",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    headers.update(source_config.headers)
    return headers


def _sina_candidate_urls(url: str) -> list[str]:
    candidates = [url]
    parsed = urlsplit(url)
    if parsed.scheme == "https" and parsed.netloc == "hq.sinajs.cn":
        candidates.append(urlunsplit(("http", parsed.netloc, parsed.path, parsed.query, parsed.fragment)))
    return candidates


def parse_sina_history_payload(
    source_name: str,
    symbol: str,
    label: str,
    payload: str,
    *,
    interval: str,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> list[MarketQuote]:
    text = payload.strip()
    if not text:
        return []
    rows = json.loads(text)
    parsed_rows: list[tuple[datetime, float, list]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 5:
            continue
        timestamp = _parse_history_datetime(str(row[0]), interval)
        close = _coerce_float(row[4])
        if close is None or close <= 0:
            continue
        parsed_rows.append((timestamp, close, row))

    if parsed_rows and (datetime.now(UTC) - max(item[0] for item in parsed_rows)) > timedelta(days=30):
        latest_ts = max(item[0] for item in parsed_rows)
        raise ValueError(
            f"Sina history feed is stale for {symbol}: latest bar {latest_ts.isoformat()}"
        )

    quotes: list[MarketQuote] = []
    for timestamp, close, row in parsed_rows:
        if start_ts is not None and timestamp < start_ts:
            continue
        if end_ts is not None and timestamp > end_ts:
            continue
        quotes.append(
            MarketQuote(
                source_name=source_name,
                symbol=symbol,
                label=label,
                ts=timestamp,
                last=close,
                bid=None,
                ask=None,
                raw_payload=json.dumps(row, ensure_ascii=False),
            )
        )
    quotes.sort(key=lambda item: item.ts)
    return quotes


def _history_symbol(symbol: str) -> str:
    normalized = symbol.strip()
    if normalized.lower().startswith("nf_"):
        normalized = normalized[3:]
    return normalized.upper()


def _history_base_url(source_config: SourceConfig) -> str:
    return source_config.params.get("history_base_url") or "https://stock2.finance.sina.com.cn"


def _history_endpoint(interval: str) -> str:
    endpoints = {
        "5m": "IndexService.getInnerFuturesMiniKLine5m",
        "15m": "IndexService.getInnerFuturesMiniKLine15m",
        "30m": "IndexService.getInnerFuturesMiniKLine30m",
        "60m": "IndexService.getInnerFuturesMiniKLine60m",
        "1d": "IndexService.getInnerFuturesDailyKLine",
    }
    if interval not in endpoints:
        raise ValueError(f"Unsupported Sina history interval: {interval}")
    return endpoints[interval]


def _parse_history_datetime(value: str, interval: str) -> datetime:
    cleaned = value.strip()
    if interval == "1d" and len(cleaned) == 10:
        cleaned = f"{cleaned}T15:00:00+08:00"
    elif " " in cleaned:
        cleaned = cleaned.replace(" ", "T") + "+08:00"
    else:
        cleaned = f"{cleaned}T00:00:00+08:00"
    return datetime.fromisoformat(cleaned).astimezone(UTC)


def _coerce_float(value: object) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric
