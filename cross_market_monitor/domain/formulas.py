from __future__ import annotations

from dataclasses import dataclass

from cross_market_monitor.domain.models import PairConfig

TROY_OUNCE_IN_GRAMS = 31.1034768
POUNDS_PER_METRIC_TON = 2204.62262
VAT_RATE = 1.13


def _apply_tax_mode(value: float, tax_mode: str) -> float:
    if tax_mode == "net":
        return value / VAT_RATE
    return value


def normalize_domestic_price(raw_price: float | None, pair: PairConfig, usd_cny: float | None) -> float | None:
    if raw_price is None or usd_cny is None or usd_cny <= 0:
        return None

    if pair.formula == "gold":
        return raw_price * TROY_OUNCE_IN_GRAMS / usd_cny

    if pair.formula == "silver":
        taxable = _apply_tax_mode(raw_price / 1000.0, pair.tax_mode)
        return taxable * TROY_OUNCE_IN_GRAMS / usd_cny

    if pair.formula == "copper":
        taxable = _apply_tax_mode(raw_price, pair.tax_mode)
        return taxable / usd_cny / POUNDS_PER_METRIC_TON

    if pair.formula == "crude_oil":
        return raw_price / usd_cny

    raise ValueError(f"Unsupported formula: {pair.formula}")


@dataclass(slots=True)
class ComparableQuote:
    last: float | None
    bid: float | None
    ask: float | None


def normalize_domestic_quote(
    pair: PairConfig,
    usd_cny: float | None,
    last: float | None,
    bid: float | None,
    ask: float | None,
) -> ComparableQuote:
    return ComparableQuote(
        last=normalize_domestic_price(last, pair, usd_cny),
        bid=normalize_domestic_price(bid, pair, usd_cny),
        ask=normalize_domestic_price(ask, pair, usd_cny),
    )


def compute_spread(overseas_price: float | None, normalized_domestic_price: float | None) -> tuple[float | None, float | None]:
    if overseas_price is None or normalized_domestic_price is None or normalized_domestic_price == 0:
        return None, None
    spread = overseas_price - normalized_domestic_price
    return spread, spread / normalized_domestic_price


def compute_executable_spreads(
    domestic_bid: float | None,
    domestic_ask: float | None,
    overseas_bid: float | None,
    overseas_ask: float | None,
) -> tuple[float | None, float | None]:
    buy_domestic_sell_overseas = None
    buy_overseas_sell_domestic = None

    if domestic_ask is not None and overseas_bid is not None:
        buy_domestic_sell_overseas = overseas_bid - domestic_ask

    if domestic_bid is not None and overseas_ask is not None:
        buy_overseas_sell_domestic = overseas_ask - domestic_bid

    return buy_domestic_sell_overseas, buy_overseas_sell_domestic
