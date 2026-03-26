from __future__ import annotations

from cross_market_monitor.application.common import display_group_name
from cross_market_monitor.domain.models import PairConfig

UNIT_LABELS = {
    "CNY_PER_GRAM": "CNY/g",
    "CNY_PER_KG": "CNY/kg",
    "CNY_PER_TON": "CNY/ton",
    "CNY_PER_BARREL": "CNY/bbl",
    "USD_PER_OUNCE": "USD/oz",
    "USD_PER_POUND": "USD/lb",
    "USD_PER_BARREL": "USD/bbl",
    "USD_PER_TON": "USD/ton",
    "USD_PER_BUSHEL": "USD/bu",
}

FORMULA_SPECS = {
    "gold": {
        "display_name": "黄金",
        "normalized_unit_label": "USD/oz",
        "basis_note": "国内金价按克报价, 换算到国际金价的金衡盎司口径。",
    },
    "silver": {
        "display_name": "白银",
        "normalized_unit_label": "USD/oz",
        "basis_note": "国内白银按千克报价, 换算到国际白银的金衡盎司口径。",
    },
    "copper": {
        "display_name": "铜",
        "normalized_unit_label": "USD/lb",
        "basis_note": "国内铜价按吨报价, 换算到国际铜价的磅口径。",
    },
    "crude_oil": {
        "display_name": "原油",
        "normalized_unit_label": "USD/bbl",
        "basis_note": "国内原油按桶报价, 与海外原油桶口径直接比较。",
    },
    "cotton": {
        "display_name": "棉花",
        "normalized_unit_label": "USD/lb",
        "basis_note": "国内棉花按吨报价, 换算到国际棉花的磅口径。",
    },
    "sugar": {
        "display_name": "白糖",
        "normalized_unit_label": "USD/lb",
        "basis_note": "国内白糖按吨报价, 换算到国际白糖的磅口径。",
    },
    "aluminium": {
        "display_name": "铝",
        "normalized_unit_label": "USD/ton",
        "basis_note": "国内铝价按吨报价, 与海外铝吨口径直接比较。",
    },
    "soybean": {
        "display_name": "豆二",
        "normalized_unit_label": "USD/bu",
        "basis_note": "国内豆二按吨报价, 换算到国际大豆的蒲式耳口径。",
    },
}

HEDGE_UNIT_LABELS = {
    "USD_PER_OUNCE": "oz",
    "USD_PER_POUND": "lb",
    "USD_PER_BARREL": "bbl",
    "USD_PER_TON": "ton",
    "USD_PER_BUSHEL": "bu",
}


def build_commodity_spec(pair: PairConfig) -> dict:
    formula_spec = FORMULA_SPECS.get(pair.formula, {})
    return {
        "group_name": pair.group_name,
        "display_name": display_group_name(pair.group_name),
        "formula": pair.formula,
        "formula_display_name": formula_spec.get("display_name", pair.formula),
        "formula_version": pair.formula_version,
        "basis_note": formula_spec.get("basis_note"),
        "domestic_symbol": pair.domestic_symbol,
        "overseas_symbol": pair.overseas_symbol,
        "domestic_unit": pair.domestic_unit,
        "domestic_unit_label": UNIT_LABELS.get(pair.domestic_unit, pair.domestic_unit),
        "target_unit": pair.target_unit,
        "target_unit_label": UNIT_LABELS.get(pair.target_unit, pair.target_unit),
        "normalized_unit_label": formula_spec.get("normalized_unit_label", UNIT_LABELS.get(pair.target_unit, pair.target_unit)),
        "tax_mode": pair.tax_mode,
        "domestic_lot_size": pair.domestic_lot_size,
        "hedge_contract_size": pair.hedge_contract_size,
        "hedge_unit_label": HEDGE_UNIT_LABELS.get(pair.target_unit),
        "trading_sessions_local": list(pair.trading_sessions_local),
    }
