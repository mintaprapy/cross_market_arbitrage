from __future__ import annotations

from cross_market_monitor.domain.models import SourceConfig

SOURCE_CAPABILITIES = {
    "sina_futures": {
        "supports_quote": True,
        "supports_history": True,
        "supports_fx": False,
        "history_mode": "direct",
        "notes": "新浪国内期货, 支持实时行情和历史K线。",
    },
    "sina_fx": {
        "supports_quote": False,
        "supports_history": False,
        "supports_fx": True,
        "history_mode": "none",
        "notes": "新浪外汇, 仅实时汇率。",
    },
    "shfe_delaymarket": {
        "supports_quote": True,
        "supports_history": False,
        "supports_fx": False,
        "history_mode": "none",
        "notes": "上期所/能源中心延时行情。",
    },
    "tqsdk_main": {
        "supports_quote": True,
        "supports_history": True,
        "supports_fx": False,
        "history_mode": "direct",
        "notes": "TqSdk 主连, 支持实时与历史。",
    },
    "okx_swap": {
        "supports_quote": True,
        "supports_history": True,
        "supports_fx": False,
        "history_mode": "paged_backward",
        "notes": "OKX 永续, 历史按分页回补。",
    },
    "binance_futures": {
        "supports_quote": True,
        "supports_history": True,
        "supports_fx": False,
        "history_mode": "paged_forward",
        "notes": "Binance 永续, 历史按分页回补。",
    },
    "gate_futures": {
        "supports_quote": True,
        "supports_history": True,
        "supports_fx": False,
        "history_mode": "paged_forward",
        "notes": "Gate 永续, 历史按分页回补。",
    },
    "gate_tradfi": {
        "supports_quote": True,
        "supports_history": True,
        "supports_fx": False,
        "history_mode": "direct_coarsened",
        "history_limit": 500,
        "notes": "Gate TradFi, 单次最多500根K线, 超窗自动升粗粒度。",
    },
    "hyperliquid": {
        "supports_quote": True,
        "supports_history": False,
        "supports_fx": False,
        "history_mode": "none",
        "notes": "Hyperliquid 实时盘口/标记价。",
    },
    "cme_reference": {
        "supports_quote": True,
        "supports_history": False,
        "supports_fx": False,
        "history_mode": "none",
        "notes": "CME 参考价, 不作为主链路历史源。",
    },
    "frankfurter_fx": {
        "supports_quote": False,
        "supports_history": True,
        "supports_fx": True,
        "history_mode": "direct",
        "notes": "Frankfurter 汇率, 支持实时与历史汇率。",
    },
    "open_er_api_fx": {
        "supports_quote": False,
        "supports_history": True,
        "supports_fx": True,
        "history_mode": "direct",
        "notes": "Open ER 汇率, 作为汇率备源。",
    },
    "mock_quote": {
        "supports_quote": True,
        "supports_history": False,
        "supports_fx": False,
        "history_mode": "none",
        "notes": "测试用 mock 行情源。",
    },
    "mock_fx": {
        "supports_quote": False,
        "supports_history": False,
        "supports_fx": True,
        "history_mode": "none",
        "notes": "测试用 mock 汇率源。",
    },
}


def capability_for_kind(kind: str) -> dict:
    capability = SOURCE_CAPABILITIES.get(kind, {})
    return {
        "supports_quote": bool(capability.get("supports_quote")),
        "supports_history": bool(capability.get("supports_history")),
        "supports_fx": bool(capability.get("supports_fx")),
        "history_mode": capability.get("history_mode", "unknown"),
        "history_limit": capability.get("history_limit"),
        "notes": capability.get("notes"),
    }


def capability_for_source(source_name: str, source_config: SourceConfig) -> dict:
    capability = capability_for_kind(source_config.kind)
    return {
        "source_name": source_name,
        "kind": source_config.kind,
        "base_url": source_config.base_url,
        **capability,
    }
