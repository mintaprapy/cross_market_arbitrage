from __future__ import annotations

from pathlib import Path

import yaml

from cross_market_monitor.domain.models import MonitorConfig


def load_config(path: str | Path) -> MonitorConfig:
    config_path = Path(path).resolve()
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw = _merge_trading_calendar(raw, config_path)
    config = MonitorConfig.model_validate(raw)

    if config.app.domestic_trading_calendar_path:
        calendar_path = Path(config.app.domestic_trading_calendar_path)
        if not calendar_path.is_absolute():
            calendar_path = (config_path.parent / calendar_path).resolve()
            app_config = config.app.model_copy(update={"domestic_trading_calendar_path": str(calendar_path)})
            config = config.model_copy(update={"app": app_config})

    sqlite_path = Path(config.app.sqlite_path)
    if not sqlite_path.is_absolute():
        sqlite_path = (config_path.parent.parent / sqlite_path).resolve()
        app_config = config.app.model_copy(update={"sqlite_path": str(sqlite_path)})
        config = config.model_copy(update={"app": app_config})

    export_dir = Path(config.app.export_dir)
    if not export_dir.is_absolute():
        export_dir = (config_path.parent.parent / export_dir).resolve()
        app_config = config.app.model_copy(update={"export_dir": str(export_dir)})
        config = config.model_copy(update={"app": app_config})

    return config


def _merge_trading_calendar(raw: dict | None, config_path: Path) -> dict:
    payload = dict(raw or {})
    app = dict(payload.get("app") or {})
    calendar_ref = app.get("domestic_trading_calendar_path")
    if not calendar_ref:
        payload["app"] = app
        return payload

    calendar_path = Path(calendar_ref)
    if not calendar_path.is_absolute():
        calendar_path = (config_path.parent / calendar_path).resolve()
    calendar_raw = yaml.safe_load(calendar_path.read_text(encoding="utf-8")) or {}
    domestic_calendar = dict(calendar_raw.get("domestic") or calendar_raw)

    if "weekends_closed" in domestic_calendar and "domestic_weekends_closed" not in app:
        app["domestic_weekends_closed"] = domestic_calendar["weekends_closed"]

    existing_dates = list(app.get("domestic_non_trading_dates_local") or [])
    merged_dates = existing_dates + list(domestic_calendar.get("non_trading_dates_local") or [])
    if merged_dates:
        deduped: list[object] = []
        seen: set[str] = set()
        for item in merged_dates:
            key = item.isoformat() if hasattr(item, "isoformat") else str(item)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        app["domestic_non_trading_dates_local"] = deduped

    payload["app"] = app
    return payload
