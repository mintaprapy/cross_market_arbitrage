from __future__ import annotations

from pathlib import Path

import yaml

from cross_market_monitor.domain.models import MonitorConfig


def load_config(path: str | Path) -> MonitorConfig:
    config_path = Path(path).resolve()
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config = MonitorConfig.model_validate(raw)

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
