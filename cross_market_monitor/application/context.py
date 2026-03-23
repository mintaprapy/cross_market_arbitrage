from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from cross_market_monitor.application.common import DEFAULT_HISTORY_RANGE_KEY, utc_now
from cross_market_monitor.application.replay import ReplayAnalyzer
from cross_market_monitor.domain.models import FXQuote, MonitorConfig, PairConfig, SourceHealth, SpreadSnapshot
from cross_market_monitor.domain.stats import RollingWindow
from cross_market_monitor.infrastructure.repository import SQLiteRepository


@dataclass(slots=True)
class ServiceContext:
    config: MonitorConfig
    repository: SQLiteRepository
    adapters: dict[str, Any]
    notifiers: list[Any]
    windows: dict[str, RollingWindow]
    fx_window: RollingWindow
    source_health: dict[str, SourceHealth]
    replay: ReplayAnalyzer
    local_tz: Any
    pair_map: dict[str, PairConfig]
    enabled_pairs: list[PairConfig]
    preferred_domestic_symbols: dict[str, str]
    preferred_overseas_symbols: dict[str, str]
    started_at: datetime = field(default_factory=utc_now)
    last_poll_started_at: datetime | None = None
    last_poll_finished_at: datetime | None = None
    is_polling: bool = False
    total_cycles: int = 0
    latest_fx_quote: FXQuote | None = None
    latest_fx_jump_pct: float | None = None
    latest_fx_is_live: bool = False
    latest_fx_last_live_at: datetime | None = None
    latest_fx_frozen_since: datetime | None = None
    latest_snapshots: dict[str, SpreadSnapshot] = field(default_factory=dict)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    startup_completed: bool = False
    startup_task: asyncio.Task | None = None
    shadow_stop_event: threading.Event = field(default_factory=threading.Event)
    shadow_thread: threading.Thread | None = None
    cooldowns: dict[tuple[str, str], datetime] = field(default_factory=dict)
    issue_started_at: dict[tuple[str, str], datetime] = field(default_factory=dict)
    default_history_range_key: str = DEFAULT_HISTORY_RANGE_KEY
    history_preview_limit: int = 240
    history_card_limit: int = 900
    history_backfill_attempts: dict[tuple[str, str, str, str, str, str], datetime] = field(default_factory=dict)
    retention_last_run_at: datetime | None = None
