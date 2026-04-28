from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from cross_market_monitor.application.common import data_quality_group_name
from cross_market_monitor.application.common import is_within_trading_sessions
from cross_market_monitor.application.common import display_group_name
from cross_market_monitor.application.common import display_source_name
from cross_market_monitor.application.common import age_seconds
from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.application.history.history_service import HistoryService
from cross_market_monitor.domain.models import AlertEvent, NotificationDelivery, PairConfig, SpreadSnapshot

LOGGER = logging.getLogger("cross_market_monitor")


class AlertService:
    def __init__(self, context: ServiceContext, history: HistoryService) -> None:
        self.context = context
        self.history = history

    def evaluate_alerts(self, pair: PairConfig, snapshot: SpreadSnapshot) -> list[AlertEvent]:
        alerts: list[AlertEvent | None] = []
        now = snapshot.ts

        should_emit_data_quality_alert = self.should_emit_data_quality_alert(pair, snapshot)
        if snapshot.status in {"partial", "stale", "error"} and should_emit_data_quality_alert:
            issue_detail = self.data_quality_issue_detail(pair, snapshot)
            alerts.append(
                self.make_alert(
                    now,
                    pair.group_name,
                    "data_quality",
                    "critical" if snapshot.status == "error" else "warning",
                    (
                        f"{display_group_name(pair.group_name)} 数据状态异常：{self.status_text(snapshot.status)}\n"
                        f"异常明细：{issue_detail}"
                    ),
                    {
                        "errors": snapshot.errors,
                        "issue_detail": issue_detail,
                        "domestic_source": snapshot.domestic_source or pair.domestic_source,
                        "overseas_source": snapshot.overseas_source or pair.overseas_source,
                        "fx_source": snapshot.fx_source or self.context.config.app.fx_source,
                        "domestic_age_sec": snapshot.domestic_age_sec,
                        "overseas_age_sec": snapshot.overseas_age_sec,
                        "fx_age_sec": snapshot.fx_age_sec,
                        "max_skew_sec": snapshot.max_skew_sec,
                    },
                )
            )

        if snapshot.status == "paused":
            alerts.append(
                self.make_alert(
                    now,
                    pair.group_name,
                    "fx",
                    "warning",
                    snapshot.pause_reason or f"{pair.group_name} signals paused because of FX jump",
                    {
                        "fx_jump_pct": snapshot.fx_jump_pct,
                        "fx_rate": snapshot.fx_rate,
                    },
                )
            )

        live_fx_age_sec = self.live_fx_age_sec(snapshot)
        if live_fx_age_sec is not None and live_fx_age_sec > self.context.config.app.fx_max_age_sec:
            alerts.append(
                self.make_alert(
                    now,
                    "FX",
                    "fx",
                    "warning",
                    f"USD/CNY 汇率数据已过期（{live_fx_age_sec / 3600:.1f} 小时未更新）",
                    {
                        "fx_age_sec": live_fx_age_sec,
                        "effective_fx_age_sec": snapshot.fx_age_sec,
                        "fx_rate": self.context.latest_fx_quote.rate if self.context.latest_fx_quote is not None else snapshot.fx_rate,
                        "fx_source": self.context.latest_fx_quote.source_name if self.context.latest_fx_quote is not None else snapshot.fx_source,
                    },
                )
            )

        if snapshot.fx_rate is None and self.context.latest_fx_quote is None:
            alerts.append(
                self.make_alert(
                    now,
                    "FX",
                    "fx",
                    "critical",
                    "USD/CNY 汇率不可用",
                    {
                        "fx_source": snapshot.fx_source,
                    },
                )
            )

        if snapshot.status == "ok":
            if (
                pair.thresholds.spread_alert_above is not None
                and snapshot.spread is not None
                and snapshot.spread >= pair.thresholds.spread_alert_above
            ):
                alerts.append(
                    self.make_alert(
                        now,
                        pair.group_name,
                        "spread_level",
                        "warning",
                        self.format_spread_notification_message(pair, snapshot),
                        {
                            "spread": snapshot.spread,
                            "threshold": pair.thresholds.spread_alert_above,
                            "trigger_direction": "above",
                            "normalized_last": snapshot.normalized_last,
                            "overseas_last": snapshot.overseas_last,
                        },
                    )
                )

            if (
                pair.thresholds.spread_alert_below is not None
                and snapshot.spread is not None
                and snapshot.spread <= pair.thresholds.spread_alert_below
            ):
                alerts.append(
                    self.make_alert(
                        now,
                        pair.group_name,
                        "spread_level",
                        "warning",
                        self.format_spread_notification_message(pair, snapshot),
                        {
                            "spread": snapshot.spread,
                            "threshold": pair.thresholds.spread_alert_below,
                            "trigger_direction": "below",
                            "normalized_last": snapshot.normalized_last,
                            "overseas_last": snapshot.overseas_last,
                        },
                    )
                )

            alerts.extend(self.evaluate_spread_pct_alerts(now, pair, snapshot))
            alerts.extend(self.evaluate_zscore_alerts(now, pair, snapshot))

            shadow_comparison = self.history.get_shadow_comparison(pair.group_name, limit=30)
            if shadow_comparison is not None:
                latest_shadow_spread_pct = shadow_comparison.get("latest_spread_pct")
                if latest_shadow_spread_pct is not None and abs(latest_shadow_spread_pct) >= 0.005:
                    alerts.append(
                        self.make_alert(
                            now,
                            pair.group_name,
                            "data_quality",
                            "warning",
                            f"{display_group_name(pair.group_name)} 主链路与 TqSdk 影子链路偏差达到 {latest_shadow_spread_pct:.2%}",
                            {
                                "main_symbol": shadow_comparison.get("main_symbol"),
                                "shadow_symbol": shadow_comparison.get("shadow_symbol"),
                                "latest_spread_pct": latest_shadow_spread_pct,
                                "latest_main_last": shadow_comparison.get("latest_main_last"),
                                "latest_shadow_last": shadow_comparison.get("latest_shadow_last"),
                            },
                        )
                    )

        return [alert for alert in alerts if alert is not None]

    def data_quality_issue_detail(self, pair: PairConfig, snapshot: SpreadSnapshot) -> str:
        domestic_source = display_source_name(
            self.issue_source_name(snapshot, "domestic", snapshot.domestic_source or pair.domestic_source)
        )
        overseas_source = display_source_name(
            self.issue_source_name(snapshot, "overseas", snapshot.overseas_source or pair.overseas_source)
        )
        fx_source = display_source_name(snapshot.fx_source or self.context.config.app.fx_source)
        details: list[str] = []

        if snapshot.domestic_last_raw is None:
            details.append(f"国内{domestic_source}缺失")
        elif self.is_age_stale(snapshot.domestic_age_sec, pair.thresholds.stale_seconds):
            details.append(f"国内{domestic_source}过期 {self.format_age(snapshot.domestic_age_sec)}")

        if snapshot.overseas_last is None:
            details.append(f"海外{overseas_source}缺失")
        elif self.is_age_stale(snapshot.overseas_age_sec, pair.thresholds.stale_seconds):
            details.append(f"海外{overseas_source}过期 {self.format_age(snapshot.overseas_age_sec)}")

        if snapshot.fx_rate is None:
            details.append(f"汇率{fx_source}缺失")
        elif self.is_age_stale(snapshot.fx_age_sec, self.context.config.app.fx_max_age_sec):
            details.append(f"汇率{fx_source}过期 {self.format_age(snapshot.fx_age_sec)}")

        if snapshot.max_skew_sec is not None and snapshot.max_skew_sec > pair.thresholds.max_skew_seconds:
            details.append(f"国内/海外/汇率时间差过大 {self.format_age(snapshot.max_skew_sec)}")

        if not details:
            details.extend(self.translate_quality_errors(pair, snapshot))

        return "；".join(details) if details else "见数据源健康"

    def translate_quality_errors(self, pair: PairConfig, snapshot: SpreadSnapshot) -> list[str]:
        domestic_source = display_source_name(
            self.issue_source_name(snapshot, "domestic", snapshot.domestic_source or pair.domestic_source)
        )
        overseas_source = display_source_name(
            self.issue_source_name(snapshot, "overseas", snapshot.overseas_source or pair.overseas_source)
        )
        fx_source = display_source_name(snapshot.fx_source or self.context.config.app.fx_source)
        translated: list[str] = []
        for error in snapshot.errors:
            lowered = error.lower()
            if "domestic" in lowered:
                translated.append(f"国内{domestic_source}异常")
            elif "overseas" in lowered:
                translated.append(f"海外{overseas_source}异常")
            elif lowered.startswith("fx:") or "fx" in lowered:
                translated.append(f"汇率{fx_source}异常")
            elif "stale" in lowered:
                translated.append("报价过期")
            elif "skew" in lowered or "timestamps" in lowered:
                translated.append("时间差过大")
        return translated

    @staticmethod
    def issue_source_name(snapshot: SpreadSnapshot, leg_type: str, fallback: str | None) -> str | None:
        prefix = f"{leg_type}:"
        for error in snapshot.errors:
            if not error.startswith(prefix):
                continue
            parts = error.split(":", 2)
            if len(parts) >= 2 and parts[1].strip():
                return parts[1].strip()
        return fallback

    @staticmethod
    def is_age_stale(age_sec: float | None, threshold_sec: float) -> bool:
        return age_sec is not None and age_sec > threshold_sec

    @staticmethod
    def format_age(age_sec: float | None) -> str:
        if age_sec is None:
            return "--"
        if age_sec < 60:
            return f"{age_sec:.0f}s"
        if age_sec < 3600:
            return f"{age_sec / 60:.1f}min"
        return f"{age_sec / 3600:.1f}h"

    def evaluate_spread_pct_alerts(
        self,
        now: datetime,
        pair: PairConfig,
        snapshot: SpreadSnapshot,
    ) -> list[AlertEvent]:
        if snapshot.spread_pct is None:
            return []

        alerts: list[AlertEvent] = []
        upper = pair.thresholds.spread_pct_alert_above
        lower = pair.thresholds.spread_pct_alert_below
        legacy_abs = pair.thresholds.spread_pct_abs

        if upper is not None and snapshot.spread_pct >= upper:
            alerts.append(
                self.make_alert(
                    now,
                    pair.group_name,
                    "spread_pct",
                    "warning",
                    self.format_spread_notification_message(pair, snapshot),
                    {
                        "spread_pct": snapshot.spread_pct,
                        "spread": snapshot.spread,
                        "threshold": upper,
                        "trigger_direction": "above",
                    },
                )
            )
        if lower is not None and snapshot.spread_pct <= lower:
            alerts.append(
                self.make_alert(
                    now,
                    pair.group_name,
                    "spread_pct",
                    "warning",
                    self.format_spread_notification_message(pair, snapshot),
                    {
                        "spread_pct": snapshot.spread_pct,
                        "spread": snapshot.spread,
                        "threshold": lower,
                        "trigger_direction": "below",
                    },
                )
            )
        if upper is None and lower is None and legacy_abs is not None and abs(snapshot.spread_pct) >= legacy_abs:
            alerts.append(
                self.make_alert(
                    now,
                    pair.group_name,
                    "spread_pct",
                    "warning",
                    self.format_spread_notification_message(pair, snapshot),
                    {
                        "spread_pct": snapshot.spread_pct,
                        "spread": snapshot.spread,
                        "threshold": legacy_abs,
                    },
                )
            )
        return [alert for alert in alerts if alert is not None]

    def format_spread_notification_message(self, pair: PairConfig, snapshot: SpreadSnapshot) -> str:
        name_text = display_group_name(pair.group_name)
        spread_pct_text = self.format_percentage(snapshot.spread_pct)
        spread_text = self.format_fixed(snapshot.spread, decimals=2)
        domestic_text = self.format_domestic_price(snapshot.domestic_last_raw)
        normalized_text = self.format_fixed(snapshot.normalized_last, decimals=2)
        overseas_text = self.format_fixed(snapshot.overseas_last, decimals=2)
        return (
            f"{name_text}：{spread_pct_text}  |  {spread_text}\n"
            f"中 {domestic_text} | 换 {normalized_text} | 外 {overseas_text}"
        )

    def format_percentage(self, value: float | None) -> str:
        if value is None:
            return "--"
        return f"{value:.2%}"

    def format_fixed(self, value: float | None, *, decimals: int) -> str:
        if value is None:
            return "--"
        return f"{value:,.{decimals}f}"

    def format_domestic_price(self, value: float | None) -> str:
        if value is None:
            return "--"
        if abs(value - round(value)) < 1e-9:
            return f"{round(value):,}"
        return self.format_fixed(value, decimals=2)

    def evaluate_zscore_alerts(
        self,
        now: datetime,
        pair: PairConfig,
        snapshot: SpreadSnapshot,
    ) -> list[AlertEvent]:
        if snapshot.zscore is None:
            return []

        alerts: list[AlertEvent] = []
        upper = pair.thresholds.zscore_alert_above
        lower = pair.thresholds.zscore_alert_below
        legacy_abs = pair.thresholds.zscore_abs

        if upper is not None and snapshot.zscore >= upper:
            alerts.append(
                self.make_alert(
                    now,
                    pair.group_name,
                    "zscore",
                    "warning",
                    f"{display_group_name(pair.group_name)} 的 Z-Score 达到 {snapshot.zscore:.2f}，高于阈值 {upper:.2f}",
                    {"zscore": snapshot.zscore, "spread": snapshot.spread, "threshold": upper},
                )
            )
        if lower is not None and snapshot.zscore <= lower:
            alerts.append(
                self.make_alert(
                    now,
                    pair.group_name,
                    "zscore",
                    "warning",
                    f"{display_group_name(pair.group_name)} 的 Z-Score 达到 {snapshot.zscore:.2f}，低于阈值 {lower:.2f}",
                    {"zscore": snapshot.zscore, "spread": snapshot.spread, "threshold": lower},
                )
            )
        if upper is None and lower is None and legacy_abs is not None and abs(snapshot.zscore) >= legacy_abs:
            alerts.append(
                self.make_alert(
                    now,
                    pair.group_name,
                    "zscore",
                    "warning",
                    f"{display_group_name(pair.group_name)} 的 Z-Score 达到 {snapshot.zscore:.2f}",
                    {"zscore": snapshot.zscore, "spread": snapshot.spread, "threshold": legacy_abs},
                )
        )
        return [alert for alert in alerts if alert is not None]

    def status_text(self, status: str) -> str:
        mapping = {
            "ok": "正常",
            "partial": "部分缺失",
            "stale": "已过期",
            "error": "错误",
            "paused": "已暂停",
        }
        return mapping.get(status, status)

    def should_emit_data_quality_alert(self, pair: PairConfig, snapshot: SpreadSnapshot) -> bool:
        if snapshot.status == "error":
            return True
        if snapshot.status not in {"partial", "stale"}:
            self.clear_issue_started_at(pair.group_name, "data_quality")
            return True
        if self.is_fx_only_issue(snapshot):
            self.clear_issue_started_at(pair.group_name, "data_quality")
            return False
        if not pair.trading_sessions_local:
            return self.issue_has_reached_delay(pair.group_name, "data_quality", snapshot.ts, pair.thresholds.data_quality_alert_delay_sec)
        local_dt = snapshot.ts.astimezone(self.context.local_tz)
        if not is_within_trading_sessions(
            local_dt,
            pair.trading_sessions_local,
            grace_sec=pair.thresholds.stale_alert_grace_sec,
            non_trading_dates=self.context.config.app.domestic_non_trading_dates_local,
            weekends_closed=self.context.config.app.domestic_weekends_closed,
        ):
            self.clear_issue_started_at(pair.group_name, "data_quality")
            return False
        return self.issue_has_reached_delay(
            pair.group_name,
            "data_quality",
            snapshot.ts,
            pair.thresholds.data_quality_alert_delay_sec,
        )

    def is_fx_only_issue(self, snapshot: SpreadSnapshot) -> bool:
        non_fx_errors = [error for error in snapshot.errors if not error.startswith("fx:")]
        if snapshot.fx_rate is None:
            return not non_fx_errors
        if snapshot.fx_age_sec is not None and snapshot.fx_age_sec > self.context.config.app.fx_max_age_sec:
            return not non_fx_errors
        return False

    def live_fx_age_sec(self, snapshot: SpreadSnapshot) -> float | None:
        if self.context.latest_fx_last_live_at is not None:
            return max((snapshot.ts - self.context.latest_fx_last_live_at).total_seconds(), 0.0)
        if self.context.latest_fx_quote is not None:
            return max((snapshot.ts - self.context.latest_fx_quote.ts).total_seconds(), 0.0)
        if snapshot.fx_age_sec is not None:
            return snapshot.fx_age_sec
        return None

    def issue_has_reached_delay(
        self,
        group_name: str,
        category: str,
        now: datetime,
        delay_sec: int,
    ) -> bool:
        key = (self.issue_group_key(group_name, category), category)
        started_at = self.context.issue_started_at.get(key)
        if started_at is None:
            self.context.issue_started_at[key] = now
            return delay_sec <= 0
        return (now - started_at).total_seconds() >= delay_sec

    def clear_issue_started_at(self, group_name: str, category: str) -> None:
        key = (self.issue_group_key(group_name, category), category)
        self.context.issue_started_at.pop(key, None)

    def issue_group_key(self, group_name: str, category: str) -> str:
        if category == "data_quality":
            return data_quality_group_name(group_name)
        return group_name

    def make_alert(
        self,
        ts: datetime,
        group_name: str,
        category: str,
        severity: str,
        message: str,
        metadata: dict,
    ) -> AlertEvent | None:
        key = (self.issue_group_key(group_name, category), category)
        previous = self.context.cooldowns.get(key)
        cooldown = self.cooldown_seconds_for_alert(group_name, category)
        if previous and (ts - previous).total_seconds() < cooldown:
            return None

        self.context.cooldowns[key] = ts
        return AlertEvent(
            ts=ts,
            group_name=group_name,
            category=category,  # type: ignore[arg-type]
            severity=severity,  # type: ignore[arg-type]
            message=message,
            metadata=metadata,
        )

    def cooldown_seconds_for_alert(self, group_name: str, category: str) -> int:
        if category == "fx":
            return int(self.context.config.app.fx_alert_cooldown_seconds)

        matching_pair = next(
            (
                pair
                for pair in self.context.config.pairs
                if pair.group_name == group_name or data_quality_group_name(pair.group_name) == group_name
            ),
            None,
        )
        if matching_pair is None:
            return 300
        if category == "data_quality" and matching_pair.thresholds.data_quality_alert_cooldown_seconds is not None:
            return int(matching_pair.thresholds.data_quality_alert_cooldown_seconds)
        return int(matching_pair.thresholds.alert_cooldown_seconds)

    async def dispatch_alerts(self, alerts: list[AlertEvent]) -> None:
        if not alerts or not self.context.notifiers:
            return

        deliveries = await asyncio.gather(*(self.deliver_alert(alert) for alert in alerts))
        for delivery_batch in deliveries:
            for delivery in delivery_batch:
                self.context.repository.insert_notification_delivery(
                    delivery,
                    timezone_name=self.context.config.app.timezone,
                )

    async def deliver_alert(self, alert: AlertEvent) -> list[NotificationDelivery]:
        deliveries: list[NotificationDelivery] = []
        for notifier in self.context.notifiers:
            if not notifier.should_send(alert):
                continue
            try:
                result = await asyncio.to_thread(notifier.send, alert)
                deliveries.append(
                    NotificationDelivery(
                        ts=alert.ts,
                        notifier_name=result.notifier_name,
                        group_name=alert.group_name,
                        category=alert.category,
                        severity=alert.severity,
                        success=result.success,
                        response_message=result.response_message,
                        payload=result.payload,
                    )
                )
            except Exception as exc:
                deliveries.append(
                    NotificationDelivery(
                        ts=alert.ts,
                        notifier_name=getattr(getattr(notifier, "config", None), "name", "unknown"),
                        group_name=alert.group_name,
                        category=alert.category,
                        severity=alert.severity,
                        success=False,
                        response_message=str(exc),
                        payload={
                            "group_name": alert.group_name,
                            "category": alert.category,
                            "severity": alert.severity,
                            "message": alert.message,
                        },
                    )
                )
                LOGGER.warning("Notifier delivery failed: %s", exc)
        return deliveries
