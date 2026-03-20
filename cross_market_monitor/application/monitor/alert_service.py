from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from cross_market_monitor.application.common import is_within_trading_sessions
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

        should_emit_stale_alert = snapshot.status != "stale" or self.should_emit_stale_alert(pair, snapshot)
        if snapshot.status in {"partial", "stale", "error"} and should_emit_stale_alert:
            alerts.append(
                self.make_alert(
                    now,
                    pair.group_name,
                    "data_quality",
                    "critical" if snapshot.status == "error" else "warning",
                    f"{pair.group_name} data status is {snapshot.status}",
                    {
                        "errors": snapshot.errors,
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

        if snapshot.fx_rate is None:
            alerts.append(
                self.make_alert(
                    now,
                    pair.group_name,
                    "fx",
                    "critical",
                    f"{pair.group_name} FX rate is unavailable",
                    {},
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
                        (
                            f"{pair.group_name} spread reached {snapshot.spread:.4f}, "
                            f"above threshold {pair.thresholds.spread_alert_above:.4f}"
                        ),
                        {
                            "spread": snapshot.spread,
                            "threshold": pair.thresholds.spread_alert_above,
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
                        (
                            f"{pair.group_name} spread reached {snapshot.spread:.4f}, "
                            f"below threshold {pair.thresholds.spread_alert_below:.4f}"
                        ),
                        {
                            "spread": snapshot.spread,
                            "threshold": pair.thresholds.spread_alert_below,
                            "normalized_last": snapshot.normalized_last,
                            "overseas_last": snapshot.overseas_last,
                        },
                    )
                )

            if snapshot.spread_pct is not None and abs(snapshot.spread_pct) >= pair.thresholds.spread_pct_abs:
                alerts.append(
                    self.make_alert(
                        now,
                        pair.group_name,
                        "spread_pct",
                        "warning",
                        f"{pair.group_name} spread_pct reached {snapshot.spread_pct:.2%}",
                        {"spread_pct": snapshot.spread_pct, "spread": snapshot.spread},
                    )
                )

            if snapshot.zscore is not None and abs(snapshot.zscore) >= pair.thresholds.zscore_abs:
                alerts.append(
                    self.make_alert(
                        now,
                        pair.group_name,
                        "zscore",
                        "warning",
                        f"{pair.group_name} zscore reached {snapshot.zscore:.2f}",
                        {"zscore": snapshot.zscore, "spread": snapshot.spread},
                    )
                )

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
                            f"{pair.group_name} main vs TqSdk shadow diverged by {latest_shadow_spread_pct:.2%}",
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

    def should_emit_stale_alert(self, pair: PairConfig, snapshot: SpreadSnapshot) -> bool:
        if snapshot.status != "stale":
            return True
        if not pair.trading_sessions_local:
            return True
        local_dt = snapshot.ts.astimezone(self.context.local_tz)
        return is_within_trading_sessions(
            local_dt,
            pair.trading_sessions_local,
            grace_sec=pair.thresholds.stale_alert_grace_sec,
        )

    def make_alert(
        self,
        ts: datetime,
        group_name: str,
        category: str,
        severity: str,
        message: str,
        metadata: dict,
    ) -> AlertEvent | None:
        key = (group_name, category)
        previous = self.context.cooldowns.get(key)
        cooldown = next(
            (
                pair.thresholds.alert_cooldown_seconds
                for pair in self.context.config.pairs
                if pair.group_name == group_name
            ),
            300,
        )
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
