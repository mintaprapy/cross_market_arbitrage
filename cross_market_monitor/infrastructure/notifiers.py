from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from cross_market_monitor.application.common import display_group_name, format_local_display_timestamp
from cross_market_monitor.domain.models import AlertEvent, NotifierConfig
from cross_market_monitor.infrastructure.http_client import HttpClient

SEVERITY_RANK = {"info": 1, "warning": 2, "critical": 3}
SEVERITY_LABELS = {"info": "提示", "warning": "警告", "critical": "严重"}
CATEGORY_LABELS = {
    "data_quality": "数据质量",
    "fx": "汇率",
    "spread_level": "价差",
    "spread_pct": "价差百分比",
    "zscore": "Z-Score",
}


@dataclass(slots=True)
class NotifyResult:
    notifier_name: str
    success: bool
    response_message: str
    payload: dict


class BaseNotifier:
    def __init__(self, config: NotifierConfig, timezone_name: str = "Asia/Shanghai") -> None:
        self.config = config
        self.timezone_name = timezone_name
        self.timezone = ZoneInfo(timezone_name)

    def should_send(self, alert: AlertEvent) -> bool:
        if SEVERITY_RANK[alert.severity] < SEVERITY_RANK[self.config.min_severity]:
            return False
        if self.config.group_names and alert.group_name not in self.config.group_names:
            return False
        return True


class ConsoleNotifier(BaseNotifier):
    def send(self, alert: AlertEvent) -> NotifyResult:
        payload = alert_payload(alert, self.timezone)
        print(
            f"[ALERT][{alert.severity.upper()}][{alert.group_name}][{alert.category}] {alert.message}"
        )
        return NotifyResult(
            notifier_name=self.config.name,
            success=True,
            response_message="printed to console",
            payload=payload,
        )


class WebhookNotifier(BaseNotifier):
    def __init__(
        self,
        config: NotifierConfig,
        http_client: HttpClient,
        timezone_name: str = "Asia/Shanghai",
    ) -> None:
        super().__init__(config, timezone_name=timezone_name)
        self.http_client = http_client

    def send(self, alert: AlertEvent) -> NotifyResult:
        if not self.config.url:
            raise ValueError(f"Webhook notifier {self.config.name} is missing url")
        payload = alert_payload(alert, self.timezone)
        response = self.http_client.post_json(self.config.url, payload, headers=self.config.headers)
        return NotifyResult(
            notifier_name=self.config.name,
            success=True,
            response_message=response[:300] if response else "ok",
            payload=payload,
        )


class FeishuNotifier(WebhookNotifier):
    def send(self, alert: AlertEvent) -> NotifyResult:
        if not self.config.url:
            raise ValueError(f"Feishu notifier {self.config.name} is missing url")
        payload = {
            "msg_type": "text",
            "content": {
                "text": human_notification_text(alert, self.timezone)
            },
        }
        response = self.http_client.post_json(self.config.url, payload, headers=self.config.headers)
        return NotifyResult(
            notifier_name=self.config.name,
            success=True,
            response_message=response[:300] if response else "ok",
            payload=payload,
        )


class WecomNotifier(WebhookNotifier):
    def send(self, alert: AlertEvent) -> NotifyResult:
        if not self.config.url:
            raise ValueError(f"WeCom notifier {self.config.name} is missing url")
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": human_notification_text(alert, self.timezone).replace("\n", "\n> ")
            },
        }
        response = self.http_client.post_json(self.config.url, payload, headers=self.config.headers)
        return NotifyResult(
            notifier_name=self.config.name,
            success=True,
            response_message=response[:300] if response else "ok",
            payload=payload,
        )


class TelegramNotifier(BaseNotifier):
    def __init__(
        self,
        config: NotifierConfig,
        http_client: HttpClient,
        timezone_name: str = "Asia/Shanghai",
    ) -> None:
        super().__init__(config, timezone_name=timezone_name)
        self.http_client = http_client

    def send(self, alert: AlertEvent) -> NotifyResult:
        if not self.config.bot_token or not self.config.chat_id:
            raise ValueError(
                f"Telegram notifier {self.config.name} requires bot_token and chat_id"
            )
        payload = {
            "chat_id": self.config.chat_id,
            "text": human_notification_text(alert, self.timezone),
        }
        url = f"https://api.telegram.org/bot{self.config.bot_token}/sendMessage"
        response = self.http_client.post_json(url, payload, headers=self.config.headers)
        return NotifyResult(
            notifier_name=self.config.name,
            success=True,
            response_message=response[:300] if response else "ok",
            payload=payload,
        )


def _format_local_timestamp(ts: datetime, timezone: ZoneInfo) -> str:
    return ts.astimezone(timezone).isoformat()


def _format_human_timestamp(ts: datetime, timezone: ZoneInfo) -> str:
    return format_local_display_timestamp(ts, timezone)


def alert_payload(alert: AlertEvent, timezone: ZoneInfo) -> dict:
    title_group_name = display_group_name(alert.group_name) if alert.category == "data_quality" else alert.group_name
    return {
        "timestamp": _format_local_timestamp(alert.ts, timezone),
        "timestamp_local": _format_local_timestamp(alert.ts, timezone),
        "timestamp_utc": alert.ts.astimezone(UTC).isoformat(),
        "title": f"{title_group_name} {category_label(alert.category)} {severity_label(alert.severity)}",
        "group_name": alert.group_name,
        "category": alert.category,
        "severity": alert.severity,
        "message": alert.message,
        "metadata": alert.metadata,
    }


def human_notification_text(alert: AlertEvent, timezone: ZoneInfo | None = None) -> str:
    if alert.category in {"spread_level", "spread_pct"}:
        return alert.message
    timezone = timezone or ZoneInfo("Asia/Shanghai")
    group_name = display_group_name(alert.group_name) if alert.category == "data_quality" else alert.group_name
    return (
        f"[{severity_label(alert.severity)}] {group_name} {category_label(alert.category)}\n"
        f"{alert.message}\n"
        f"{_format_human_timestamp(alert.ts, timezone)}"
    )


def severity_label(severity: str) -> str:
    return SEVERITY_LABELS.get(severity, severity)


def category_label(category: str) -> str:
    return CATEGORY_LABELS.get(category, category)


def build_notifier(config: NotifierConfig, timezone_name: str = "Asia/Shanghai"):
    http_client = HttpClient(timeout_sec=config.timeout_sec)
    if config.kind == "console":
        return ConsoleNotifier(config, timezone_name=timezone_name)
    if config.kind == "webhook":
        return WebhookNotifier(config, http_client, timezone_name=timezone_name)
    if config.kind == "feishu":
        return FeishuNotifier(config, http_client, timezone_name=timezone_name)
    if config.kind == "telegram":
        return TelegramNotifier(config, http_client, timezone_name=timezone_name)
    if config.kind == "wecom":
        return WecomNotifier(config, http_client, timezone_name=timezone_name)
    raise ValueError(f"Unsupported notifier kind: {config.kind}")
