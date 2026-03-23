from __future__ import annotations

from dataclasses import dataclass

from cross_market_monitor.application.common import display_group_name
from cross_market_monitor.domain.models import AlertEvent, NotifierConfig
from cross_market_monitor.infrastructure.http_client import HttpClient

SEVERITY_RANK = {"info": 1, "warning": 2, "critical": 3}


@dataclass(slots=True)
class NotifyResult:
    notifier_name: str
    success: bool
    response_message: str
    payload: dict


class BaseNotifier:
    def __init__(self, config: NotifierConfig) -> None:
        self.config = config

    def should_send(self, alert: AlertEvent) -> bool:
        if SEVERITY_RANK[alert.severity] < SEVERITY_RANK[self.config.min_severity]:
            return False
        if self.config.group_names and alert.group_name not in self.config.group_names:
            return False
        return True


class ConsoleNotifier(BaseNotifier):
    def send(self, alert: AlertEvent) -> NotifyResult:
        payload = alert_payload(alert)
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
    def __init__(self, config: NotifierConfig, http_client: HttpClient) -> None:
        super().__init__(config)
        self.http_client = http_client

    def send(self, alert: AlertEvent) -> NotifyResult:
        if not self.config.url:
            raise ValueError(f"Webhook notifier {self.config.name} is missing url")
        payload = alert_payload(alert)
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
                "text": human_notification_text(alert)
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
                "content": human_notification_text(alert).replace("\n", "\n> ")
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
    def __init__(self, config: NotifierConfig, http_client: HttpClient) -> None:
        super().__init__(config)
        self.http_client = http_client

    def send(self, alert: AlertEvent) -> NotifyResult:
        if not self.config.bot_token or not self.config.chat_id:
            raise ValueError(
                f"Telegram notifier {self.config.name} requires bot_token and chat_id"
            )
        payload = {
            "chat_id": self.config.chat_id,
            "text": human_notification_text(alert),
        }
        url = f"https://api.telegram.org/bot{self.config.bot_token}/sendMessage"
        response = self.http_client.post_json(url, payload, headers=self.config.headers)
        return NotifyResult(
            notifier_name=self.config.name,
            success=True,
            response_message=response[:300] if response else "ok",
            payload=payload,
        )


def alert_payload(alert: AlertEvent) -> dict:
    title_group_name = display_group_name(alert.group_name) if alert.category == "data_quality" else alert.group_name
    return {
        "timestamp": alert.ts.isoformat(),
        "title": f"{title_group_name} {alert.category} {alert.severity}",
        "group_name": alert.group_name,
        "category": alert.category,
        "severity": alert.severity,
        "message": alert.message,
        "metadata": alert.metadata,
    }


def human_notification_text(alert: AlertEvent) -> str:
    if alert.category in {"spread_level", "spread_pct"}:
        return alert.message
    group_name = display_group_name(alert.group_name) if alert.category == "data_quality" else alert.group_name
    return (
        f"[{alert.severity.upper()}] {group_name} {alert.category}\n"
        f"{alert.message}\n"
        f"{alert.ts.isoformat()}"
    )


def build_notifier(config: NotifierConfig):
    http_client = HttpClient(timeout_sec=config.timeout_sec)
    if config.kind == "console":
        return ConsoleNotifier(config)
    if config.kind == "webhook":
        return WebhookNotifier(config, http_client)
    if config.kind == "feishu":
        return FeishuNotifier(config, http_client)
    if config.kind == "telegram":
        return TelegramNotifier(config, http_client)
    if config.kind == "wecom":
        return WecomNotifier(config, http_client)
    raise ValueError(f"Unsupported notifier kind: {config.kind}")
