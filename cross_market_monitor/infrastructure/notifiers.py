from __future__ import annotations

from dataclasses import dataclass

from cross_market_monitor.domain.models import AlertEvent, NotifierConfig
from cross_market_monitor.infrastructure.http_client import HttpClient

SEVERITY_RANK = {"info": 1, "warning": 2, "critical": 3}


@dataclass(slots=True)
class NotifyResult:
    notifier_name: str
    success: bool
    response_message: str
    payload: dict


class ConsoleNotifier:
    def __init__(self, config: NotifierConfig) -> None:
        self.config = config

    def should_send(self, severity: str) -> bool:
        return SEVERITY_RANK[severity] >= SEVERITY_RANK[self.config.min_severity]

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


class WebhookNotifier:
    def __init__(self, config: NotifierConfig, http_client: HttpClient) -> None:
        self.config = config
        self.http_client = http_client

    def should_send(self, severity: str) -> bool:
        return SEVERITY_RANK[severity] >= SEVERITY_RANK[self.config.min_severity]

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


def alert_payload(alert: AlertEvent) -> dict:
    return {
        "timestamp": alert.ts.isoformat(),
        "title": f"{alert.group_name} {alert.category} {alert.severity}",
        "group_name": alert.group_name,
        "category": alert.category,
        "severity": alert.severity,
        "message": alert.message,
        "metadata": alert.metadata,
    }


def build_notifier(config: NotifierConfig) -> ConsoleNotifier | WebhookNotifier:
    if config.kind == "console":
        return ConsoleNotifier(config)
    if config.kind == "webhook":
        return WebhookNotifier(config, HttpClient(timeout_sec=config.timeout_sec))
    raise ValueError(f"Unsupported notifier kind: {config.kind}")
