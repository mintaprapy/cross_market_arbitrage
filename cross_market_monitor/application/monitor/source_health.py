from __future__ import annotations

from cross_market_monitor.application.common import utc_now
from cross_market_monitor.application.context import ServiceContext


class SourceHealthRecorder:
    def __init__(self, context: ServiceContext) -> None:
        self.context = context

    def record_success(self, source_name: str, symbol: str, latency_ms: float) -> None:
        health = self.context.source_health[source_name]
        now = utc_now()
        health.success_count += 1
        health.last_success_at = now
        health.last_symbol = symbol
        health.last_latency_ms = latency_ms
        health.last_error = None
        health.updated_at = now
        self.context.repository.upsert_source_health(health)

    def record_failure(self, source_name: str, symbol: str, latency_ms: float, error: str) -> None:
        health = self.context.source_health[source_name]
        now = utc_now()
        health.failure_count += 1
        health.last_failure_at = now
        health.last_symbol = symbol
        health.last_latency_ms = latency_ms
        health.last_error = error
        health.updated_at = now
        self.context.repository.upsert_source_health(health)
