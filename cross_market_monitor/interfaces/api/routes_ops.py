from __future__ import annotations

from fastapi import APIRouter, Query


def build_ops_router(service) -> APIRouter:
    router = APIRouter()

    @router.get("/api/alerts")
    async def alerts(limit: int = Query(default=100, ge=1, le=500)) -> list[dict]:
        return service.get_alerts(limit)

    @router.get("/api/notification-deliveries")
    async def notification_deliveries(limit: int = Query(default=100, ge=1, le=500)) -> list[dict]:
        return service.get_notification_deliveries(limit)

    @router.get("/api/job-runs")
    async def job_runs() -> list[dict]:
        return service.get_job_runs()

    @router.get("/api/replay/summary")
    async def replay_summary(
        group_name: str = Query(...),
        limit: int = Query(default=1000, ge=1, le=10000),
        start_ts: str | None = Query(default=None),
        end_ts: str | None = Query(default=None),
    ) -> dict:
        return service.replay_summary(group_name, limit=limit, start_ts=start_ts, end_ts=end_ts)

    return router
