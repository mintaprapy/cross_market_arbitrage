from __future__ import annotations

from fastapi import APIRouter, Query


def build_market_router(service) -> APIRouter:
    router = APIRouter()

    @router.get("/api/health")
    async def health() -> dict:
        return service.get_health()

    @router.get("/api/snapshot")
    async def snapshot(include_cards: bool = Query(default=False)) -> dict:
        return service.get_snapshot(include_cards=include_cards)

    @router.get("/api/snapshot-summary")
    async def snapshot_summary() -> dict:
        return service.get_snapshot_summary()

    @router.get("/api/history")
    async def history(
        group_name: str = Query(...),
        limit: int = Query(default=900, ge=1, le=5000),
        range_key: str | None = Query(default=None),
    ) -> list[dict]:
        return service.get_history(group_name, limit, range_key=range_key)

    @router.get("/api/card")
    async def card(group_name: str = Query(...), range_key: str | None = Query(default=None)) -> dict:
        return service.get_card_view(group_name, range_key=range_key)

    return router
