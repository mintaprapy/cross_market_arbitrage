from __future__ import annotations

from fastapi import APIRouter, Query

from cross_market_monitor.interfaces.api.payloads import BackfillPayload, RoutePreferencePayload


def build_control_router(service) -> APIRouter:
    router = APIRouter()

    @router.get("/api/domestic-routes")
    async def domestic_routes(group_name: str = Query(...)) -> dict:
        return service.get_domestic_route_options(group_name)

    @router.post("/api/domestic-routes/select")
    async def select_domestic_route(payload: RoutePreferencePayload, group_name: str = Query(...)) -> dict:
        return service.set_domestic_route_preference(group_name, payload.symbol)

    @router.get("/api/overseas-routes")
    async def overseas_routes(group_name: str = Query(...)) -> dict:
        return service.get_overseas_route_options(group_name)

    @router.post("/api/overseas-routes/select")
    async def select_overseas_route(payload: RoutePreferencePayload, group_name: str = Query(...)) -> dict:
        return service.set_overseas_route_preference(group_name, payload.symbol)

    @router.post("/api/backfill/domestic")
    async def backfill_domestic(payload: BackfillPayload, group_name: str = Query(...)) -> dict:
        return service.backfill_domestic_history(
            group_name,
            interval=payload.interval or "5m",
            range_key=payload.range_key,
            start_ts=payload.start_ts,
            end_ts=payload.end_ts,
        )

    @router.post("/api/backfill/overseas")
    async def backfill_overseas(payload: BackfillPayload, group_name: str = Query(...)) -> dict:
        return service.backfill_overseas_history(
            group_name,
            interval=payload.interval or "60m",
            range_key=payload.range_key,
            start_ts=payload.start_ts,
            end_ts=payload.end_ts,
        )

    return router
