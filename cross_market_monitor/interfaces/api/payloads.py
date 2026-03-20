from __future__ import annotations

from pydantic import BaseModel


class RoutePreferencePayload(BaseModel):
    symbol: str | None = None


class BackfillPayload(BaseModel):
    interval: str | None = None
    range_key: str | None = None
    start_ts: str | None = None
    end_ts: str | None = None
