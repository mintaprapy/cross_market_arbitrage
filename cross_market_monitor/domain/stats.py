from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta
from math import sqrt


def mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def std_dev(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    avg = mean(values)
    if avg is None:
        return None
    variance = sum((value - avg) ** 2 for value in values) / len(values)
    return sqrt(variance)


class RollingWindow:
    def __init__(
        self,
        size: int | None,
        seed: list[float] | None = None,
        *,
        max_age: timedelta | None = None,
        bucket_size: timedelta | None = None,
        seed_points: list[tuple[datetime, float]] | None = None,
    ) -> None:
        self._max_age = max_age
        self._bucket_size = bucket_size
        self._values: deque[tuple[datetime | None, float]] = deque(maxlen=size)
        if seed_points:
            self.replace(seed_points)
            return
        if seed:
            values = seed[-size:] if size is not None else seed
            for value in values:
                self.append(value)

    def append(self, value: float | None, *, ts: datetime | None = None) -> None:
        if value is None:
            return
        normalized_ts = self._bucket_ts(ts)
        if normalized_ts is not None and self._values:
            last_ts, _ = self._values[-1]
            if last_ts == normalized_ts:
                self._values[-1] = (normalized_ts, value)
                self._prune(normalized_ts)
                return
        self._values.append((normalized_ts, value))
        self._prune(normalized_ts)

    def replace(self, points: list[tuple[datetime, float]]) -> None:
        self._values.clear()
        for ts, value in points:
            self.append(value, ts=ts)

    def values(self, *, as_of: datetime | None = None) -> list[float]:
        self._prune(as_of)
        return [value for _, value in self._values]

    def last(self, *, as_of: datetime | None = None) -> float | None:
        self._prune(as_of)
        if not self._values:
            return None
        return self._values[-1][1]

    def summary(
        self,
        current: float | None,
        *,
        current_ts: datetime | None = None,
    ) -> tuple[float | None, float | None, float | None, float | None]:
        if current is None:
            return None, None, None, None

        previous = self.last(as_of=current_ts)
        window_values = self.values(as_of=current_ts)
        avg = mean(window_values)
        sd = std_dev(window_values)
        zscore = None
        if avg is not None and sd not in (None, 0):
            zscore = (current - avg) / sd

        delta = None
        if previous is not None:
            delta = current - previous

        return avg, sd, zscore, delta

    def _prune(self, reference_ts: datetime | None) -> None:
        if self._max_age is None or reference_ts is None:
            return
        cutoff = reference_ts - self._max_age
        while self._values:
            item_ts, _ = self._values[0]
            if item_ts is None or item_ts >= cutoff:
                break
            self._values.popleft()

    def _bucket_ts(self, ts: datetime | None) -> datetime | None:
        if ts is None or self._bucket_size is None:
            return ts
        normalized = ts.astimezone(UTC)
        bucket_seconds = max(int(self._bucket_size.total_seconds()), 1)
        bucket_start = int(normalized.timestamp()) // bucket_seconds * bucket_seconds
        return datetime.fromtimestamp(bucket_start, tz=UTC)
