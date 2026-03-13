from __future__ import annotations

from collections import deque
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
    def __init__(self, size: int, seed: list[float] | None = None) -> None:
        self._values: deque[float] = deque(maxlen=size)
        if seed:
            for value in seed[-size:]:
                self.append(value)

    def append(self, value: float | None) -> None:
        if value is None:
            return
        self._values.append(value)

    def values(self) -> list[float]:
        return list(self._values)

    def last(self) -> float | None:
        if not self._values:
            return None
        return self._values[-1]

    def summary(self, current: float | None) -> tuple[float | None, float | None, float | None, float | None]:
        if current is None:
            return None, None, None, None

        previous = self.last()
        window_values = self.values()
        avg = mean(window_values)
        sd = std_dev(window_values)
        zscore = None
        if avg is not None and sd not in (None, 0):
            zscore = (current - avg) / sd

        delta = None
        if previous is not None:
            delta = current - previous

        return avg, sd, zscore, delta
