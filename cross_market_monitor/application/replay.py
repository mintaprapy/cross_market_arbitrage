from __future__ import annotations

from datetime import datetime
from statistics import mean, pstdev

from cross_market_monitor.domain.models import PairConfig, ReplayHighlight, ReplayReport, ReplaySignalEvent
from cross_market_monitor.infrastructure.repository import SQLiteRepository


class ReplayAnalyzer:
    def __init__(self, repository: SQLiteRepository, pairs: list[PairConfig]) -> None:
        self.repository = repository
        self.pairs = {pair.group_name: pair for pair in pairs}

    def analyze(
        self,
        group_name: str,
        *,
        limit: int = 1000,
        start_ts: str | None = None,
        end_ts: str | None = None,
        highlight_limit: int = 5,
        signal_limit: int = 20,
    ) -> dict:
        pair = self.pairs[group_name]
        rows = self.repository.fetch_snapshots(
            group_name=group_name,
            limit=limit,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        if not rows:
            return ReplayReport(group_name=group_name, sample_count=0).model_dump(mode="json")

        statuses = {"ok": 0, "partial": 0, "stale": 0, "error": 0}
        for row in rows:
            statuses[row["status"]] = statuses.get(row["status"], 0) + 1

        spreads = [row["spread"] for row in rows if row["spread"] is not None]
        spread_pcts = [row["spread_pct"] for row in rows if row["spread_pct"] is not None]
        zscores = [row["zscore"] for row in rows if row["zscore"] is not None]

        convergence_count = divergence_count = flat_count = 0
        previous_spread: float | None = None
        for row in rows:
            current = row["spread"]
            if current is None or previous_spread is None:
                previous_spread = current
                continue
            current_abs = abs(current)
            previous_abs = abs(previous_spread)
            if current_abs < previous_abs:
                convergence_count += 1
            elif current_abs > previous_abs:
                divergence_count += 1
            else:
                flat_count += 1
            previous_spread = current

        denominator = convergence_count + divergence_count + flat_count
        highlights = self._top_highlights(rows, limit=highlight_limit)
        signals = self._signal_entries(rows, pair, signal_limit=signal_limit)

        report = ReplayReport(
            group_name=group_name,
            sample_count=len(rows),
            ok_count=statuses.get("ok", 0),
            partial_count=statuses.get("partial", 0),
            stale_count=statuses.get("stale", 0),
            error_count=statuses.get("error", 0),
            start_ts=_parse_iso(rows[0]["ts"]),
            end_ts=_parse_iso(rows[-1]["ts"]),
            latest_spread=rows[-1]["spread"],
            latest_spread_pct=rows[-1]["spread_pct"],
            latest_zscore=rows[-1]["zscore"],
            spread_mean=_safe_mean(spreads),
            spread_std=_safe_std(spreads),
            spread_min=min(spreads) if spreads else None,
            spread_max=max(spreads) if spreads else None,
            spread_pct_mean=_safe_mean(spread_pcts),
            spread_pct_min=min(spread_pcts) if spread_pcts else None,
            spread_pct_max=max(spread_pcts) if spread_pcts else None,
            max_abs_zscore=max((abs(value) for value in zscores), default=None),
            spread_pct_breach_count=sum(
                1 for value in spread_pcts if abs(value) >= pair.thresholds.spread_pct_abs
            ),
            zscore_breach_count=sum(
                1 for value in zscores if abs(value) >= pair.thresholds.zscore_abs
            ),
            convergence_count=convergence_count,
            divergence_count=divergence_count,
            flat_count=flat_count,
            convergence_ratio=(convergence_count / denominator) if denominator else None,
            divergence_ratio=(divergence_count / denominator) if denominator else None,
            top_highlights=highlights,
            signal_entries=signals,
        )
        return report.model_dump(mode="json")

    def _top_highlights(self, rows: list[dict], limit: int) -> list[ReplayHighlight]:
        scored_rows: list[tuple[float, ReplayHighlight]] = []
        for row in rows:
            if row["zscore"] is not None:
                highlight = ReplayHighlight(
                    ts=_parse_iso(row["ts"]),
                    metric="zscore",
                    score=abs(row["zscore"]),
                    spread=row["spread"],
                    spread_pct=row["spread_pct"],
                    zscore=row["zscore"],
                    status=row["status"],
                )
                scored_rows.append((highlight.score, highlight))
                continue
            if row["spread_pct"] is not None:
                highlight = ReplayHighlight(
                    ts=_parse_iso(row["ts"]),
                    metric="spread_pct",
                    score=abs(row["spread_pct"]),
                    spread=row["spread"],
                    spread_pct=row["spread_pct"],
                    zscore=row["zscore"],
                    status=row["status"],
                )
                scored_rows.append((highlight.score, highlight))
                continue
            if row["spread"] is not None:
                highlight = ReplayHighlight(
                    ts=_parse_iso(row["ts"]),
                    metric="spread_abs",
                    score=abs(row["spread"]),
                    spread=row["spread"],
                    spread_pct=row["spread_pct"],
                    zscore=row["zscore"],
                    status=row["status"],
                )
                scored_rows.append((highlight.score, highlight))

        scored_rows.sort(key=lambda item: item[0], reverse=True)
        return [item[1] for item in scored_rows[:limit]]

    def _signal_entries(self, rows: list[dict], pair: PairConfig, signal_limit: int) -> list[ReplaySignalEvent]:
        entries: list[ReplaySignalEvent] = []
        active_spread = False
        active_zscore = False
        for row in rows:
            if row["spread_pct"] is not None:
                in_breach = abs(row["spread_pct"]) >= pair.thresholds.spread_pct_abs
                if in_breach and not active_spread:
                    entries.append(
                        ReplaySignalEvent(
                            ts=_parse_iso(row["ts"]),
                            group_name=pair.group_name,
                            trigger="spread_pct",
                            value=row["spread_pct"],
                            threshold=pair.thresholds.spread_pct_abs,
                            direction="positive" if row["spread_pct"] > 0 else "negative",
                        )
                    )
                active_spread = in_breach
            if row["zscore"] is not None:
                in_breach = abs(row["zscore"]) >= pair.thresholds.zscore_abs
                if in_breach and not active_zscore:
                    entries.append(
                        ReplaySignalEvent(
                            ts=_parse_iso(row["ts"]),
                            group_name=pair.group_name,
                            trigger="zscore",
                            value=row["zscore"],
                            threshold=pair.thresholds.zscore_abs,
                            direction="positive" if row["zscore"] > 0 else "negative",
                        )
                    )
                active_zscore = in_breach
        return entries[-signal_limit:]


def _safe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return mean(values)


def _safe_std(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    return pstdev(values)


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)
