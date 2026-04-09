from __future__ import annotations

from datetime import datetime
from math import sqrt
from statistics import mean, median, pstdev

from cross_market_monitor.domain.models import PairConfig, ReplayHighlight, ReplayReport, ReplaySignalEvent
from cross_market_monitor.infrastructure.repository import SQLiteRepository


class ReplayAnalyzer:
    def __init__(
        self,
        repository: SQLiteRepository,
        pairs: list[PairConfig],
        *,
        target_daily_vol_pct: float = 0.015,
        bucket_minutes: int = 15,
    ) -> None:
        self.repository = repository
        self.pairs = {pair.group_name: pair for pair in pairs}
        self.target_daily_vol_pct = target_daily_vol_pct
        self.bucket_minutes = max(int(bucket_minutes), 1)

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
        rows = self._load_bucketed_rows(
            group_name,
            limit=limit,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        if not rows:
            return ReplayReport(group_name=group_name, sample_count=0).model_dump(mode="json")

        statuses = {"ok": 0, "partial": 0, "stale": 0, "error": 0, "paused": 0}
        for row in rows:
            statuses[row["status"]] = statuses.get(row["status"], 0) + 1

        spreads = [row["spread"] for row in rows if row["spread"] is not None]
        spread_pcts = [row["spread_pct"] for row in rows if row["spread_pct"] is not None]
        zscores = [row["zscore"] for row in rows if row["zscore"] is not None]
        domestic_prices = [row["normalized_last"] for row in rows if row["normalized_last"] is not None]
        overseas_prices = [row["overseas_last"] for row in rows if row["overseas_last"] is not None]

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
        hedge_ratio, hedge_intercept = _ols_beta_intercept(domestic_prices, overseas_prices)
        realized_daily_vol_pct = _realized_daily_vol_pct(spreads, domestic_prices)
        recommended_position_scale = None
        if realized_daily_vol_pct not in (None, 0):
            recommended_position_scale = self.target_daily_vol_pct / realized_daily_vol_pct

        round_trip_costs = [
            _round_trip_cost(pair, row["normalized_last"], row["overseas_last"])
            for row in rows
            if row["normalized_last"] is not None and row["overseas_last"] is not None
        ]
        net_edges = [
            abs(row["spread"]) - cost
            for row, cost in zip(
                [row for row in rows if row["spread"] is not None and row["normalized_last"] is not None and row["overseas_last"] is not None],
                round_trip_costs,
            )
        ]
        profitable_after_cost_count = sum(1 for edge in net_edges if edge > 0)

        highlights = self._top_highlights(rows, limit=highlight_limit)
        signals = self._signal_entries(rows, pair, signal_limit=signal_limit)
        signals.extend(self._cost_signals(rows, pair, signal_limit=signal_limit))
        signals = signals[-signal_limit:]

        report = ReplayReport(
            group_name=group_name,
            sample_count=len(rows),
            ok_count=statuses.get("ok", 0),
            partial_count=statuses.get("partial", 0),
            stale_count=statuses.get("stale", 0) + statuses.get("paused", 0),
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
            spread_pct_std=_safe_std(spread_pcts),
            spread_pct_median=median(spread_pcts) if spread_pcts else None,
            spread_pct_min=min(spread_pcts) if spread_pcts else None,
            spread_pct_max=max(spread_pcts) if spread_pcts else None,
            latest_spread_pct_percentile=_percentile_rank(spread_pcts, rows[-1]["spread_pct"]),
            max_abs_zscore=max((abs(value) for value in zscores), default=None),
            spread_pct_breach_count=sum(
                1 for value in spread_pcts if _value_breaches_thresholds(
                    value,
                    above=pair.thresholds.spread_pct_alert_above,
                    below=pair.thresholds.spread_pct_alert_below,
                    legacy_abs=pair.thresholds.spread_pct_abs,
                )
            ),
            zscore_breach_count=sum(
                1 for value in zscores if _value_breaches_thresholds(
                    value,
                    above=pair.thresholds.zscore_alert_above,
                    below=pair.thresholds.zscore_alert_below,
                    legacy_abs=pair.thresholds.zscore_abs,
                )
            ),
            convergence_count=convergence_count,
            divergence_count=divergence_count,
            flat_count=flat_count,
            convergence_ratio=(convergence_count / denominator) if denominator else None,
            divergence_ratio=(divergence_count / denominator) if denominator else None,
            hedge_ratio_ols=hedge_ratio,
            hedge_intercept=hedge_intercept,
            realized_daily_vol_pct=realized_daily_vol_pct,
            recommended_position_scale=recommended_position_scale,
            average_round_trip_cost=_safe_mean(round_trip_costs),
            average_net_edge_after_cost=_safe_mean(net_edges),
            profitable_after_cost_count=profitable_after_cost_count,
            top_highlights=highlights,
            signal_entries=signals,
        )
        return report.model_dump(mode="json")

    def _load_bucketed_rows(
        self,
        group_name: str,
        *,
        limit: int,
        start_ts: str | None,
        end_ts: str | None,
    ) -> list[dict]:
        if self.bucket_minutes <= 1:
            return self.repository.fetch_snapshots(
                group_name=group_name,
                limit=limit,
                start_ts=start_ts,
                end_ts=end_ts,
            )

        if start_ts is not None or end_ts is not None or limit <= 0:
            rows = self.repository.fetch_snapshots(
                group_name=group_name,
                limit=None,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            bucketed_rows = _bucket_rows(rows, self.bucket_minutes)
            return bucketed_rows[-limit:] if limit > 0 else bucketed_rows

        batch_size = max(limit * 4, 1000)
        offset = 0
        raw_rows_desc: list[dict] = []
        bucket_keys: set[datetime] = set()
        while len(bucket_keys) < limit:
            batch = self.repository.fetch_snapshots(
                group_name=group_name,
                limit=batch_size,
                offset=offset,
                descending=True,
            )
            if not batch:
                break
            raw_rows_desc.extend(batch)
            for row in batch:
                bucket_key = _bucket_key(row, self.bucket_minutes)
                if bucket_key is not None:
                    bucket_keys.add(bucket_key)
            offset += len(batch)
            if len(batch) < batch_size:
                break

        rows = list(reversed(raw_rows_desc))
        bucketed_rows = _bucket_rows(rows, self.bucket_minutes)
        return bucketed_rows[-limit:]

    def _top_highlights(self, rows: list[dict], limit: int) -> list[ReplayHighlight]:
        scored_rows: list[tuple[float, ReplayHighlight]] = []
        for row in rows:
            if row["zscore"] is not None:
                scored_rows.append(
                    (
                        abs(row["zscore"]),
                        ReplayHighlight(
                            ts=_parse_iso(row["ts"]),
                            metric="zscore",
                            score=abs(row["zscore"]),
                            spread=row["spread"],
                            spread_pct=row["spread_pct"],
                            zscore=row["zscore"],
                            status=row["status"],
                        ),
                    )
                )
            elif row["spread_pct"] is not None:
                scored_rows.append(
                    (
                        abs(row["spread_pct"]),
                        ReplayHighlight(
                            ts=_parse_iso(row["ts"]),
                            metric="spread_pct",
                            score=abs(row["spread_pct"]),
                            spread=row["spread"],
                            spread_pct=row["spread_pct"],
                            zscore=row["zscore"],
                            status=row["status"],
                        ),
                    )
                )
            elif row["spread"] is not None:
                scored_rows.append(
                    (
                        abs(row["spread"]),
                        ReplayHighlight(
                            ts=_parse_iso(row["ts"]),
                            metric="spread_abs",
                            score=abs(row["spread"]),
                            spread=row["spread"],
                            spread_pct=row["spread_pct"],
                            zscore=row["zscore"],
                            status=row["status"],
                        ),
                    )
                )
        scored_rows.sort(key=lambda item: item[0], reverse=True)
        return [item[1] for item in scored_rows[:limit]]

    def _signal_entries(self, rows: list[dict], pair: PairConfig, signal_limit: int) -> list[ReplaySignalEvent]:
        entries: list[ReplaySignalEvent] = []
        active_spread = False
        active_zscore = False
        for row in rows:
            if row["spread_pct"] is not None:
                threshold_value, direction, in_breach = _threshold_match(
                    row["spread_pct"],
                    above=pair.thresholds.spread_pct_alert_above,
                    below=pair.thresholds.spread_pct_alert_below,
                    legacy_abs=pair.thresholds.spread_pct_abs,
                )
                if in_breach and not active_spread:
                    entries.append(
                        ReplaySignalEvent(
                            ts=_parse_iso(row["ts"]),
                            group_name=pair.group_name,
                            trigger="spread_pct",
                            value=row["spread_pct"],
                            threshold=threshold_value if threshold_value is not None else 0.0,
                            direction=direction,
                        )
                    )
                active_spread = in_breach
            if row["zscore"] is not None:
                threshold_value, direction, in_breach = _threshold_match(
                    row["zscore"],
                    above=pair.thresholds.zscore_alert_above,
                    below=pair.thresholds.zscore_alert_below,
                    legacy_abs=pair.thresholds.zscore_abs,
                )
                if in_breach and not active_zscore:
                    entries.append(
                        ReplaySignalEvent(
                            ts=_parse_iso(row["ts"]),
                            group_name=pair.group_name,
                            trigger="zscore",
                            value=row["zscore"],
                            threshold=threshold_value if threshold_value is not None else 0.0,
                            direction=direction,
                        )
                    )
                active_zscore = in_breach
        return entries[-signal_limit:]

    def _cost_signals(self, rows: list[dict], pair: PairConfig, signal_limit: int) -> list[ReplaySignalEvent]:
        entries: list[ReplaySignalEvent] = []
        active = False
        for row in rows:
            if row["spread"] is None or row["normalized_last"] is None or row["overseas_last"] is None:
                continue
            cost = _round_trip_cost(pair, row["normalized_last"], row["overseas_last"])
            edge = abs(row["spread"]) - cost
            in_breach = edge <= 0
            if in_breach and not active:
                entries.append(
                    ReplaySignalEvent(
                        ts=_parse_iso(row["ts"]),
                        group_name=pair.group_name,
                        trigger="cost_edge",
                        value=edge,
                        threshold=0.0,
                        direction="negative" if edge < 0 else "positive",
                    )
                )
            active = in_breach
        return entries[-signal_limit:]


def _safe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return mean(values)


def _safe_std(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    return pstdev(values)


def _percentile_rank(values: list[float], current: float | None) -> float | None:
    if not values or current is None:
        return None
    less_than = sum(1 for value in values if value < current)
    equal_to = sum(1 for value in values if value == current)
    return (less_than + 0.5 * equal_to) / len(values)


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _bucket_rows(rows: list[dict], bucket_minutes: int) -> list[dict]:
    if bucket_minutes <= 1 or len(rows) < 2:
        return rows
    by_bucket: dict[datetime, dict] = {}
    for row in rows:
        bucket_start = _bucket_key(row, bucket_minutes)
        if bucket_start is None:
            continue
        by_bucket[bucket_start] = row
    return [by_bucket[key] for key in sorted(by_bucket)]


def _bucket_key(row: dict, bucket_minutes: int) -> datetime | None:
    ts_value = row.get("ts_local") or row.get("ts")
    if not ts_value:
        return None
    dt = _parse_iso(ts_value)
    return dt.replace(
        minute=(dt.minute // bucket_minutes) * bucket_minutes,
        second=0,
        microsecond=0,
    )


def _value_breaches_thresholds(
    value: float,
    *,
    above: float | None,
    below: float | None,
    legacy_abs: float | None,
) -> bool:
    if above is not None and value >= above:
        return True
    if below is not None and value <= below:
        return True
    if above is None and below is None and legacy_abs is not None and abs(value) >= legacy_abs:
        return True
    return False


def _threshold_match(
    value: float,
    *,
    above: float | None,
    below: float | None,
    legacy_abs: float | None,
) -> tuple[float | None, str, bool]:
    if above is not None and value >= above:
        return above, "positive", True
    if below is not None and value <= below:
        return below, "negative", True
    if above is None and below is None and legacy_abs is not None and abs(value) >= legacy_abs:
        return legacy_abs, "positive" if value > 0 else "negative", True
    return None, "positive" if value > 0 else "negative", False


def _ols_beta_intercept(x_values: list[float], y_values: list[float]) -> tuple[float | None, float | None]:
    if len(x_values) < 2 or len(y_values) < 2 or len(x_values) != len(y_values):
        return None, None
    x_mean = mean(x_values)
    y_mean = mean(y_values)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values))
    denominator = sum((x - x_mean) ** 2 for x in x_values)
    if denominator == 0:
        return None, None
    beta = numerator / denominator
    intercept = y_mean - beta * x_mean
    return beta, intercept


def _realized_daily_vol_pct(spreads: list[float], domestic_prices: list[float]) -> float | None:
    if len(spreads) < 2 or len(domestic_prices) < 2:
        return None
    spread_std = pstdev(spreads)
    baseline = mean(abs(price) for price in domestic_prices if price is not None)
    if baseline == 0:
        return None
    return (spread_std / baseline) * sqrt(24)


def _round_trip_cost(pair: PairConfig, domestic_price: float, overseas_price: float) -> float:
    domestic_bps = pair.costs.domestic_fee_bps + pair.costs.domestic_slippage_bps
    overseas_bps = pair.costs.overseas_fee_bps + pair.costs.overseas_slippage_bps
    funding_bps = pair.costs.funding_bps_per_day * (pair.costs.holding_hours / 24.0)
    domestic_cost = abs(domestic_price) * domestic_bps / 10_000
    overseas_cost = abs(overseas_price) * (overseas_bps + funding_bps) / 10_000
    return domestic_cost + overseas_cost
