from __future__ import annotations

import asyncio
import math
import threading
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from time import perf_counter

from cross_market_monitor.application.common import (
    HISTORY_RANGE_CONFIG,
    OVERSEAS_HISTORY_INTERVAL_BY_RANGE,
    default_overseas_symbol,
    infer_product_code,
    utc_now,
)
from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.application.control.route_preference_service import RoutePreferenceService
from cross_market_monitor.application.monitor.source_health import SourceHealthRecorder
from cross_market_monitor.domain.formulas import compute_spread, normalize_domestic_price
from cross_market_monitor.domain.models import MarketQuote, PairConfig, QuoteRouteConfig
from cross_market_monitor.infrastructure.marketdata.tqsdk import (
    TqSdkMainAdapter,
    TqSdkShadowRunner,
    tqsdk_main_symbol_for_product,
)


class HistoryService:
    def __init__(
        self,
        context: ServiceContext,
        route_preferences: RoutePreferenceService,
        health: SourceHealthRecorder,
    ) -> None:
        self.context = context
        self.route_preferences = route_preferences
        self.health = health

    def get_history(
        self,
        group_name: str,
        limit: int = 300,
        *,
        range_key: str | None = None,
        ensure_local_history: bool = False,
    ) -> list[dict]:
        pair = self.context.pair_map[group_name]
        domestic_symbol = self.context.preferred_domestic_symbols.get(group_name, pair.domestic_symbol)
        overseas_symbol = self.context.preferred_overseas_symbols.get(group_name, default_overseas_symbol(pair))
        normalized_range_key = self.normalize_history_range_key(range_key)
        start_ts = self.history_window_start(normalized_range_key)
        if ensure_local_history:
            self.ensure_overseas_history(
                pair,
                range_key=normalized_range_key,
                start_ts=start_ts,
                end_ts=None,
            )
        history = self.build_chart_history(
            pair,
            domestic_symbol,
            overseas_symbol,
            start_ts=start_ts,
        )
        snapshot_history = self.filter_snapshot_history(
            self.context.repository.fetch_history(group_name, None, start_ts=start_ts),
            domestic_symbol=domestic_symbol,
            overseas_symbol=overseas_symbol,
        )
        merged_history = self.merge_history_rows(history, snapshot_history)
        if merged_history:
            return self.downsample_history_rows(
                merged_history,
                self.history_target_points(normalized_range_key, limit),
            )
        return self.downsample_history_rows(
            snapshot_history,
            self.history_target_points(normalized_range_key, limit),
        )

    def normalize_history_range_key(self, range_key: str | None) -> str:
        if range_key in HISTORY_RANGE_CONFIG:
            return str(range_key)
        return self.context.default_history_range_key

    def history_window_start(self, range_key: str) -> str | None:
        window = HISTORY_RANGE_CONFIG.get(range_key, HISTORY_RANGE_CONFIG[self.context.default_history_range_key])["duration"]
        if window is None or not isinstance(window, timedelta):
            return None
        return (utc_now() - window).isoformat()

    def resolve_history_window(
        self,
        *,
        range_key: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> tuple[datetime | None, datetime | None]:
        start_dt = self.parse_optional_datetime(start_ts)
        end_dt = self.parse_optional_datetime(end_ts)
        if start_dt is not None or end_dt is not None:
            return start_dt, end_dt
        normalized_range_key = self.normalize_history_range_key(range_key)
        derived_start = self.history_window_start(normalized_range_key)
        return self.parse_optional_datetime(derived_start), None

    def history_target_points(self, range_key: str, limit: int | None) -> int:
        configured = HISTORY_RANGE_CONFIG.get(range_key, HISTORY_RANGE_CONFIG[self.context.default_history_range_key])["target_points"]
        target_points = int(configured) if isinstance(configured, int) else 300
        if limit is None or limit <= 0:
            return target_points
        return min(limit, target_points)

    def parse_optional_datetime(self, value: str | None) -> datetime | None:
        if value in (None, ""):
            return None
        normalized = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)

    def downsample_history_rows(self, history: list[dict], target_points: int) -> list[dict]:
        if target_points <= 0 or len(history) <= target_points:
            return history

        timestamped_rows = [
            (row, self.parse_history_ts(row))
            for row in history
        ]
        valid_rows = [(row, ts) for row, ts in timestamped_rows if ts is not None]
        if len(valid_rows) <= target_points:
            return [row for row, _ in valid_rows] if valid_rows else history

        interval_seconds = self.estimate_interval_seconds([ts for _, ts in valid_rows])
        total_span_seconds = max(1, int((valid_rows[-1][1] - valid_rows[0][1]).total_seconds()))
        bucket_seconds = max(interval_seconds, math.ceil(total_span_seconds / max(target_points - 1, 1)))
        anchor_ts = valid_rows[0][1]

        compressed: list[dict] = [valid_rows[0][0]]
        current_bucket = 0
        last_row_in_bucket = valid_rows[0][0]
        for row, ts in valid_rows[1:-1]:
            bucket = int((ts - anchor_ts).total_seconds() // bucket_seconds)
            if bucket == current_bucket:
                last_row_in_bucket = row
                continue
            if last_row_in_bucket is not compressed[-1]:
                compressed.append(last_row_in_bucket)
            current_bucket = bucket
            last_row_in_bucket = row

        if valid_rows[-1][0] is not compressed[-1]:
            compressed.append(valid_rows[-1][0])

        if len(compressed) <= target_points:
            return compressed

        step = max(1, math.ceil(len(compressed) / target_points))
        reduced = compressed[::step]
        if reduced[-1] is not compressed[-1]:
            reduced.append(compressed[-1])
        return reduced

    def filter_snapshot_history(
        self,
        history: list[dict],
        *,
        domestic_symbol: str,
        overseas_symbol: str,
    ) -> list[dict]:
        return [
            row
            for row in history
            if row.get("domestic_symbol") == domestic_symbol and row.get("overseas_symbol") == overseas_symbol
        ]

    def merge_history_rows(self, primary_rows: list[dict], snapshot_rows: list[dict]) -> list[dict]:
        if not primary_rows:
            return snapshot_rows
        if not snapshot_rows:
            return primary_rows

        merged: dict[str, dict] = {}
        for row in primary_rows:
            key = self.history_row_key(row)
            if key:
                merged[key] = row
        for row in snapshot_rows:
            key = self.history_row_key(row)
            if key:
                merged[key] = row

        return sorted(
            merged.values(),
            key=lambda row: self.parse_history_ts(row) or datetime.min.replace(tzinfo=UTC),
        )

    def history_row_key(self, row: dict) -> str:
        raw_value = row.get("ts") or row.get("ts_utc") or row.get("ts_local")
        return str(raw_value) if raw_value is not None else ""

    def parse_history_ts(self, row: dict) -> datetime | None:
        raw_value = row.get("ts") or row.get("ts_utc") or row.get("ts_local")
        if raw_value is None:
            return None
        if isinstance(raw_value, datetime):
            return raw_value if raw_value.tzinfo else raw_value.replace(tzinfo=UTC)
        value = str(raw_value).replace("Z", "+00:00")
        with suppress(ValueError):
            parsed = datetime.fromisoformat(value)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        return None

    def estimate_interval_seconds(self, timestamps: list[datetime]) -> int:
        deltas = [
            int((current - previous).total_seconds())
            for previous, current in zip(timestamps, timestamps[1:], strict=False)
            if (current - previous).total_seconds() > 0
        ]
        if not deltas:
            return 60
        deltas.sort()
        return max(1, deltas[len(deltas) // 2])

    def build_chart_history(
        self,
        pair: PairConfig,
        domestic_symbol: str,
        overseas_symbol: str,
        *,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> list[dict]:
        normalized_rows = self.context.repository.fetch_normalized_domestic_history(
            pair.group_name,
            symbol=domestic_symbol,
            limit=None,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        if normalized_rows:
            overseas_rows = self.context.repository.fetch_raw_quote_history(
                pair.group_name,
                "overseas",
                symbol=overseas_symbol,
                limit=None,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            if overseas_rows:
                history: list[dict] = []
                for overseas_row, domestic_row in self.align_history_rows(overseas_rows, normalized_rows):
                    spread, spread_pct = compute_spread(domestic_row["normalized_last"], overseas_row["last_px"])
                    history.append(
                        {
                            "ts": overseas_row["ts"],
                            "ts_utc": overseas_row["ts_utc"],
                            "ts_local": overseas_row["ts_local"],
                            "domestic_symbol": domestic_row["symbol"],
                            "overseas_symbol": overseas_row["symbol"],
                            "spread": spread,
                            "spread_pct": spread_pct,
                            "zscore": None,
                            "domestic_last_raw": domestic_row["raw_last"],
                            "normalized_last": domestic_row["normalized_last"],
                            "overseas_last": overseas_row["last_px"],
                            "status": None,
                            "signal_state": None,
                            "pause_reason": None,
                        }
                    )
                if history:
                    return history

        domestic_rows = self.context.repository.fetch_raw_quote_history(
            pair.group_name,
            "domestic",
            symbol=domestic_symbol,
            limit=None,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        overseas_rows = self.context.repository.fetch_raw_quote_history(
            pair.group_name,
            "overseas",
            symbol=overseas_symbol,
            limit=None,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        fx_rows = self.fetch_fx_history_rows(start_ts=start_ts, end_ts=end_ts)
        if not domestic_rows or not overseas_rows or not fx_rows:
            return []

        normalized_domestic_rows: list[dict] = []
        for domestic_row, fx_row in self.align_history_rows(domestic_rows, fx_rows):
            normalized_last = normalize_domestic_price(domestic_row["last_px"], pair, fx_row["rate"])
            normalized_domestic_rows.append(
                {
                    "ts": domestic_row["ts"],
                    "ts_utc": domestic_row["ts_utc"],
                    "ts_local": domestic_row["ts_local"],
                    "symbol": domestic_row["symbol"],
                    "raw_last": domestic_row["last_px"],
                    "normalized_last": normalized_last,
                }
            )

        if not normalized_domestic_rows:
            return []

        history: list[dict] = []
        for domestic_row, overseas_row in self.align_history_rows(normalized_domestic_rows, overseas_rows):
            spread, spread_pct = compute_spread(domestic_row["normalized_last"], overseas_row["last_px"])
            history.append(
                {
                    "ts": domestic_row["ts"],
                    "ts_utc": domestic_row["ts_utc"],
                    "ts_local": domestic_row["ts_local"],
                    "domestic_symbol": domestic_row["symbol"],
                    "overseas_symbol": overseas_row["symbol"],
                    "spread": spread,
                    "spread_pct": spread_pct,
                    "zscore": None,
                    "domestic_last_raw": domestic_row["raw_last"],
                    "normalized_last": domestic_row["normalized_last"],
                    "overseas_last": overseas_row["last_px"],
                    "status": None,
                    "signal_state": None,
                    "pause_reason": None,
                }
            )
        return history

    def fetch_fx_history_rows(
        self,
        *,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> list[dict]:
        merged_rows: dict[str, dict] = {}
        for source_name in [self.context.config.app.fx_source, *self.context.config.app.fx_backup_sources]:
            if not source_name:
                continue
            rows = self.context.repository.fetch_fx_history(
                source_name,
                limit=None,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            for row in rows:
                row_ts = str(row.get("ts") or "")
                if row_ts and row_ts not in merged_rows:
                    merged_rows[row_ts] = row
        return [merged_rows[row_ts] for row_ts in sorted(merged_rows)]

    def ensure_overseas_history(
        self,
        pair: PairConfig,
        *,
        range_key: str,
        start_ts: str | None,
        end_ts: str | None,
    ) -> None:
        candidate = self.route_preferences.selected_overseas_candidate(pair)
        if candidate is None:
            return
        adapter = self.context.adapters.get(candidate.source)
        if adapter is None or not hasattr(adapter, "fetch_history"):
            return

        existing_rows = self.context.repository.fetch_raw_quote_history(
            pair.group_name,
            "overseas",
            symbol=candidate.symbol,
            limit=None,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        required_rows = max(24, self.history_target_points(range_key, self.context.history_preview_limit) // 3)
        if len(existing_rows) >= required_rows:
            return

        interval = OVERSEAS_HISTORY_INTERVAL_BY_RANGE.get(range_key, "60m")
        cache_key = (
            pair.group_name,
            candidate.source,
            candidate.symbol,
            interval,
            start_ts or "",
            end_ts or "",
        )
        attempted_at = self.context.history_backfill_attempts.get(cache_key)
        if attempted_at is not None and (utc_now() - attempted_at).total_seconds() < 300:
            return
        self.context.history_backfill_attempts[cache_key] = utc_now()
        self.backfill_overseas_history(
            pair.group_name,
            interval=interval,
            range_key=range_key,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    def align_history_rows(self, reference_rows: list[dict], compare_rows: list[dict]) -> list[tuple[dict, dict]]:
        if not reference_rows or not compare_rows:
            return []

        parsed_reference = [(row, self.parse_history_ts(row)) for row in reference_rows]
        parsed_reference = [(row, ts) for row, ts in parsed_reference if ts is not None]
        parsed_compare = [(row, self.parse_history_ts(row)) for row in compare_rows]
        parsed_compare = [(row, ts) for row, ts in parsed_compare if ts is not None]
        if not parsed_reference or not parsed_compare:
            return []

        aligned: list[tuple[dict, dict]] = []
        compare_index = 0
        latest_compare_row: dict | None = None
        for reference_row, reference_ts in parsed_reference:
            while compare_index < len(parsed_compare) and parsed_compare[compare_index][1] <= reference_ts:
                latest_compare_row = parsed_compare[compare_index][0]
                compare_index += 1
            if latest_compare_row is None:
                continue
            aligned.append((reference_row, latest_compare_row))
        return aligned

    def backfill_domestic_history(
        self,
        group_name: str,
        *,
        interval: str = "5m",
        range_key: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> dict:
        linked_groups = self.route_preferences.linked_variant_groups(group_name)
        selected_group = group_name if group_name in linked_groups else linked_groups[0]
        pair = self.context.pair_map[selected_group]
        try:
            candidate = self.domestic_history_candidate(pair)
        except ValueError as exc:
            return {
                "group_name": selected_group,
                "linked_groups": linked_groups,
                "supported": False,
                "interval": interval,
                "range_key": self.normalize_history_range_key(range_key),
                "domestic_source": pair.domestic_history_source or pair.domestic_source,
                "domestic_symbol": pair.domestic_history_symbol or pair.domestic_symbol,
                "domestic_label": pair.domestic_history_label or pair.domestic_label,
                "reason": str(exc),
            }

        adapter = self.context.adapters[candidate.source]
        fetch_history = getattr(adapter, "fetch_history", None)
        normalized_range_key = self.normalize_history_range_key(range_key)
        start_dt, end_dt = self.resolve_history_window(
            range_key=normalized_range_key,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        if fetch_history is None:
            return {
                "group_name": selected_group,
                "linked_groups": linked_groups,
                "supported": False,
                "interval": interval,
                "range_key": normalized_range_key,
                "domestic_source": candidate.source,
                "domestic_symbol": candidate.symbol,
                "domestic_label": candidate.label,
                "reason": f"{candidate.source} does not expose same-source history backfill in the current adapter",
            }

        started = perf_counter()
        try:
            quotes = fetch_history(
                candidate.symbol,
                candidate.label,
                interval=interval,
                start_ts=start_dt,
                end_ts=end_dt,
            )
            latency_ms = (perf_counter() - started) * 1000
            self.health.record_success(candidate.source, candidate.symbol, latency_ms)
        except Exception as exc:
            latency_ms = (perf_counter() - started) * 1000
            self.health.record_failure(candidate.source, candidate.symbol, latency_ms, str(exc))
            return {
                "group_name": selected_group,
                "linked_groups": linked_groups,
                "supported": False,
                "interval": interval,
                "range_key": normalized_range_key,
                "domestic_source": candidate.source,
                "domestic_symbol": candidate.symbol,
                "domestic_label": candidate.label,
                "reason": str(exc),
            }

        inserted_total = 0
        skipped_total = 0
        per_group: list[dict] = []
        for linked_group in linked_groups:
            inserted = 0
            skipped = 0
            for quote in quotes:
                stored = self.context.repository.insert_raw_quote_if_missing(
                    linked_group,
                    "domestic",
                    quote,
                    timezone_name=self.context.config.app.timezone,
                )
                if stored:
                    inserted += 1
                else:
                    skipped += 1
            inserted_total += inserted
            skipped_total += skipped
            per_group.append(
                {
                    "group_name": linked_group,
                    "inserted_rows": inserted,
                    "skipped_rows": skipped,
                }
            )

        return {
            "group_name": selected_group,
            "linked_groups": linked_groups,
            "supported": True,
            "interval": interval,
            "range_key": normalized_range_key,
            "domestic_source": candidate.source,
            "domestic_symbol": candidate.symbol,
            "domestic_label": candidate.label,
            "requested_start_ts": start_dt.isoformat() if start_dt else None,
            "requested_end_ts": end_dt.isoformat() if end_dt else None,
            "available_start_ts": quotes[0].ts.isoformat() if quotes else None,
            "available_end_ts": quotes[-1].ts.isoformat() if quotes else None,
            "fetched_rows": len(quotes),
            "inserted_rows": inserted_total,
            "skipped_rows": skipped_total,
            "per_group": per_group,
        }

    def domestic_history_candidate(self, pair: PairConfig) -> QuoteRouteConfig:
        if pair.domestic_history_source:
            source_name = pair.domestic_history_source
            source_config = self.context.config.sources.get(source_name)
            if source_config is None:
                raise ValueError(f"{pair.group_name} domestic history source {source_name} is not configured")
            symbol = pair.domestic_history_symbol
            if not symbol and source_config.kind == "tqsdk_main":
                product_code = pair.domestic_product_code or infer_product_code(pair.domestic_symbol)
                symbol = tqsdk_main_symbol_for_product(product_code)
            label = pair.domestic_history_label
            if not label:
                label = f"TqSdk {pair.domestic_label}" if source_config.kind == "tqsdk_main" else pair.domestic_label
            if not symbol:
                raise ValueError(f"{pair.group_name} domestic history source {source_name} is missing a symbol")
            return QuoteRouteConfig(source=source_name, symbol=symbol, label=label)

        candidate = self.route_preferences.selected_domestic_candidate(pair)
        if candidate is None:
            raise ValueError(f"{pair.group_name} does not have a selected domestic candidate")
        return candidate

    def backfill_overseas_history(
        self,
        group_name: str,
        *,
        interval: str = "60m",
        range_key: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> dict:
        linked_groups = self.route_preferences.linked_variant_groups(group_name)
        selected_group = group_name if group_name in linked_groups else linked_groups[0]
        pair = self.context.pair_map[selected_group]
        candidate = self.route_preferences.selected_overseas_candidate(pair)
        if candidate is None:
            raise ValueError(f"{group_name} does not have a selected overseas candidate")

        adapter = self.context.adapters[candidate.source]
        fetch_history = getattr(adapter, "fetch_history", None)
        normalized_range_key = self.normalize_history_range_key(range_key)
        start_dt, end_dt = self.resolve_history_window(
            range_key=normalized_range_key,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        if fetch_history is None:
            return {
                "group_name": selected_group,
                "linked_groups": linked_groups,
                "supported": False,
                "interval": interval,
                "range_key": normalized_range_key,
                "overseas_source": candidate.source,
                "overseas_symbol": candidate.symbol,
                "overseas_label": candidate.label,
                "reason": f"{candidate.source} does not expose same-source history backfill in the current adapter",
            }

        started = perf_counter()
        try:
            quotes = fetch_history(
                candidate.symbol,
                candidate.label,
                interval=interval,
                start_ts=start_dt,
                end_ts=end_dt,
            )
            latency_ms = (perf_counter() - started) * 1000
            self.health.record_success(candidate.source, candidate.symbol, latency_ms)
        except Exception as exc:
            latency_ms = (perf_counter() - started) * 1000
            self.health.record_failure(candidate.source, candidate.symbol, latency_ms, str(exc))
            return {
                "group_name": selected_group,
                "linked_groups": linked_groups,
                "supported": False,
                "interval": interval,
                "range_key": normalized_range_key,
                "overseas_source": candidate.source,
                "overseas_symbol": candidate.symbol,
                "overseas_label": candidate.label,
                "reason": str(exc),
            }

        inserted_total = 0
        skipped_total = 0
        per_group: list[dict] = []
        for linked_group in linked_groups:
            inserted = 0
            skipped = 0
            for quote in quotes:
                stored = self.context.repository.insert_raw_quote_if_missing(
                    linked_group,
                    "overseas",
                    quote,
                    timezone_name=self.context.config.app.timezone,
                )
                if stored:
                    inserted += 1
                else:
                    skipped += 1
            inserted_total += inserted
            skipped_total += skipped
            per_group.append(
                {
                    "group_name": linked_group,
                    "inserted_rows": inserted,
                    "skipped_rows": skipped,
                }
            )

        return {
            "group_name": selected_group,
            "linked_groups": linked_groups,
            "supported": True,
            "interval": interval,
            "range_key": normalized_range_key,
            "overseas_source": candidate.source,
            "overseas_symbol": candidate.symbol,
            "overseas_label": candidate.label,
            "requested_start_ts": start_dt.isoformat() if start_dt else None,
            "requested_end_ts": end_dt.isoformat() if end_dt else None,
            "available_start_ts": quotes[0].ts.isoformat() if quotes else None,
            "available_end_ts": quotes[-1].ts.isoformat() if quotes else None,
            "fetched_rows": len(quotes),
            "inserted_rows": inserted_total,
            "skipped_rows": skipped_total,
            "per_group": per_group,
        }

    def tqsdk_source_name(self) -> str | None:
        if not self.context.config.app.tqsdk_shadow_enabled:
            return None
        source_name = self.context.config.app.tqsdk_shadow_source
        if not source_name:
            return None
        source_config = self.context.config.sources.get(source_name)
        if source_config is None or source_config.kind != "tqsdk_main":
            return None
        return source_name

    def tqsdk_shadow_specs(self) -> list[dict]:
        specs_by_product: dict[str, dict] = {}
        for pair in self.context.enabled_pairs:
            product_code = pair.domestic_product_code or infer_product_code(pair.domestic_symbol)
            symbol = tqsdk_main_symbol_for_product(product_code)
            if not product_code or not symbol:
                continue
            entry = specs_by_product.setdefault(
                product_code,
                {
                    "product_code": product_code,
                    "symbol": symbol,
                    "label": f"TqSdk {pair.domestic_label}",
                    "group_names": [],
                },
            )
            entry["group_names"].append(pair.group_name)
        return list(specs_by_product.values())

    async def maybe_backfill_tqsdk_shadow_history(self) -> None:
        if not self.context.config.app.tqsdk_shadow_enabled or not self.context.config.app.tqsdk_startup_backfill_enabled:
            return
        source_name = self.tqsdk_source_name()
        if source_name is None:
            return
        adapter = self.context.adapters.get(source_name)
        if (
            not isinstance(adapter, TqSdkMainAdapter)
            or not adapter.is_configured()
        ):
            return

        specs = self.tqsdk_shadow_specs()
        if not specs:
            return

        start_dt, end_dt = self.resolve_history_window(
            range_key=self.context.config.app.tqsdk_startup_backfill_range_key,
        )
        for spec in specs:
            started = perf_counter()
            try:
                quotes = await asyncio.to_thread(
                    adapter.fetch_history,
                    spec["symbol"],
                    spec["label"],
                    interval=self.context.config.app.tqsdk_startup_backfill_interval,
                    start_ts=start_dt,
                    end_ts=end_dt,
                )
                latency_ms = (perf_counter() - started) * 1000
                self.health.record_success(source_name, spec["symbol"], latency_ms)
            except Exception as exc:
                latency_ms = (perf_counter() - started) * 1000
                self.health.record_failure(source_name, spec["symbol"], latency_ms, str(exc))
                continue

            for quote in quotes:
                for group_name in spec["group_names"]:
                    self.context.repository.insert_raw_quote_if_missing(
                        group_name,
                        "domestic_shadow",
                        quote,
                        timezone_name=self.context.config.app.timezone,
                    )

    def start_tqsdk_shadow_collector(self) -> None:
        if not self.context.config.app.tqsdk_shadow_enabled or self.context.shadow_thread is not None:
            return
        source_name = self.tqsdk_source_name()
        if source_name is None:
            return
        adapter = self.context.adapters.get(source_name)
        if (
            not isinstance(adapter, TqSdkMainAdapter)
            or not adapter.is_configured()
            or not hasattr(adapter, "source_config")
        ):
            return
        specs = self.tqsdk_shadow_specs()
        if not specs:
            return

        runner = TqSdkShadowRunner(
            adapter=adapter,
            specs=specs,
            interval_sec=self.context.config.app.tqsdk_shadow_poll_interval_sec,
            on_quote=self.store_tqsdk_shadow_quote,
            on_success=self.health.record_success,
            on_failure=self.health.record_failure,
        )
        self.context.shadow_stop_event.clear()
        self.context.shadow_thread = threading.Thread(
            target=runner.run,
            args=(self.context.shadow_stop_event,),
            daemon=True,
            name="tqsdk-shadow",
        )
        self.context.shadow_thread.start()

    def store_tqsdk_shadow_quote(self, spec: dict, quote: MarketQuote) -> None:
        for group_name in spec["group_names"]:
            self.context.repository.insert_raw_quote_if_missing(
                group_name,
                "domestic_shadow",
                quote,
                timezone_name=self.context.config.app.timezone,
            )

    def align_shadow_rows(self, main_rows: list[dict], shadow_rows: list[dict]) -> list[dict]:
        aligned: list[dict] = []
        shadow_index = 0
        for main_row in main_rows:
            main_ts = self.parse_history_ts(main_row)
            main_last = main_row.get("last_px")
            if main_ts is None or main_last is None:
                continue
            while shadow_index + 1 < len(shadow_rows):
                current_ts = self.parse_history_ts(shadow_rows[shadow_index])
                next_ts = self.parse_history_ts(shadow_rows[shadow_index + 1])
                if current_ts is None or next_ts is None:
                    shadow_index += 1
                    continue
                if abs((next_ts - main_ts).total_seconds()) <= abs((current_ts - main_ts).total_seconds()):
                    shadow_index += 1
                    continue
                break
            shadow_row = shadow_rows[shadow_index]
            shadow_ts = self.parse_history_ts(shadow_row)
            shadow_last = shadow_row.get("last_px")
            if shadow_ts is None or shadow_last is None:
                continue
            if abs((main_ts - shadow_ts).total_seconds()) > 180:
                continue
            aligned.append(
                {
                    "main_ts": main_row["ts"],
                    "shadow_ts": shadow_row["ts"],
                    "main_last": float(main_last),
                    "shadow_last": float(shadow_last),
                }
            )
        return aligned

    def get_shadow_comparison(self, group_name: str, *, limit: int = 240) -> dict | None:
        pair = self.context.pair_map[group_name]
        shadow_source = self.tqsdk_source_name()
        shadow_symbol = tqsdk_main_symbol_for_product(pair.domestic_product_code or infer_product_code(pair.domestic_symbol))
        if shadow_source is None or shadow_symbol is None:
            return None

        main_rows = self.context.repository.fetch_raw_quote_history(
            group_name,
            "domestic",
            symbol=pair.domestic_symbol,
            limit=limit,
        )
        shadow_rows = self.context.repository.fetch_raw_quote_history(
            group_name,
            "domestic_shadow",
            symbol=shadow_symbol,
            limit=limit,
        )
        if not main_rows or not shadow_rows:
            return None

        aligned = self.align_shadow_rows(main_rows, shadow_rows)
        if not aligned:
            return None

        spreads = [item["main_last"] - item["shadow_last"] for item in aligned]
        abs_spreads = [abs(item) for item in spreads]
        mean_abs = sum(abs_spreads) / len(abs_spreads)
        max_abs = max(abs_spreads)
        latest = aligned[-1]
        latest_spread = latest["main_last"] - latest["shadow_last"]
        latest_pct = None
        denominator = (latest["main_last"] + latest["shadow_last"]) / 2
        if denominator:
            latest_pct = latest_spread / denominator

        return {
            "group_name": group_name,
            "enabled": True,
            "source_name": shadow_source,
            "main_symbol": pair.domestic_symbol,
            "shadow_symbol": shadow_symbol,
            "sample_count": len(aligned),
            "latest_main_last": latest["main_last"],
            "latest_shadow_last": latest["shadow_last"],
            "latest_spread": latest_spread,
            "latest_spread_pct": latest_pct,
            "mean_abs_spread": mean_abs,
            "max_abs_spread": max_abs,
            "latest_main_ts": latest["main_ts"],
            "latest_shadow_ts": latest["shadow_ts"],
            "history": [
                {
                    "ts": item["main_ts"],
                    "main_last": item["main_last"],
                    "shadow_last": item["shadow_last"],
                    "spread": item["main_last"] - item["shadow_last"],
                }
                for item in aligned
            ],
        }
