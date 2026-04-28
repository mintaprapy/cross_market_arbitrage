from __future__ import annotations

from cross_market_monitor.application.common import age_seconds, is_pair_domestic_session_open, utc_now, variant_group_base
from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.application.control.route_preference_service import RoutePreferenceService
from cross_market_monitor.application.history.history_service import HistoryService
from cross_market_monitor.domain.commodity_specs import build_commodity_spec
from cross_market_monitor.domain.formulas import compute_spread, normalize_domestic_quote
from cross_market_monitor.domain.models import FXQuote, MarketQuote, PairConfig, RuntimeHealth, SourceHealth, SpreadSnapshot, WorkerRuntimeState
from cross_market_monitor.domain.source_capabilities import capability_for_source


class QueryService:
    def __init__(
        self,
        context: ServiceContext,
        route_preferences: RoutePreferenceService,
        history: HistoryService,
    ) -> None:
        self.context = context
        self.route_preferences = route_preferences
        self.history = history

    def _current_snapshots(self, pairs: list | None = None) -> dict[str, SpreadSnapshot]:
        target_pairs = pairs or self.context.dashboard_pairs
        enabled_group_names = {pair.group_name for pair in target_pairs}
        if self.context.is_polling:
            return {
                group_name: snapshot
                for group_name, snapshot in self.context.latest_snapshots.items()
                if group_name in enabled_group_names
            }
        latest = {
            snapshot.group_name: snapshot
            for snapshot in self.context.repository.load_latest_snapshots()
            if snapshot.group_name in enabled_group_names
        }
        fallback = {
            group_name: snapshot
            for group_name, snapshot in self.context.latest_snapshots.items()
            if group_name in enabled_group_names
        }
        return latest or fallback

    def _current_runtime_state(self, snapshots: dict[str, SpreadSnapshot]) -> WorkerRuntimeState:
        if self.context.is_polling:
            return WorkerRuntimeState(
                started_at=self.context.started_at,
                last_poll_started_at=self.context.last_poll_started_at,
                last_poll_finished_at=self.context.last_poll_finished_at,
                last_heartbeat_at=self.context.last_poll_finished_at or self.context.last_poll_started_at or self.context.started_at,
                is_polling=self.context.is_polling,
                total_cycles=self.context.total_cycles,
                latest_fx_rate=self.context.latest_fx_quote.rate if self.context.latest_fx_quote else None,
                latest_fx_source=self.context.latest_fx_quote.source_name if self.context.latest_fx_quote else None,
                latest_fx_jump_pct=self.context.latest_fx_jump_pct,
                fx_is_live=self.context.latest_fx_is_live,
                fx_is_frozen=bool(self.context.latest_fx_quote is not None and not self.context.latest_fx_is_live),
                fx_last_live_at=self.context.latest_fx_last_live_at,
                fx_frozen_since=self.context.latest_fx_frozen_since,
            )
        persisted = self.context.repository.load_runtime_state("worker")
        if persisted is not None:
            return persisted
        return WorkerRuntimeState(
            started_at=self.context.started_at,
            last_poll_started_at=self.context.last_poll_started_at,
            last_poll_finished_at=max((snapshot.ts for snapshot in snapshots.values()), default=self.context.last_poll_finished_at),
            is_polling=self.context.is_polling,
            total_cycles=self.context.total_cycles,
            latest_fx_rate=self._current_latest_fx_rate(snapshots),
            latest_fx_source=self.context.latest_fx_quote.source_name if self.context.latest_fx_quote else None,
            latest_fx_jump_pct=self.context.latest_fx_jump_pct,
            fx_is_live=self.context.latest_fx_is_live,
            fx_is_frozen=bool(self.context.latest_fx_quote is not None and not self.context.latest_fx_is_live),
            fx_last_live_at=self.context.latest_fx_last_live_at,
            fx_frozen_since=self.context.latest_fx_frozen_since,
        )

    def _current_source_health(self) -> list[SourceHealth]:
        if self.context.is_polling:
            return sorted(self.context.source_health.values(), key=lambda entry: entry.source_name)
        persisted = {item.source_name: item for item in self.context.repository.load_source_health_state()}
        merged: list[SourceHealth] = []
        for source_name in sorted(self.context.source_health):
            merged.append(persisted.get(source_name, self.context.source_health[source_name]))
        for source_name in sorted(set(persisted) - set(self.context.source_health)):
            merged.append(persisted[source_name])
        return merged

    def _current_last_poll_finished_at(self, snapshots: dict[str, SpreadSnapshot]) -> str | None:
        if self.context.is_polling and self.context.last_poll_finished_at is not None:
            return self.context.last_poll_finished_at.isoformat()
        if not snapshots:
            return self.context.last_poll_finished_at.isoformat() if self.context.last_poll_finished_at else None
        return max(snapshot.ts for snapshot in snapshots.values()).isoformat()

    def _current_latest_fx_rate(self, snapshots: dict[str, SpreadSnapshot]) -> float | None:
        if self.context.is_polling and self.context.latest_fx_quote is not None:
            return self.context.latest_fx_quote.rate
        latest_fx = self.context.repository.load_latest_fx_rate_any(
            [self.context.config.app.fx_source, *self.context.config.app.fx_backup_sources]
        )
        if latest_fx is not None:
            return latest_fx.rate
        if not snapshots:
            return self.context.latest_fx_quote.rate if self.context.latest_fx_quote else None
        newest_snapshot = max(snapshots.values(), key=lambda item: item.ts)
        return newest_snapshot.fx_rate

    def _snapshot_payload(self, snapshot: SpreadSnapshot) -> dict:
        payload = snapshot.model_dump(mode="json")
        pair = self.context.pair_map.get(snapshot.group_name)
        payload["hedge_contract_size"] = pair.hedge_contract_size if pair is not None else None
        payload["domestic_lot_size"] = pair.domestic_lot_size if pair is not None else None
        payload["domestic_lot_notional"] = (
            snapshot.domestic_last_raw * pair.domestic_lot_size
            if pair is not None and pair.domestic_lot_size is not None and snapshot.domestic_last_raw is not None
            else None
        )
        payload["trading_sessions_local"] = list(pair.trading_sessions_local) if pair is not None else []
        payload["domestic_weekends_closed"] = self.context.config.app.domestic_weekends_closed
        payload["domestic_non_trading_dates_local"] = [
            item.isoformat()
            for item in self.context.config.app.domestic_non_trading_dates_local
        ]
        payload["commodity_spec"] = build_commodity_spec(pair) if pair is not None else None
        return payload

    def _advance_snapshot_age(self, snapshot: SpreadSnapshot, age_sec: float | None) -> float | None:
        if age_sec is None:
            return age_seconds(snapshot.ts)
        return max(age_sec + age_seconds(snapshot.ts), 0.0)

    def _snapshot_with_live_overseas_when_closed(self, snapshot: SpreadSnapshot) -> SpreadSnapshot:
        pair = self.context.pair_map.get(snapshot.group_name)
        if pair is None or not pair.trading_sessions_local:
            return snapshot
        now_utc = utc_now()
        now_local = now_utc.astimezone(self.context.local_tz)
        if is_pair_domestic_session_open(
            pair,
            now_local,
            non_trading_dates=self.context.config.app.domestic_non_trading_dates_local,
            weekends_closed=self.context.config.app.domestic_weekends_closed,
        ):
            return snapshot
        overseas_symbol = snapshot.overseas_symbol or pair.overseas_symbol
        latest_overseas = self.context.repository.load_latest_raw_quote_before(
            snapshot.group_name,
            "overseas",
            overseas_symbol,
            now_utc,
        )
        if latest_overseas is None:
            return snapshot
        route_detail = dict(snapshot.route_detail)
        route_detail["off_session_overseas_only"] = True
        off_session_memory_snapshot = bool(snapshot.route_detail.get("off_session_overseas_only"))
        return snapshot.model_copy(
            update={
                "overseas_source": latest_overseas.source_name,
                "overseas_symbol": latest_overseas.symbol,
                "overseas_label": latest_overseas.label,
                "overseas_last": latest_overseas.last,
                "overseas_bid": latest_overseas.bid,
                "overseas_ask": latest_overseas.ask,
                "domestic_age_sec": (
                    snapshot.domestic_age_sec
                    if off_session_memory_snapshot
                    else self._advance_snapshot_age(snapshot, snapshot.domestic_age_sec)
                ),
                "overseas_age_sec": age_seconds(latest_overseas.ts),
                "fx_age_sec": (
                    snapshot.fx_age_sec
                    if off_session_memory_snapshot
                    else self._advance_snapshot_age(snapshot, snapshot.fx_age_sec)
                ),
                "route_detail": route_detail,
            }
        )

    def _snapshot_with_latest_values_when_partial(self, snapshot: SpreadSnapshot) -> SpreadSnapshot:
        if snapshot.status != "partial":
            return snapshot
        pair = self.context.pair_map.get(snapshot.group_name)
        if pair is None:
            return snapshot

        now_utc = utc_now()
        updates: dict = {}
        domestic_last = snapshot.domestic_last_raw
        domestic_bid = snapshot.domestic_bid_raw
        domestic_ask = snapshot.domestic_ask_raw
        overseas_last = snapshot.overseas_last
        overseas_bid = snapshot.overseas_bid
        overseas_ask = snapshot.overseas_ask
        fx_rate = snapshot.fx_rate

        if domestic_last is None:
            latest_domestic = self._latest_leg_quote(
                pair,
                snapshot,
                "domestic",
                now_utc,
            )
            if latest_domestic is not None:
                domestic_last = latest_domestic.last
                domestic_bid = latest_domestic.bid
                domestic_ask = latest_domestic.ask
                updates.update(
                    {
                        "domestic_source": latest_domestic.source_name,
                        "domestic_symbol": latest_domestic.symbol,
                        "domestic_label": latest_domestic.label,
                        "domestic_last_raw": latest_domestic.last,
                        "domestic_bid_raw": latest_domestic.bid,
                        "domestic_ask_raw": latest_domestic.ask,
                        "domestic_age_sec": age_seconds(latest_domestic.ts),
                    }
                )

        if overseas_last is None:
            latest_overseas = self._latest_leg_quote(
                pair,
                snapshot,
                "overseas",
                now_utc,
            )
            if latest_overseas is not None:
                overseas_last = latest_overseas.last
                overseas_bid = latest_overseas.bid
                overseas_ask = latest_overseas.ask
                updates.update(
                    {
                        "overseas_source": latest_overseas.source_name,
                        "overseas_symbol": latest_overseas.symbol,
                        "overseas_label": latest_overseas.label,
                        "overseas_last": latest_overseas.last,
                        "overseas_bid": latest_overseas.bid,
                        "overseas_ask": latest_overseas.ask,
                        "overseas_age_sec": age_seconds(latest_overseas.ts),
                    }
                )

        if fx_rate is None:
            latest_fx = self._latest_fx_quote(snapshot)
            if latest_fx is not None:
                fx_rate = latest_fx.rate
                updates.update(
                    {
                        "fx_source": latest_fx.source_name,
                        "fx_rate": latest_fx.rate,
                        "fx_age_sec": age_seconds(latest_fx.ts),
                    }
                )

        normalized_last = snapshot.normalized_last
        if domestic_last is not None and fx_rate is not None:
            normalized_quote = normalize_domestic_quote(
                pair,
                fx_rate,
                domestic_last,
                domestic_bid,
                domestic_ask,
            )
            normalized_last = normalized_quote.last
            updates.update(
                {
                    "normalized_last": normalized_quote.last,
                    "normalized_bid": normalized_quote.bid,
                    "normalized_ask": normalized_quote.ask,
                }
            )

        spread, spread_pct = compute_spread(normalized_last, overseas_last)
        if spread is not None and spread_pct is not None:
            updates.update({"spread": spread, "spread_pct": spread_pct})
            if snapshot.rolling_mean is not None and snapshot.rolling_std not in (None, 0):
                updates["zscore"] = (spread_pct - snapshot.rolling_mean) / snapshot.rolling_std

        if not updates:
            return snapshot

        route_detail = dict(snapshot.route_detail)
        route_detail["query_latest_values_fallback"] = True
        updates["route_detail"] = route_detail
        return snapshot.model_copy(update=updates)

    def _latest_leg_quote(
        self,
        pair: PairConfig,
        snapshot: SpreadSnapshot,
        leg_type: str,
        target_ts,
    ) -> MarketQuote | None:
        for symbol in self._candidate_leg_symbols(pair, snapshot, leg_type):
            quote = self.context.repository.load_latest_raw_quote_before(
                snapshot.group_name,
                leg_type,
                symbol,
                target_ts,
            )
            if quote is not None:
                return quote
        return None

    def _candidate_leg_symbols(
        self,
        pair: PairConfig,
        snapshot: SpreadSnapshot,
        leg_type: str,
    ) -> list[str]:
        values: list[str | None] = []
        if leg_type == "domestic":
            values.extend(
                [
                    snapshot.domestic_symbol,
                    getattr(self.context, "preferred_domestic_symbols", {}).get(pair.group_name),
                    pair.domestic_symbol,
                ]
            )
            values.extend(candidate.symbol for candidate in pair.domestic_candidates if candidate.enabled)
            values.extend(candidate.symbol for candidate in pair.domestic_candidates if not candidate.enabled)
        else:
            values.extend(
                [
                    snapshot.overseas_symbol,
                    getattr(self.context, "preferred_overseas_symbols", {}).get(pair.group_name),
                    pair.overseas_symbol,
                ]
            )
            values.extend(candidate.symbol for candidate in pair.overseas_candidates if candidate.enabled)
            values.extend(candidate.symbol for candidate in pair.overseas_candidates if not candidate.enabled)

        symbols: list[str] = []
        seen: set[str] = set()
        for value in values:
            if not value or value in seen:
                continue
            symbols.append(value)
            seen.add(value)
        return symbols

    def _latest_fx_quote(self, snapshot: SpreadSnapshot) -> FXQuote | None:
        if self.context.latest_fx_quote is not None:
            return self.context.latest_fx_quote
        source_names = [
            snapshot.fx_source,
            self.context.config.app.fx_source,
            *self.context.config.app.fx_backup_sources,
        ]
        deduped = [source for index, source in enumerate(source_names) if source and source not in source_names[:index]]
        return self.context.repository.load_latest_fx_rate_any(deduped)

    def get_health(self) -> dict:
        snapshots = self._current_snapshots(self.context.dashboard_pairs)
        runtime_state = self._current_runtime_state(snapshots)
        health = RuntimeHealth(
            started_at=runtime_state.started_at,
            last_poll_started_at=runtime_state.last_poll_started_at,
            last_poll_finished_at=runtime_state.last_poll_finished_at,
            poll_interval_sec=self.context.config.app.poll_interval_sec,
            rolling_window_size=self.context.config.app.rolling_window_size,
            history_limit=self.context.config.app.history_limit,
            is_polling=runtime_state.is_polling,
            total_cycles=runtime_state.total_cycles,
            latest_fx_rate=runtime_state.latest_fx_rate,
            latest_fx_source=runtime_state.latest_fx_source,
            latest_fx_jump_pct=runtime_state.latest_fx_jump_pct,
            last_heartbeat_at=runtime_state.last_heartbeat_at,
            fx_is_live=runtime_state.fx_is_live,
            fx_is_frozen=runtime_state.fx_is_frozen,
            fx_last_live_at=runtime_state.fx_last_live_at,
            fx_frozen_since=runtime_state.fx_frozen_since,
        ).model_dump(mode="json")
        health["pairs"] = [
            {
                "group_name": pair.group_name,
                "status": snapshots.get(pair.group_name).status if pair.group_name in snapshots else "waiting",
            }
            for pair in self.context.config.pairs
            if pair.enabled and pair.dashboard_enabled
        ]
        health["sources"] = [
            {
                **item.model_dump(mode="json"),
                "capability": capability_for_source(
                    item.source_name,
                    self.context.config.sources[item.source_name],
                ) if item.source_name in self.context.config.sources else None,
            }
            for item in self._current_source_health()
        ]
        return health

    def get_snapshot_summary(self) -> dict:
        snapshots = {
            group_name: self._snapshot_with_live_overseas_when_closed(snapshot)
            for group_name, snapshot in self._current_snapshots(self.context.dashboard_pairs).items()
        }
        return {
            "as_of": self._current_last_poll_finished_at(snapshots),
            "health": self.get_health(),
            "default_history_range_key": self.context.default_history_range_key,
            "source_capabilities": {
                source_name: capability_for_source(source_name, source_config)
                for source_name, source_config in self.context.config.sources.items()
            },
            "snapshots": [
                self._snapshot_payload(snapshot)
                for snapshot in sorted(snapshots.values(), key=lambda item: item.group_name)
            ],
        }

    def get_snapshot_row(self, group_name: str) -> dict | None:
        snapshots = self._current_snapshots(self.context.enabled_pairs)
        snapshot = snapshots.get(group_name)
        if snapshot is None:
            return None
        snapshot = self._snapshot_with_live_overseas_when_closed(snapshot)
        snapshot = self._snapshot_with_latest_values_when_partial(snapshot)
        return self._snapshot_payload(snapshot)

    def get_snapshot(self, *, include_cards: bool = False) -> dict:
        summary = self.get_snapshot_summary()
        domestic_preferences = {
            pair.group_name: self.route_preferences.get_domestic_route_options(pair.group_name, refresh_dynamic=False)
            for pair in self.context.dashboard_pairs
        }
        overseas_preferences = {
            pair.group_name: self.route_preferences.get_overseas_route_options(pair.group_name)
            for pair in self.context.dashboard_pairs
        }
        payload = {
            **summary,
            "domestic_route_preferences": domestic_preferences,
            "overseas_route_preferences": overseas_preferences,
            "card_endpoint": "/api/card",
            "snapshot_mode": "lightweight",
        }
        if not include_cards:
            return payload
        payload["snapshot_mode"] = "full"
        payload["histories"] = {
            pair.group_name: self.history.get_history(
                pair.group_name,
                limit=self.context.history_preview_limit,
                range_key=self.context.default_history_range_key,
            )
            for pair in self.context.dashboard_pairs
        }
        payload["shadow_comparisons"] = {
            pair.group_name: self.history.get_shadow_comparison(pair.group_name, limit=120)
            for pair in self.context.dashboard_pairs
        }
        return payload

    def get_card_view(
        self,
        group_name: str,
        range_key: str | None = None,
        *,
        include_replay: bool = False,
    ) -> dict:
        snapshots = self._current_snapshots(self.context.dashboard_pairs)
        linked_groups = self.route_preferences.linked_variant_groups(group_name)
        selected_group = group_name if group_name in linked_groups else linked_groups[0]
        domestic_preference = self.route_preferences.get_domestic_route_options(selected_group, refresh_dynamic=False)
        overseas_preference = self.route_preferences.get_overseas_route_options(selected_group)
        normalized_range_key = self.history.normalize_history_range_key(range_key)
        history_rows = self.history.get_history(
            selected_group,
            limit=self.context.history_card_limit,
            range_key=normalized_range_key,
        )
        shadow_comparison = self.history.get_shadow_comparison(selected_group, limit=240)
        variants = [
            self._snapshot_payload(self._snapshot_with_live_overseas_when_closed(snapshots[pair_group]))
            for pair_group in linked_groups
            if pair_group in snapshots
        ]
        selected_item = next(
            (item for item in variants if item["group_name"] == selected_group),
            variants[0] if variants else None,
        )
        if selected_item is not None:
            selected_item = dict(selected_item)
            selected_item["domestic_symbol"] = domestic_preference["selected_symbol"] or selected_item.get("domestic_symbol")
            selected_item["overseas_symbol"] = overseas_preference["selected_symbol"] or selected_item.get("overseas_symbol")
            selected_item["domestic_label"] = domestic_preference["selected_label"] or selected_item.get("domestic_label")
            selected_item["overseas_label"] = overseas_preference["selected_label"] or selected_item.get("overseas_label")
            if history_rows:
                latest_history = history_rows[-1]
                selected_item["domestic_symbol"] = latest_history["domestic_symbol"]
                selected_item["overseas_symbol"] = latest_history["overseas_symbol"]
                selected_item["domestic_last_raw"] = latest_history["domestic_last_raw"]
                selected_item["normalized_last"] = latest_history["normalized_last"]
                selected_item["overseas_last"] = latest_history["overseas_last"]
                selected_item["spread"] = latest_history["spread"]
                selected_item["spread_pct"] = latest_history["spread_pct"]
        payload = {
            "card_group": {
                "card_key": variant_group_base(selected_group),
                "variants": variants,
                "selected_item": selected_item,
            },
            "domestic_route_preference": domestic_preference,
            "overseas_route_preference": overseas_preference,
            "history_range_key": normalized_range_key,
            "history": history_rows,
            "shadow_comparison": shadow_comparison,
        }
        if include_replay:
            payload["replay_summary"] = self.replay_summary(selected_group, limit=500)
        return payload

    def get_alerts(self, limit: int = 100) -> list[dict]:
        return self.context.repository.fetch_alerts(limit)

    def get_notification_deliveries(self, limit: int = 100) -> list[dict]:
        return self.context.repository.fetch_notification_deliveries(limit)

    def get_job_runs(self) -> list[dict]:
        return [
            item.model_dump(mode="json")
            for item in self.context.repository.load_job_runs()
        ]

    def get_source_health(self) -> list[dict]:
        return [
            item.model_dump(mode="json")
            for item in self._current_source_health()
        ]

    def replay_summary(
        self,
        group_name: str,
        *,
        limit: int = 1000,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> dict:
        return self.context.replay.analyze(group_name, limit=limit, start_ts=start_ts, end_ts=end_ts)
