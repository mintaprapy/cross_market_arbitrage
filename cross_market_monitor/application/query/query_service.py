from __future__ import annotations

from cross_market_monitor.application.common import variant_group_base
from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.application.control.route_preference_service import RoutePreferenceService
from cross_market_monitor.application.history.history_service import HistoryService
from cross_market_monitor.domain.commodity_specs import build_commodity_spec
from cross_market_monitor.domain.models import RuntimeHealth, SourceHealth, SpreadSnapshot, WorkerRuntimeState
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

    def _current_snapshots(self) -> dict[str, SpreadSnapshot]:
        enabled_group_names = {pair.group_name for pair in self.context.enabled_pairs}
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

    def get_health(self) -> dict:
        snapshots = self._current_snapshots()
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
            if pair.enabled
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
        snapshots = self._current_snapshots()
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

    def get_snapshot(self, *, include_cards: bool = False) -> dict:
        summary = self.get_snapshot_summary()
        domestic_preferences = {
            pair.group_name: self.route_preferences.get_domestic_route_options(pair.group_name, refresh_dynamic=False)
            for pair in self.context.enabled_pairs
        }
        overseas_preferences = {
            pair.group_name: self.route_preferences.get_overseas_route_options(pair.group_name)
            for pair in self.context.enabled_pairs
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
            for pair in self.context.enabled_pairs
        }
        payload["shadow_comparisons"] = {
            pair.group_name: self.history.get_shadow_comparison(pair.group_name, limit=120)
            for pair in self.context.enabled_pairs
        }
        return payload

    def get_card_view(self, group_name: str, range_key: str | None = None) -> dict:
        snapshots = self._current_snapshots()
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
            self._snapshot_payload(snapshots[pair_group])
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
        return {
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
            "replay_summary": self.replay_summary(selected_group, limit=500),
        }

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
