from __future__ import annotations

from cross_market_monitor.application.common import (
    default_overseas_symbol,
    dedupe_candidates,
    prioritize_candidates,
    variant_group_base,
)
from cross_market_monitor.application.context import ServiceContext
from cross_market_monitor.domain.models import PairConfig, QuoteRouteConfig


class RoutePreferenceService:
    def __init__(self, context: ServiceContext) -> None:
        self.context = context

    def load_persisted_preferences(self) -> None:
        for row in self.context.repository.load_route_preferences():
            group_name = row["group_name"]
            leg_type = row["leg_type"]
            symbol = row["symbol"]
            if group_name not in self.context.pair_map:
                continue
            if leg_type == "domestic":
                self.context.preferred_domestic_symbols[group_name] = symbol
            elif leg_type == "overseas":
                self.context.preferred_overseas_symbols[group_name] = symbol

    def selected_domestic_candidate(self, pair: PairConfig) -> QuoteRouteConfig | None:
        selected_symbol = self.context.preferred_domestic_symbols.get(pair.group_name, pair.domestic_symbol)
        options = self.all_domestic_candidates(pair)
        return next((item for item in options if item.symbol == selected_symbol), None)

    def selected_overseas_candidate(self, pair: PairConfig) -> QuoteRouteConfig | None:
        selected_symbol = self.context.preferred_overseas_symbols.get(pair.group_name, default_overseas_symbol(pair))
        options = self.all_overseas_candidates(pair)
        return next((item for item in options if item.symbol == selected_symbol), None)

    def get_domestic_route_options(self, group_name: str, *, refresh_dynamic: bool = True) -> dict:
        pair = self.context.pair_map[group_name]
        options = self.all_domestic_candidates(pair, refresh_dynamic=refresh_dynamic)
        selected = self.context.preferred_domestic_symbols.get(group_name, pair.domestic_symbol)
        return {
            "group_name": group_name,
            "selected_symbol": selected,
            "selected_label": next((item.label for item in options if item.symbol == selected), None),
            "options": [
                {
                    "source": candidate.source,
                    "symbol": candidate.symbol,
                    "label": candidate.label,
                    "enabled": candidate.enabled,
                    "selected": candidate.symbol == selected,
                }
                for candidate in options
            ],
        }

    def set_domestic_route_preference(self, group_name: str, symbol: str | None) -> dict:
        linked_groups = self.linked_variant_groups(group_name)
        pair = self.context.pair_map[group_name]
        options = self.all_domestic_candidates(pair)
        if symbol in (None, "", "__auto__"):
            for linked_group in linked_groups:
                linked_pair = self.context.pair_map[linked_group]
                self.context.preferred_domestic_symbols[linked_group] = linked_pair.domestic_symbol
                self.context.repository.delete_route_preference(linked_group, "domestic")
            return self.get_domestic_route_options(group_name)

        normalized = symbol.lower()
        matched = next((item for item in options if item.symbol.lower() == normalized), None)
        if matched is None:
            raise ValueError(f"{group_name} does not have domestic candidate {symbol}")
        self.context.preferred_domestic_symbols[group_name] = matched.symbol
        self.context.repository.upsert_route_preference(group_name, "domestic", matched.symbol)
        for linked_group in linked_groups:
            if linked_group == group_name:
                continue
            linked_pair = self.context.pair_map[linked_group]
            linked_options = self.all_domestic_candidates(linked_pair, refresh_dynamic=False)
            linked_match = next((item for item in linked_options if item.symbol.lower() == normalized), None)
            if linked_match is None:
                linked_options = self.all_domestic_candidates(linked_pair)
                linked_match = next((item for item in linked_options if item.symbol.lower() == normalized), None)
            if linked_match is not None:
                self.context.preferred_domestic_symbols[linked_group] = linked_match.symbol
                self.context.repository.upsert_route_preference(linked_group, "domestic", linked_match.symbol)
        return self.get_domestic_route_options(group_name)

    def get_overseas_route_options(self, group_name: str) -> dict:
        pair = self.context.pair_map[group_name]
        options = self.all_overseas_candidates(pair)
        selected = self.context.preferred_overseas_symbols.get(group_name, default_overseas_symbol(pair))
        options = prioritize_candidates(options, selected)
        return {
            "group_name": group_name,
            "selected_symbol": selected,
            "selected_label": next((item.label for item in options if item.symbol == selected), None),
            "options": [
                {
                    "source": candidate.source,
                    "symbol": candidate.symbol,
                    "label": candidate.label,
                    "enabled": candidate.enabled,
                    "selected": candidate.symbol == selected,
                }
                for candidate in options
            ],
        }

    def set_overseas_route_preference(self, group_name: str, symbol: str | None) -> dict:
        linked_groups = self.linked_variant_groups(group_name)
        pair = self.context.pair_map[group_name]
        options = self.all_overseas_candidates(pair)
        if symbol in (None, "", "__auto__"):
            for linked_group in linked_groups:
                linked_pair = self.context.pair_map[linked_group]
                selected_symbol = default_overseas_symbol(linked_pair)
                self.context.preferred_overseas_symbols[linked_group] = selected_symbol
                self.context.repository.delete_route_preference(linked_group, "overseas")
            return self.get_overseas_route_options(group_name)

        normalized = symbol.lower()
        matched = next((item for item in options if item.symbol.lower() == normalized), None)
        if matched is None:
            raise ValueError(f"{group_name} does not have overseas candidate {symbol}")
        self.context.preferred_overseas_symbols[group_name] = matched.symbol
        self.context.repository.upsert_route_preference(group_name, "overseas", matched.symbol)
        for linked_group in linked_groups:
            if linked_group == group_name:
                continue
            linked_pair = self.context.pair_map[linked_group]
            linked_options = self.all_overseas_candidates(linked_pair)
            linked_match = next((item for item in linked_options if item.symbol.lower() == normalized), None)
            if linked_match is not None:
                self.context.preferred_overseas_symbols[linked_group] = linked_match.symbol
                self.context.repository.upsert_route_preference(linked_group, "overseas", linked_match.symbol)
        return self.get_overseas_route_options(group_name)

    def domestic_candidates(self, pair: PairConfig) -> list[QuoteRouteConfig]:
        candidates = self.all_domestic_candidates(pair)
        return prioritize_candidates(candidates, self.context.preferred_domestic_symbols.get(pair.group_name))

    def all_domestic_candidates(
        self,
        pair: PairConfig,
        *,
        refresh_dynamic: bool = True,
    ) -> list[QuoteRouteConfig]:
        del refresh_dynamic
        candidates: list[QuoteRouteConfig] = []
        if pair.domestic_candidates:
            candidates.extend(pair.domestic_candidates)
        else:
            candidates.append(
                QuoteRouteConfig(
                    source=pair.domestic_source,
                    symbol=pair.domestic_symbol,
                    label=pair.domestic_label,
                )
            )
        return dedupe_candidates(candidates)

    def overseas_candidates(self, pair: PairConfig) -> list[QuoteRouteConfig]:
        candidates = self.all_overseas_candidates(pair)
        return prioritize_candidates(candidates, self.context.preferred_overseas_symbols.get(pair.group_name))

    def all_overseas_candidates(self, pair: PairConfig) -> list[QuoteRouteConfig]:
        if pair.overseas_candidates:
            return dedupe_candidates(pair.overseas_candidates)
        return [
            QuoteRouteConfig(
                source=pair.overseas_source,
                symbol=pair.overseas_symbol,
                label=pair.overseas_label,
            )
        ]

    def linked_variant_groups(self, group_name: str) -> list[str]:
        base_name = variant_group_base(group_name)
        linked_groups = [
            pair.group_name
            for pair in self.context.enabled_pairs
            if variant_group_base(pair.group_name) == base_name
        ]
        return linked_groups or [group_name]
