"""Shared construction of filtered, weighted capacity orders."""

from __future__ import annotations

from agent_run.config import Config
from agent_run.state.store import StateStore

from .ranking import CapacityOrder, rank_capacity_routes
from .snapshot import CapacityRouteSnapshot, build_capacity_routes


def build_capacity_order(store: StateStore, config: Config, *, now: float) -> CapacityOrder:
    """Build the enabled-runtime order from one persisted capacity snapshot.

    ``store`` is read on its owning thread and ``now`` is the single epoch used
    for freshness and ranking. Disabled runtimes and evidence are filtered;
    enabled runtimes without evidence remain unavailable. Account overrides
    take precedence over lane overrides, which take precedence over the
    runtime factor; account overrides match explicit descriptor labels only.
    A null account falls through to lane/runtime, not ``default_account``.
    Route keys include both runtime and route id. The builder
    performs no provider calls or writes and delegates malformed-input
    rejection.
    """

    enabled = {
        name: runtime for name, runtime in config.runtimes.items() if runtime.enabled
    }
    snapshot = build_capacity_routes(
        store, retention=config.capacity.sample_retention, now=now
    )
    filtered = CapacityRouteSnapshot(
        tuple(route for route in snapshot.routes if route.descriptor.runtime in enabled),
        tuple(item for item in snapshot.deferred if item.runtime in enabled),
        tuple(item for item in snapshot.unavailable if item.runtime in enabled),
    )
    runtime_multipliers = {
        name: runtime.priority_multiplier for name, runtime in enabled.items()
    }
    route_multipliers: dict[tuple[str, str], float] = {}
    for route in filtered.routes:
        runtime = enabled[route.descriptor.runtime]
        account_multiplier = (
            runtime.priority_account_multipliers.get(route.descriptor.account)
            if route.descriptor.account is not None
            else None
        )
        route_multipliers[(route.descriptor.runtime, route.descriptor.route_id)] = (
            account_multiplier
            if account_multiplier is not None
            else runtime.priority_lane_multipliers.get(
                route.descriptor.quota_lane, runtime.priority_multiplier
            )
        )
    order = rank_capacity_routes(
        filtered,
        runtime_multipliers,
        now=now,
        route_multipliers=route_multipliers,
    )
    evidenced = {route.descriptor.runtime for route in filtered.routes}
    evidenced |= {item.runtime for item in filtered.deferred}
    evidenced |= {item.runtime for item in filtered.unavailable}
    unavailable = tuple(
        sorted(set(order.unavailable_runtimes) | (set(enabled) - evidenced))
    )
    return CapacityOrder(
        order.observed_at,
        order.routes,
        order.deferred,
        order.omitted,
        unavailable,
        order.insufficient_diversity,
    )
