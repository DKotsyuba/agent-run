"""Validated, in-memory joins of persisted route topology and forecasts."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Mapping

from agent_run.errors import ValidationError
from agent_run.state import StateStore

from .forecast import CapacityForecast, build_forecasts
from .history import CapacityKey, load_series
from .topology import (
    CapacityRouteDescriptor,
    CapacityTopology,
    PhysicalPoolDescriptor,
    validate_topology,
)


@dataclass(frozen=True)
class CapacityRoute:
    """One routable descriptor with its exact pools and known forecasts."""

    descriptor: CapacityRouteDescriptor
    pools: tuple[PhysicalPoolDescriptor, ...]
    forecasts: tuple[CapacityForecast, ...]


@dataclass(frozen=True)
class CapacityRouteEvidence:
    """A scope-local reason why persisted topology is not routable."""

    runtime: str
    scope_id: str
    reason: str
    detail: str


@dataclass(frozen=True)
class CapacityRouteSnapshot:
    """Immutable routes and bounded evidence produced for one observation time.

    ``routes`` contains only fresh, validated routes whose every exact pool key
    has a known forecast. ``deferred`` records malformed, conflicting, or
    missing-forecast topology. ``unavailable`` records expired topology and
    unknown forecasts. No ranking or scoring is performed here.
    """

    routes: tuple[CapacityRoute, ...]
    deferred: tuple[CapacityRouteEvidence, ...]
    unavailable: tuple[CapacityRouteEvidence, ...]


def _text(value: object, name: str) -> str:
    """Validate one required opaque string field from untrusted JSON."""

    if not isinstance(value, str) or not value:
        raise ValidationError(f"{name} must be a non-empty string")
    return value


def _key(value: object) -> CapacityKey:
    """Parse one persisted exact capacity key without widening its identity."""

    if not isinstance(value, dict):
        raise ValidationError("capacity key must be an object")
    target = value.get("target")
    if target is not None:
        target = _text(target, "target")
    return CapacityKey(
        _text(value.get("runtime"), "key runtime"),
        _text(value.get("lane"), "key lane"),
        _text(value.get("window"), "key window"),
        target,
        _text(value.get("source"), "key source"),
    )


def _topology(payload: object) -> CapacityTopology:
    """Parse and validate one persisted topology payload atomically."""

    if not isinstance(payload, dict):
        raise ValidationError("route payload must be an object")
    pools_raw, routes_raw = payload.get("pools"), payload.get("routes")
    if not isinstance(pools_raw, list) or not isinstance(routes_raw, list):
        raise ValidationError("route payload must contain pools and routes lists")
    pools = []
    for raw in pools_raw:
        if not isinstance(raw, dict):
            raise ValidationError("pool must be an object")
        keys = raw.get("keys")
        if not isinstance(keys, list):
            raise ValidationError("pool keys must be a list")
        pools.append(PhysicalPoolDescriptor(_text(raw.get("pool_id"), "pool_id"), frozenset(_key(item) for item in keys)))
    routes = []
    for raw in routes_raw:
        if not isinstance(raw, dict):
            raise ValidationError("route must be an object")
        account = raw.get("account")
        if account is not None:
            account = _text(account, "account")
        pool_ids = raw.get("pool_ids")
        if not isinstance(pool_ids, list):
            raise ValidationError("route pool_ids must be a list")
        routes.append(CapacityRouteDescriptor(
            _text(raw.get("route_id"), "route_id"),
            _text(raw.get("runtime"), "runtime"),
            account,
            _text(raw.get("quota_lane"), "quota_lane"),
            tuple(_text(item, "pool reference") for item in pool_ids),
        ))
    return validate_topology(pools, routes)


def _row_payload(row: Mapping[str, object]) -> CapacityTopology:
    """Decode a state row payload and return its validated topology."""

    try:
        payload = row["payload_json"]
        if not isinstance(payload, str):
            raise ValidationError("route payload JSON must be text")
        return _topology(json.loads(payload))
    except (KeyError, TypeError, ValueError, ValidationError) as error:
        raise ValidationError("malformed persisted route snapshot") from error


def build_capacity_routes(
    store: StateStore, *, retention: int, now: float
) -> CapacityRouteSnapshot:
    """Build fresh routable routes by exact-key forecast join.

    ``store`` is read on its owning thread; ``retention`` is the positive
    history bound passed to :func:`load_series`, and ``now`` is one finite
    epoch timestamp used for every freshness decision. Persisted topology is
    never inferred from legacy target values. Invalid input raises
    :class:`ValidationError`; bad individual scopes become bounded evidence.
    """

    if not isinstance(store, StateStore):
        raise ValidationError("store must be a StateStore")
    if not isinstance(retention, int) or isinstance(retention, bool) or retention <= 0:
        raise ValidationError("retention must be positive")
    if not isinstance(now, (int, float)) or isinstance(now, bool) or not math.isfinite(now):
        raise ValidationError("now must be finite")
    forecasts = {item.key: item for item in build_forecasts(load_series(store, retention=retention), now=now)}
    routes: list[CapacityRoute] = []
    deferred: list[CapacityRouteEvidence] = []
    unavailable: list[CapacityRouteEvidence] = []
    pool_definitions: dict[tuple[str, str], list[tuple[str, PhysicalPoolDescriptor]]] = {}
    route_definitions: dict[tuple[str, str], list[tuple[str, CapacityRouteDescriptor]]] = {}
    fresh: list[tuple[str, str, CapacityTopology]] = []
    for row in store.capacity_route_snapshots():
        runtime, scope = str(row["runtime"]), str(row["scope_id"])
        def evidence(reason: str, detail: str) -> CapacityRouteEvidence:
            """Create evidence attached to the current persisted scope."""

            return CapacityRouteEvidence(runtime, scope, reason, detail)
        try:
            observed, valid_until = row["observed_at"], row["valid_until"]
            if (
                not isinstance(observed, (int, float))
                or isinstance(observed, bool)
                or not math.isfinite(observed)
                or not isinstance(valid_until, (int, float))
                or isinstance(valid_until, bool)
                or not math.isfinite(valid_until)
                or valid_until < observed
            ):
                raise ValidationError("snapshot timestamps are malformed")
            topology = _row_payload(row)
        except ValidationError as error:
            deferred.append(evidence("malformed", str(error)))
            continue
        if not observed <= now <= valid_until:
            unavailable.append(evidence("expired", "topology snapshot is not fresh"))
            continue
        if any(key.runtime != runtime for pool in topology.pools for key in pool.keys) or any(descriptor.runtime != runtime for descriptor in topology.routes):
            deferred.append(evidence("malformed", "topology runtime differs from row runtime"))
            continue
        fresh.append((runtime, scope, topology))
    for runtime, scope, topology in fresh:
        for pool in topology.pools:
            pool_definitions.setdefault((runtime, pool.pool_id), []).append((scope, pool))
        for descriptor in topology.routes:
            route_definitions.setdefault((runtime, descriptor.route_id), []).append((scope, descriptor))
    conflicted_pools = {identity for identity, values in pool_definitions.items() if len({pool for _, pool in values}) != 1}
    conflicted_routes = {identity for identity, values in route_definitions.items() if len({route for _, route in values}) != 1}
    pools = {identity: values[0][1] for identity, values in pool_definitions.items() if identity not in conflicted_pools}
    for (runtime, route_id), values in sorted(route_definitions.items()):
        if (runtime, route_id) in conflicted_routes:
            for scope, _ in values:
                deferred.append(CapacityRouteEvidence(runtime, scope, "conflict", "conflicting route definition"))
            continue
        descriptor = values[0][1]
        if any((runtime, pool_id) in conflicted_pools or (runtime, pool_id) not in pools for pool_id in descriptor.pool_ids):
            for scope, _ in values:
                deferred.append(CapacityRouteEvidence(runtime, scope, "conflict", "route references conflicted pool"))
            continue
        route_pools = tuple(pools[(runtime, pool_id)] for pool_id in descriptor.pool_ids)
        keys = tuple(sorted((key for pool in route_pools for key in pool.keys), key=lambda item: (item.runtime, item.lane, item.window, item.target or "", item.source)))
        matching = tuple(forecasts[key] for key in keys if key in forecasts)
        missing = tuple(key for key in keys if key not in forecasts)
        if missing:
            for scope, _ in values:
                deferred.append(CapacityRouteEvidence(runtime, scope, "missing_forecast", f"route {route_id} has no exact forecast"))
        elif any(not item.known for item in matching):
            for scope, _ in values:
                unavailable.append(CapacityRouteEvidence(runtime, scope, "unknown_forecast", f"route {route_id} has an unknown forecast"))
        else:
            routes.append(CapacityRoute(descriptor, route_pools, matching))
    key = lambda item: (item.runtime, item.scope_id, item.reason, item.detail)
    return CapacityRouteSnapshot(tuple(routes), tuple(sorted(deferred, key=key)), tuple(sorted(unavailable, key=key)))
