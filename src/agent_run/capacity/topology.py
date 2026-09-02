"""Provider-neutral physical-pool and route topology over collected samples.

A *physical pool* is one real quota reservoir identified by an exact set of
:class:`~agent_run.capacity.history.CapacityKey` identities. A *route* is one
launchable way to consume quota: a runtime, optionally a configured account,
and a quota lane, referencing one or more physical pools by id. Several routes
may reference the same pool — that is the explicit statement that they share
one reservoir, so its capacity is never multiplied.

This module is deliberately free of provider, runtime, and account literals
and never inspects or parses ``CapacityKey.target``: the mapping from a
provider's raw samples to pools and routes belongs to the source-specific
normalizers in :mod:`agent_run.capacity.sources`. Validation here checks only
structure — non-empty identifiers, exact non-empty key sets, pool-reference
integrity — and canonicalizes ordering so equal topologies compare equal
regardless of the order their parts were supplied in.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..adapters.base import LimitSample
from ..errors import ValidationError
from .history import CapacityKey


@dataclass(frozen=True)
class PhysicalPoolDescriptor:
    """One physical quota reservoir.

    ``pool_id`` is a non-empty stable identifier unique within its topology.
    ``keys`` is the exact, non-empty set of :class:`CapacityKey` sample
    identities whose measurements describe this pool — never a prefix or
    pattern. Two descriptors with different ``pool_id`` values are distinct
    pools even when their key sets overlap.
    """

    pool_id: str
    keys: frozenset[CapacityKey]


@dataclass(frozen=True)
class CapacityRouteDescriptor:
    """One launchable way to consume quota.

    ``route_id`` is a non-empty identifier unique within its topology.
    ``runtime`` names the runtime to launch. ``account`` is the configured
    account label to launch as, or ``None`` for the default account. A
    discovered-but-unconfigured account never becomes a route. ``quota_lane``
    is the lane this route spends from. ``pool_ids`` is the non-empty,
    duplicate-free tuple of physical pools the route draws on, each of which
    must exist in the same topology.
    """

    route_id: str
    runtime: str
    account: str | None
    quota_lane: str
    pool_ids: tuple[str, ...]


@dataclass(frozen=True)
class CapacityTopology:
    """The validated, canonically ordered set of pools and routes.

    ``pools`` is sorted by ``pool_id`` and ``routes`` by ``route_id``; use
    :func:`validate_topology` rather than constructing this directly so those
    invariants and reference integrity actually hold.
    """

    pools: tuple[PhysicalPoolDescriptor, ...]
    routes: tuple[CapacityRouteDescriptor, ...]


@dataclass(frozen=True)
class CapacityCollectionSlice:
    """One atomic collection round for a runtime and opaque collection scope.

    ``runtime`` identifies the engine whose capacity keys are collected, and
    ``scope_id`` independently identifies the opaque account or collection
    scope. Every element of ``samples`` is a :class:`LimitSample` exactly as
    collected. ``observed_at``/``valid_until`` are epoch seconds and bound the
    slice as a whole; ``valid_until`` is never earlier than ``observed_at``.
    Construct through :func:`validate_slice` so the whole slice is checked
    before anything may persist it.
    """

    runtime: str
    scope_id: str
    samples: tuple[LimitSample, ...]
    topology: CapacityTopology
    observed_at: float
    valid_until: float


def _required_text(name: str, value: object) -> str:
    """Return ``value`` as a non-empty string or raise ``ValidationError``.

    ``name`` labels the field in the error message. Any non-string value,
    including ``None`` and booleans, is rejected; so is the empty string.
    """

    if not isinstance(value, str) or not value:
        raise ValidationError(f"{name} must be a non-empty string")
    return value


def _validated_key(key: object) -> CapacityKey:
    """Check one pool member is a well-formed :class:`CapacityKey`.

    ``runtime``, ``lane``, ``window``, and ``source`` must be non-empty
    strings; ``target`` is either ``None`` or a non-empty string. The key is
    returned unchanged — its value is never interpreted here.
    """

    if not isinstance(key, CapacityKey):
        raise ValidationError("pool keys must be CapacityKey instances")
    for name in ("runtime", "lane", "window", "source"):
        _required_text(f"capacity key {name}", getattr(key, name))
    if key.target is not None:
        _required_text("capacity key target", key.target)
    return key


def validate_topology(
    pools: Iterable[PhysicalPoolDescriptor],
    routes: Iterable[CapacityRouteDescriptor],
) -> CapacityTopology:
    """Validate pools and routes together and return the canonical topology.

    Structural rules: unique non-empty pool and route ids; each pool carries
    an exact non-empty key set; each route names at least one existing pool
    exactly once. The returned topology lists pools sorted by ``pool_id`` and
    routes sorted by ``route_id``, so input order never leaks into identity:
    the same parts in any order validate to equal topologies. Raises
    ``ValidationError`` naming the first violated rule; in that case nothing
    is returned, which is what makes whole-slice validation atomic.
    """

    checked_pools: dict[str, PhysicalPoolDescriptor] = {}
    for pool in pools:
        if not isinstance(pool, PhysicalPoolDescriptor):
            raise ValidationError("pools must be PhysicalPoolDescriptor instances")
        pool_id = _required_text("pool_id", pool.pool_id)
        keys = frozenset(_validated_key(key) for key in pool.keys)
        if not keys:
            raise ValidationError(f"pool {pool_id} must name a non-empty key set")
        if pool_id in checked_pools:
            raise ValidationError(f"duplicate pool_id {pool_id}")
        checked_pools[pool_id] = PhysicalPoolDescriptor(pool_id, keys)

    checked_routes: dict[str, CapacityRouteDescriptor] = {}
    for route in routes:
        if not isinstance(route, CapacityRouteDescriptor):
            raise ValidationError("routes must be CapacityRouteDescriptor instances")
        route_id = _required_text("route_id", route.route_id)
        _required_text("runtime", route.runtime)
        _required_text("quota_lane", route.quota_lane)
        account = None if route.account is None else _required_text("account", route.account)
        pool_ids = tuple(route.pool_ids)
        if not pool_ids:
            raise ValidationError(f"route {route_id} must reference at least one pool")
        seen: set[str] = set()
        for pool_id in pool_ids:
            _required_text("pool reference", pool_id)
            if pool_id in seen:
                raise ValidationError(f"route {route_id} references pool {pool_id} twice")
            seen.add(pool_id)
            if pool_id not in checked_pools:
                raise ValidationError(f"route {route_id} references unknown pool {pool_id}")
        if route_id in checked_routes:
            raise ValidationError(f"duplicate route_id {route_id}")
        checked_routes[route_id] = CapacityRouteDescriptor(
            route_id, route.runtime, account, route.quota_lane, pool_ids
        )

    return CapacityTopology(
        tuple(checked_pools[pool_id] for pool_id in sorted(checked_pools)),
        tuple(checked_routes[route_id] for route_id in sorted(checked_routes)),
    )


def _finite_epoch(name: str, value: object) -> float:
    """Return ``value`` as a finite epoch float or raise ``ValidationError``.

    Rejects booleans, non-numbers, NaN, and infinities; integers are widened
    to floats so equal timestamps compare equal regardless of origin type.
    """

    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value != value
        or value in (float("inf"), float("-inf"))
    ):
        raise ValidationError(f"{name} must be a finite epoch timestamp")
    return float(value)


def validate_slice(
    runtime: str,
    scope_id: str,
    samples: Iterable[LimitSample],
    topology: object,
    observed_at: float,
    valid_until: float,
) -> CapacityCollectionSlice:
    """Validate an entire collection slice before anything may persist it.

    ``runtime`` identifies the engine and ``scope_id`` identifies the opaque
    collection scope; both must be non-empty strings. ``topology`` is
    untrusted runtime input and must be a
    :class:`CapacityTopology` produced by :func:`validate_topology`; every
    sample must be a :class:`LimitSample`. ``observed_at`` and
    ``valid_until`` are finite epoch seconds with
    ``valid_until >= observed_at``. Returns the immutable slice; raises
    ``ValidationError`` on the first violated rule, so callers either hold a
    fully valid slice or nothing.
    """

    checked_runtime = _required_text("runtime", runtime)
    checked_scope_id = _required_text("scope_id", scope_id)
    if not isinstance(topology, CapacityTopology):
        raise ValidationError("topology must be a validated CapacityTopology")
    checked_samples = tuple(samples)
    for sample in checked_samples:
        if not isinstance(sample, LimitSample):
            raise ValidationError("slice samples must be LimitSample instances")
    observed = _finite_epoch("observed_at", observed_at)
    valid = _finite_epoch("valid_until", valid_until)
    if valid < observed:
        raise ValidationError("valid_until must not precede observed_at")
    sample_keys = {
        CapacityKey(
            checked_runtime, sample.lane, sample.window, sample.target, sample.source
        )
        for sample in checked_samples
    }
    for pool in topology.pools:
        if set(pool.keys).difference(sample_keys):
            raise ValidationError(f"pool {pool.pool_id} names a key absent from the slice")
    return CapacityCollectionSlice(
        checked_runtime, checked_scope_id, checked_samples, topology, observed, valid
    )


def pools_from_samples(
    runtime: str, samples: Iterable[LimitSample]
) -> tuple[PhysicalPoolDescriptor, ...]:
    """Group samples into one physical pool per distinct sample identity.

    Pooling is by whole sample identity (lane, window, target, source): each
    distinct identity becomes one pool whose id is built from those fields,
    with ``shared`` marking a ``None`` target. Samples sharing an identity
    share one reservoir — the shared-target case — while any difference in
    identity keeps pools distinct. ``runtime`` names the engine for the ids
    and each pool's keys; it is not inspected beyond being
    a non-empty string. Input order does not affect the result: pools come
    back sorted by id. Raises ``ValidationError`` on a non-``LimitSample``
    element.
    """

    checked_runtime = _required_text("runtime", runtime)
    grouped: dict[tuple[str, str, str, str], set[CapacityKey]] = {}
    for sample in samples:
        if not isinstance(sample, LimitSample):
            raise ValidationError("samples must be LimitSample instances")
        identity = (
            sample.lane,
            sample.window,
            "shared" if sample.target is None else sample.target,
            sample.source,
        )
        grouped.setdefault(identity, set()).add(
            CapacityKey(
                checked_runtime, sample.lane, sample.window, sample.target, sample.source
            )
        )
    return tuple(
        PhysicalPoolDescriptor(
            f"{checked_runtime}:{':'.join(identity)}", frozenset(keys)
        )
        for identity, keys in sorted(grouped.items())
    )
