"""Pure, provider-neutral ordering of validated capacity routes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import math

from ..errors import ValidationError
from .forecast import CapacityForecast
from .history import CapacityKey
from .snapshot import CapacityRoute, CapacityRouteEvidence, CapacityRouteSnapshot
from .topology import CapacityRouteDescriptor


@dataclass(frozen=True)
class CapacityWindowExplanation:
    """Scoring evidence for one exact governing quota window.

    The numeric fields preserve the forecast inputs and derived reserve used by
    the ranker. ``marker`` is ``projected`` for reliable burn evidence or one
    of ``warmup``, ``thin_evidence``, and ``no_reset`` for the centered
    remaining-percent fallback.
    """

    key: CapacityKey
    remaining_percent: float
    burn_percent_per_hour: float | None
    burn_span_seconds: float | None
    reset_at: float | None
    projected_percent: float | None
    slack: float
    marker: str
    risk: str


@dataclass(frozen=True)
class CapacityOrderEvidence:
    """Explain why one scope or route was deferred from the working order.

    ``scope_id`` is present for collection/snapshot evidence. ``route_id`` is
    present for a defensive ranker rejection; exactly one may be absent.
    ``reason`` is a stable machine-readable label and ``detail`` is bounded
    diagnostic prose containing no provider payload.
    """

    runtime: str
    scope_id: str | None
    route_id: str | None
    reason: str
    detail: str


@dataclass(frozen=True)
class RankedCapacityRoute:
    """One physical capacity choice in descending usage priority.

    ``aliases`` retains every concrete launch descriptor sharing the same
    runtime and physical pool set; descriptors are ordered by descending
    effective factor, then by ``route_id``. ``score`` is the unmultiplied
    value in ``[0, 2]`` and
    ``priority`` equals ``score * multiplier``. ``limiting_key`` and
    ``limiting_reset_at`` belong to the window that produced the minimum
    slack, not merely the earliest reset in the route.
    """

    runtime: str
    aliases: tuple[CapacityRouteDescriptor, ...]
    pool_ids: tuple[str, ...]
    score: float
    multiplier: float
    priority: float
    minimum_remaining_percent: float
    limiting_key: CapacityKey
    limiting_reset_at: float | None
    windows: tuple[CapacityWindowExplanation, ...]


@dataclass(frozen=True)
class OmittedCapacityRoute:
    """One exhausted physical capacity choice excluded before scoring.

    Aliases are collapsed exactly as for a ranked route. ``limiting_key`` is
    the deterministic exhausted governing window and ``reason`` is currently
    the stable label ``exhausted``.
    """

    runtime: str
    aliases: tuple[CapacityRouteDescriptor, ...]
    pool_ids: tuple[str, ...]
    limiting_key: CapacityKey
    limiting_reset_at: float | None
    reason: str


@dataclass(frozen=True)
class CapacityOrder:
    """Role-independent ordered routes plus complete exclusion evidence.

    ``observed_at`` is the single injected ranking epoch. ``routes`` is a
    deterministic descending order, ``deferred`` retains snapshot and ranker
    evidence, ``omitted`` contains exhausted choices, and
    ``unavailable_runtimes`` names runtimes with evidence but no ranked or
    exhausted fresh choice.
    """

    observed_at: float
    routes: tuple[RankedCapacityRoute, ...]
    deferred: tuple[CapacityOrderEvidence, ...]
    omitted: tuple[OmittedCapacityRoute, ...]
    unavailable_runtimes: tuple[str, ...]
    insufficient_diversity: bool


def _finite(value: object, name: str) -> float:
    """Return a finite non-boolean number or raise ``ValidationError``.

    ``name`` identifies the rejected field. Integers and floats are accepted;
    booleans, infinities, NaN, and all other values are rejected.
    """

    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
    ):
        raise ValidationError(f"{name} must be finite")
    return float(value)


def _optional_nonnegative(value: object, name: str) -> float | None:
    """Validate an optional finite nonnegative forecast field.

    ``None`` remains absent. Numeric values are returned as floats; negative,
    boolean, non-finite, or non-numeric values raise ``ValidationError``.
    """

    if value is None:
        return None
    number = _finite(value, name)
    if number < 0:
        raise ValidationError(f"{name} must be nonnegative")
    return number


def _window(
    forecast: CapacityForecast, now: float
) -> CapacityWindowExplanation | None:
    """Convert one forecast to conservative scoring evidence.

    Unknown, malformed, future-observed, or already-reset forecasts return
    ``None`` so the caller defers their route. Reliable projection requires a
    nonnegative burn rate, at least one hour of evidence, and a future reset;
    all other known evidence uses the centered remaining-percent fallback.
    """

    if not isinstance(forecast, CapacityForecast) or not forecast.known:
        return None
    try:
        remaining = _finite(forecast.remaining_percent, "remaining_percent")
        if not 0 <= remaining <= 100:
            raise ValidationError("remaining_percent must be between 0 and 100")
        observed = _finite(forecast.observed_at, "observed_at")
        if observed > now:
            raise ValidationError("observed_at must not be in the future")
        burn = _optional_nonnegative(
            forecast.burn_percent_per_hour, "burn_percent_per_hour"
        )
        span = _optional_nonnegative(
            forecast.burn_span_seconds, "burn_span_seconds"
        )
        reset = _optional_nonnegative(forecast.reset_at, "reset_at")
        if reset is not None and reset <= now:
            raise ValidationError("reset_at must be in the future")
        if not isinstance(forecast.key, CapacityKey):
            raise ValidationError("forecast key must be a CapacityKey")
        if not isinstance(forecast.warmup, bool) or not isinstance(forecast.risk, str):
            raise ValidationError("forecast metadata is malformed")
    except ValidationError:
        return None

    if burn is not None and span is not None and span >= 3600 and reset is not None:
        projected = remaining - burn * ((reset - now) / 3600)
        slack = max(-1.0, min(1.0, projected / 100))
        marker = "projected"
    else:
        projected = None
        slack = max(-1.0, min(1.0, 2 * remaining / 100 - 1))
        if reset is None:
            marker = "no_reset"
        elif burn is None or forecast.warmup:
            marker = "warmup"
        else:
            marker = "thin_evidence"
    return CapacityWindowExplanation(
        forecast.key,
        remaining,
        burn,
        span,
        reset,
        projected,
        slack,
        marker,
        forecast.risk,
    )


def _key_order(key: CapacityKey) -> tuple[str, str, str, str, str]:
    """Return the total deterministic ordering key for a quota identity."""

    return (key.runtime, key.lane, key.window, key.target or "", key.source)


def _evidence_order(
    item: CapacityOrderEvidence,
) -> tuple[str, str, str, str, str]:
    """Return the total deterministic ordering key for deferred evidence."""

    return (
        item.runtime,
        item.scope_id or "",
        item.route_id or "",
        item.reason,
        item.detail,
    )


def _snapshot_evidence(item: CapacityRouteEvidence) -> CapacityOrderEvidence:
    """Convert scope-level snapshot evidence to the public order shape."""

    return CapacityOrderEvidence(
        item.runtime, item.scope_id, None, item.reason, item.detail
    )


def rank_capacity_routes(
    snapshot: CapacityRouteSnapshot,
    multipliers: Mapping[str, float],
    *,
    now: float,
    route_multipliers: Mapping[tuple[str, str], float] | None = None,
) -> CapacityOrder:
    """Rank capacity routes without provider calls, role selection, or writes.

    ``snapshot`` must contain validated fresh routes. ``multipliers`` maps
    opaque runtime names to positive finite factors and defaults missing names
    to ``1.0``. ``now`` is one finite nonnegative epoch used for every
    projection. Unknown or malformed evidence is deferred; any zero governing
    window is omitted before scoring. The result is a total deterministic
    order independent of input iteration order.
    ``route_multipliers`` optionally maps ``(runtime, route_id)`` pairs to
    absolute effective factors, preventing same-named route ids from crossing
    runtime boundaries. Aliases sharing runtime and pool ids collapse using
    their maximum factor; the canonical alias is ordered by factor descending
    then route id.
    Exhausted choices are never revived by a multiplier. Invalid arguments
    raise ``ValidationError``.
    """

    if not isinstance(snapshot, CapacityRouteSnapshot):
        raise ValidationError("snapshot must be a CapacityRouteSnapshot")
    if not isinstance(multipliers, Mapping):
        raise ValidationError("multipliers must be a mapping")
    ranked_at = _finite(now, "now")
    if ranked_at < 0:
        raise ValidationError("now must be nonnegative")
    checked: dict[str, float] = {}
    for runtime, multiplier in multipliers.items():
        if not isinstance(runtime, str) or not runtime.strip():
            raise ValidationError("multiplier runtime must be nonblank")
        value = _finite(multiplier, "multiplier")
        if value <= 0:
            raise ValidationError("multiplier must be positive")
        checked[runtime] = value
    checked_routes: dict[tuple[str, str], float] = {}
    if route_multipliers is not None:
        if not isinstance(route_multipliers, Mapping):
            raise ValidationError("route_multipliers must be a mapping")
        for route_key, multiplier in route_multipliers.items():
            if (
                not isinstance(route_key, tuple)
                or len(route_key) != 2
                or any(not isinstance(part, str) or not part.strip() for part in route_key)
            ):
                raise ValidationError("route multiplier keys must be (runtime, route_id) tuples")
            value = _finite(multiplier, "route multiplier")
            if value <= 0:
                raise ValidationError("route multiplier must be positive")
            checked_routes[route_key] = value

    deferred = [
        _snapshot_evidence(item)
        for item in (*snapshot.deferred, *snapshot.unavailable)
    ]
    omitted: list[OmittedCapacityRoute] = []
    grouped: dict[tuple[str, tuple[str, ...]], list[CapacityRoute]] = {}
    seen_runtimes = {item.runtime for item in deferred}
    for route in snapshot.routes:
        if not isinstance(route, CapacityRoute):
            raise ValidationError("snapshot routes must be CapacityRoute instances")
        runtime = route.descriptor.runtime
        pool_ids = tuple(sorted(pool.pool_id for pool in route.pools))
        grouped.setdefault((runtime, pool_ids), []).append(route)
        seen_runtimes.add(runtime)

    ranked: list[RankedCapacityRoute] = []
    available_runtimes: set[str] = set()
    for (runtime, pool_ids), values in sorted(grouped.items()):
        aliases = tuple({value.descriptor.route_id: value.descriptor for value in values}.values())
        aliases = tuple(sorted(aliases, key=lambda descriptor: (
            -checked_routes.get((runtime, descriptor.route_id), checked.get(runtime, 1.0)),
            descriptor.route_id,
        )))
        canonical = min(values, key=lambda value: value.descriptor.route_id)
        windows: list[CapacityWindowExplanation] = []
        malformed = False
        for forecast in canonical.forecasts:
            explanation = _window(forecast, ranked_at)
            if explanation is None:
                malformed = True
                break
            windows.append(explanation)
        windows.sort(key=lambda item: _key_order(item.key))
        if malformed or not windows:
            deferred.append(
                CapacityOrderEvidence(
                    runtime,
                    None,
                    aliases[0].route_id,
                    "ranker_unknown_forecast",
                    "forecast is unknown, stale, or malformed",
                )
            )
            continue

        exhausted = [item for item in windows if item.remaining_percent == 0]
        if exhausted:
            limiting = min(
                exhausted,
                key=lambda item: (
                    item.reset_at is None,
                    item.reset_at or math.inf,
                    _key_order(item.key),
                ),
            )
            omitted.append(
                OmittedCapacityRoute(
                    runtime,
                    aliases,
                    pool_ids,
                    limiting.key,
                    limiting.reset_at,
                    "exhausted",
                )
            )
            available_runtimes.add(runtime)
            continue

        limiting = min(
            windows,
            key=lambda item: (
                item.slack,
                item.reset_at is None,
                item.reset_at or math.inf,
                _key_order(item.key),
            ),
        )
        score = 1.0 + limiting.slack
        multiplier = max(
            (
                checked_routes.get((runtime, alias.route_id), checked.get(runtime, 1.0))
                for alias in aliases
            ),
            default=checked.get(runtime, 1.0),
        )
        ranked.append(
            RankedCapacityRoute(
                runtime,
                aliases,
                pool_ids,
                score,
                multiplier,
                score * multiplier,
                min(item.remaining_percent for item in windows),
                limiting.key,
                limiting.reset_at,
                tuple(windows),
            )
        )
        available_runtimes.add(runtime)

    ranked.sort(
        key=lambda item: (
            -item.priority,
            -item.score,
            -item.minimum_remaining_percent,
            item.limiting_reset_at is None,
            item.limiting_reset_at or math.inf,
            item.aliases[0].route_id,
        )
    )
    omitted.sort(
        key=lambda item: (item.runtime, item.aliases[0].route_id, _key_order(item.limiting_key))
    )
    deferred = sorted(set(deferred), key=_evidence_order)
    unavailable = tuple(sorted(seen_runtimes - available_runtimes))
    return CapacityOrder(
        ranked_at,
        tuple(ranked),
        tuple(deferred),
        tuple(omitted),
        unavailable,
        len(ranked) < 2,
    )
