"""Strict normalizer for Codex app-server capacity responses."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Mapping

from ..adapters.base import LimitSample
from ..errors import ValidationError
from .topology import (
    CapacityKey,
    CapacityRouteDescriptor,
    CapacityTopology,
    PhysicalPoolDescriptor,
    account_token,
    validate_topology,
)


_SOURCE = "codex_appserver"
_VALID_FOR_SECONDS = 900


def _number(value: object) -> float | None:
    """Return a finite non-boolean numeric value, or ``None`` when malformed."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _window_name(minutes: float) -> str:
    """Convert a valid window duration in minutes into its stable public name."""

    if minutes == 300:
        return "five_hour"
    if minutes == 10080:
        return "seven_day"
    return f"min{int(minutes) if minutes.is_integer() else minutes:g}"


def normalize_rate_limits(
    runtime: str,
    target: str | None,
    response: Mapping[str, object],
    observed_at: float,
) -> tuple[tuple[LimitSample, ...], CapacityTopology, str | None]:
    """Normalize one app-server result into samples, topology, and account identity.

    runtime is the configured runtime name and target is its account label, or
    None for the base home. response is normally the direct result returned by
    ProcessTransport; a JSON-RPC-style result wrapper is accepted for captured
    fixtures. Multi-bucket rateLimitsByLimitId is preferred, while legacy
    rateLimits is wrapped as one bucket. Only finite 0--100 percentages with
    positive finite window durations are emitted. Missing reset timestamps
    remain None. Malformed present windows disable their whole bucket route;
    valid samples still remain available as advisory evidence.

    Each bucket becomes one account-namespaced physical pool and route. Stable
    provider limitId values identify samples and pools; limitName is display-only
    quota_lane metadata. Evidence remains fresh for the bounded source interval,
    never until the provider reset. The returned backend account id is ephemeral
    deduplication data and callers must neither persist nor log it. Malformed
    envelopes raise ValidationError; absent/null windows represent no quota,
    whereas malformed present windows are never treated as missing constraints.
    """

    wrapped = response.get("result")
    result = wrapped if isinstance(wrapped, Mapping) else response
    buckets_obj = result.get("rateLimitsByLimitId")
    if buckets_obj is None:
        legacy = result.get("rateLimits")
        if isinstance(legacy, Mapping):
            legacy_id = legacy.get("limitId")
            bucket_id = legacy_id if isinstance(legacy_id, str) and legacy_id else "codex"
            buckets_obj = {bucket_id: legacy}
    if not isinstance(buckets_obj, Mapping):
        raise ValidationError("codex app-server rate limits must be a mapping")

    account_id = result.get("accountId")
    if not isinstance(account_id, str) or not account_id:
        account_id = None
    observed = _number(observed_at)
    if observed is None or observed < 0:
        raise ValidationError("observed_at must be a finite nonnegative epoch timestamp")
    try:
        observed_datetime = datetime.fromtimestamp(observed, timezone.utc)
    except (OverflowError, OSError, ValueError) as error:
        raise ValidationError("observed_at is outside the supported epoch range") from error

    samples: list[LimitSample] = []
    pools: list[PhysicalPoolDescriptor] = []
    routes: list[CapacityRouteDescriptor] = []
    scope = account_token(target, absent_token="base")
    for limit_id, bucket in buckets_obj.items():
        if not isinstance(limit_id, str) or not limit_id or not isinstance(bucket, Mapping):
            continue
        display_name = bucket.get("limitName")
        quota_lane = display_name if isinstance(display_name, str) and display_name else limit_id
        keys: set[CapacityKey] = set()
        complete = True
        for window in (bucket.get("primary"), bucket.get("secondary")):
            if window is None:
                continue
            if not isinstance(window, Mapping):
                complete = False
                continue
            used = _number(window.get("usedPercent"))
            minutes = _number(window.get("windowDurationMins"))
            reset_value = window.get("resetsAt")
            reset = _number(reset_value)
            if (
                used is None
                or not 0 <= used <= 100
                or minutes is None
                or minutes <= 0
                or (reset_value is not None and (reset is None or reset < 0))
            ):
                complete = False
                continue
            reset_datetime = None
            if reset is not None:
                try:
                    reset_datetime = datetime.fromtimestamp(reset, timezone.utc)
                except (OverflowError, OSError, ValueError):
                    complete = False
                    continue
            window_name = _window_name(minutes)
            samples.append(
                LimitSample(
                    lane=limit_id,
                    window=window_name,
                    remaining_percent=100.0 - used,
                    reset_at=reset_datetime,
                    observed_at=observed_datetime,
                    source=_SOURCE,
                    target=target,
                    valid_for_seconds=_VALID_FOR_SECONDS,
                )
            )
            keys.add(CapacityKey(runtime, limit_id, window_name, target, _SOURCE))
        if not keys or not complete:
            continue
        pool_id = f"{runtime}:{scope}:{limit_id}"
        pools.append(PhysicalPoolDescriptor(pool_id, frozenset(keys)))
        routes.append(
            CapacityRouteDescriptor(
                route_id=pool_id,
                runtime=runtime,
                account=target,
                quota_lane=quota_lane,
                pool_ids=(pool_id,),
            )
        )
    return tuple(samples), validate_topology(pools, routes), account_id
