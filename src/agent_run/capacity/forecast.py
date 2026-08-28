"""Warmup, burn, sustainable pace, and risk per capacity identity.

Freshness comes from each sample's ``valid_until`` (per the architecture
contract); missing or stale data becomes ``unknown`` and never blocks a
caller. A sample whose ``reset_at`` has already passed is treated the same
way: the window it describes has already reset, so it can no longer speak
for the current window. Identity (runtime/lane/window/target/source) is
preserved end to end from :mod:`agent_run.capacity.history`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .history import CapacityKey, CapacitySeries, NormalizedSample


RISK_UNKNOWN = "unknown"
RISK_LOW = "low"
RISK_MEDIUM = "medium"
RISK_HIGH = "high"

_LOW_REMAINING_THRESHOLD = 30.0
_HIGH_REMAINING_THRESHOLD = 10.0
_BURN_MEDIUM_MULTIPLIER = 1.0
_BURN_HIGH_MULTIPLIER = 1.5


@dataclass(frozen=True)
class CapacityForecast:
    key: CapacityKey
    known: bool
    remaining_percent: float | None
    reset_at: float | None
    observed_at: float | None
    warmup: bool
    burn_percent_per_hour: float | None
    sustainable_percent_per_hour: float | None
    risk: str


def build_forecasts(
    series: Iterable[CapacitySeries], *, now: float
) -> tuple[CapacityForecast, ...]:
    return tuple(_forecast_one(item, now) for item in series)


def _is_fresh(sample: NormalizedSample, now: float) -> bool:
    valid = sample.valid_until is None or sample.valid_until >= now
    not_yet_reset = sample.reset_at is None or sample.reset_at >= now
    return valid and not_yet_reset


def _forecast_one(series: CapacitySeries, now: float) -> CapacityForecast:
    if (
        not series.samples
        or not _is_fresh(series.samples[0], now)
        or series.samples[0].remaining_percent is None
    ):
        return CapacityForecast(
            key=series.key,
            known=False,
            remaining_percent=None,
            reset_at=None,
            observed_at=None,
            warmup=True,
            burn_percent_per_hour=None,
            sustainable_percent_per_hour=None,
            risk=RISK_UNKNOWN,
        )
    latest = series.samples[0]
    remaining = latest.remaining_percent
    reset_at = latest.reset_at
    window_samples = [sample for sample in series.samples if sample.reset_at == reset_at]
    burn = _burn_rate(window_samples)
    warmup = burn is None
    sustainable = _sustainable_pace(remaining, reset_at, now)
    risk = _risk(remaining, burn, sustainable, warmup)
    return CapacityForecast(
        key=series.key,
        known=True,
        remaining_percent=remaining,
        reset_at=reset_at,
        observed_at=latest.observed_at,
        warmup=warmup,
        burn_percent_per_hour=burn,
        sustainable_percent_per_hour=sustainable,
        risk=risk,
    )


def _burn_rate(window_samples: list[NormalizedSample]) -> float | None:
    if len(window_samples) < 2:
        return None
    newest = window_samples[0]
    oldest = window_samples[-1]
    if (
        newest.observed_at is None
        or oldest.observed_at is None
        or newest.remaining_percent is None
        or oldest.remaining_percent is None
    ):
        return None
    elapsed_hours = (newest.observed_at - oldest.observed_at) / 3600
    if elapsed_hours <= 0:
        return None
    consumed = oldest.remaining_percent - newest.remaining_percent
    return max(consumed, 0.0) / elapsed_hours


def _sustainable_pace(
    remaining: float, reset_at: float | None, now: float
) -> float | None:
    if reset_at is None or reset_at <= now:
        return None
    hours_left = (reset_at - now) / 3600
    if hours_left <= 0:
        return None
    return remaining / hours_left


def _risk(
    remaining: float,
    burn: float | None,
    sustainable: float | None,
    warmup: bool,
) -> str:
    if remaining <= _HIGH_REMAINING_THRESHOLD:
        return RISK_HIGH
    if remaining <= _LOW_REMAINING_THRESHOLD:
        return RISK_MEDIUM
    if not warmup and burn is not None and sustainable is not None:
        if burn > sustainable * _BURN_HIGH_MULTIPLIER:
            return RISK_HIGH
        if burn > sustainable * _BURN_MEDIUM_MULTIPLIER:
            return RISK_MEDIUM
        return RISK_LOW
    return RISK_LOW
