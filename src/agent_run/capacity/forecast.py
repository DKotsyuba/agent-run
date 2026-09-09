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
#: Burn evidence must span at least this many seconds before it may escalate
#: risk. A lane only minutes old has a remaining percentage that moves in
#: whole-number jumps, so a couple of early samples extrapolate to a burn rate
#: that looks catastrophic while the lane still holds ~99% -- the remaining
#: thresholds cover that case until the evidence is an hour deep.
_BURN_MIN_SPAN_SECONDS = 3600.0

#: Maximum reset-reporting jitter in seconds. Nearby timestamps may share a
#: cycle only before both reported resets; see :func:`_same_reset_cycle`.
_RESET_MATCH_TOLERANCE_SECONDS = 1.0


@dataclass(frozen=True)
class CapacityForecast:
    """Forecast and risk evidence for one exact capacity identity.

    ``burn_span_seconds`` is the elapsed observation span used by the burn
    calculation, or ``None`` when the forecast is unknown or has fewer than
    two usable reset-matched samples.  All existing risk and rate fields keep
    their prior meanings and are not altered by exposing this evidence.
    """

    key: CapacityKey
    known: bool
    remaining_percent: float | None
    reset_at: float | None
    observed_at: float | None
    warmup: bool
    burn_percent_per_hour: float | None
    sustainable_percent_per_hour: float | None
    risk: str
    burn_span_seconds: float | None = None


def build_forecasts(
    series: Iterable[CapacitySeries], *, now: float
) -> tuple[CapacityForecast, ...]:
    return tuple(_forecast_one(item, now) for item in series)


def _is_fresh(sample: NormalizedSample, now: float) -> bool:
    """Accept observations from now or earlier only while their window is open."""
    valid = sample.valid_until is None or sample.valid_until >= now
    observed = sample.observed_at is None or sample.observed_at <= now
    not_yet_reset = sample.reset_at is None or sample.reset_at > now
    return valid and observed and not_yet_reset


def _same_reset_cycle(sample: NormalizedSample, latest: NormalizedSample) -> bool:
    """Report whether ``sample`` belongs to the reset window ``latest`` reports.

    Both arguments are :class:`~agent_run.capacity.history.NormalizedSample`
    values from one newest-first series; only
    ``reset_at`` and ``latest.observed_at`` (epoch seconds) are read.

    A ``None`` reset matches only another ``None`` reset. Two non-null resets
    name the same cycle when they are equal, or when they differ by at most
    ``_RESET_MATCH_TOLERANCE_SECONDS`` (seconds) and the older of the two
    resets was still in the future at ``latest.observed_at``: a reset that had
    already passed by the newest observation belongs to a window that genuinely
    rolled over, however close the two timestamps are. Without a usable
    ``latest.observed_at`` -- or once the difference exceeds the tolerance --
    only exact equality groups samples, which preserves the pre-tolerance
    behavior for histories that cannot be judged.
    """

    if sample.reset_at is None or latest.reset_at is None:
        return sample.reset_at == latest.reset_at
    if sample.reset_at == latest.reset_at:
        return True
    if abs(sample.reset_at - latest.reset_at) > _RESET_MATCH_TOLERANCE_SECONDS:
        return False
    observed = latest.observed_at
    if observed is None:
        return False
    return min(sample.reset_at, latest.reset_at) > observed


def _forecast_one(series: CapacitySeries, now: float) -> CapacityForecast:
    """Build a forecast from one newest-first series at epoch seconds ``now``.

    Missing, stale or unknown latest evidence produces an unknown forecast.
    Otherwise burn uses the current reset cycle with bounded reporting jitter;
    insufficient history stays in warmup. The latest raw reset is preserved,
    and neither input samples nor stored data are modified.
    """
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
            burn_span_seconds=None,
        )
    latest = series.samples[0]
    remaining = latest.remaining_percent
    assert remaining is not None  # guarded by the unknown-forecast branch above
    reset_at = latest.reset_at
    # Burn evidence groups every sample reporting the current reset instant,
    # tolerating the subsecond jitter providers add to one shared reset time.
    # The reported ``reset_at`` stays exactly what the newest sample said.
    window_samples = [
        sample for sample in series.samples if _same_reset_cycle(sample, latest)
    ]
    burn = _burn_rate(window_samples)
    warmup = burn is None
    sustainable = _sustainable_pace(remaining, reset_at, now)
    risk = _risk(remaining, burn, sustainable, warmup, _burn_span_seconds(window_samples))
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
        burn_span_seconds=_burn_span_seconds(window_samples),
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


def _burn_span_seconds(window_samples: list[NormalizedSample]) -> float | None:
    """Seconds of history between the newest and oldest burn samples.

    Mirrors the endpoints ``_burn_rate`` uses, so the two always agree about
    which samples constitute the evidence. Returns ``None`` when there are
    fewer than two samples or either endpoint lacks an ``observed_at``, which
    is exactly when ``_burn_rate`` returns ``None`` and burn cannot escalate
    anyway.
    """

    if len(window_samples) < 2:
        return None
    newest = window_samples[0]
    oldest = window_samples[-1]
    if newest.observed_at is None or oldest.observed_at is None:
        return None
    return newest.observed_at - oldest.observed_at


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
    burn_span_seconds: float | None,
) -> str:
    """Risk tier for one lane.

    ``remaining`` is the lane's remaining percentage (0-100). ``burn`` and
    ``sustainable`` are percent-per-hour rates, either possibly ``None`` when
    they could not be derived. ``warmup`` marks a lane with too few samples to
    state a burn rate at all. ``burn_span_seconds`` is how much time the burn
    evidence actually covers, or ``None`` when unknown.

    The remaining-percent thresholds always apply. Burn-based escalation is
    additionally gated on the evidence spanning at least
    ``_BURN_MIN_SPAN_SECONDS``; below that span the burn rate is reported
    unchanged but is too thin to escalate on, so the remaining thresholds
    alone decide.
    """

    if remaining <= _HIGH_REMAINING_THRESHOLD:
        return RISK_HIGH
    if remaining <= _LOW_REMAINING_THRESHOLD:
        return RISK_MEDIUM
    if (
        not warmup
        and burn is not None
        and sustainable is not None
        and burn_span_seconds is not None
        and burn_span_seconds >= _BURN_MIN_SPAN_SECONDS
    ):
        if burn > sustainable * _BURN_HIGH_MULTIPLIER:
            return RISK_HIGH
        if burn > sustainable * _BURN_MEDIUM_MULTIPLIER:
            return RISK_MEDIUM
        return RISK_LOW
    return RISK_LOW
