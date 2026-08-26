"""Normalized capacity history grouped by lane/window/target/source identity."""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import ValidationError
from ..state import StateStore


@dataclass(frozen=True)
class CapacityKey:
    runtime: str
    lane: str
    window: str
    target: str | None
    source: str


@dataclass(frozen=True)
class NormalizedSample:
    remaining_percent: float | None
    reset_at: float | None
    observed_at: float | None
    valid_until: float | None


@dataclass(frozen=True)
class CapacitySeries:
    key: CapacityKey
    samples: tuple[NormalizedSample, ...]  # newest observed_at first


def load_series(
    store: StateStore, *, retention: int, runtime: str | None = None
) -> tuple[CapacitySeries, ...]:
    """Load globally bounded samples grouped per identity, newest first.

    Freshness for reporting is decided later by the forecast, using each row's
    own ``valid_until``; expired rows remain available for same-reset trends.
    """

    if not isinstance(store, StateStore):
        raise ValidationError("store must be a StateStore")
    rows = store.capacity_sample_history(retention=retention, runtime=runtime)
    grouped: dict[CapacityKey, list[NormalizedSample]] = {}
    for row in rows:
        key = CapacityKey(
            str(row["runtime"]), str(row["lane"]), str(row["window"]), row["target"], str(row["source"])
        )
        grouped.setdefault(key, []).append(
            NormalizedSample(
                row["remaining_percent"],
                row["reset_at"],
                row["observed_at"],
                row["valid_until"],
            )
        )
    return tuple(
        CapacitySeries(key, tuple(samples)) for key, samples in grouped.items()
    )
