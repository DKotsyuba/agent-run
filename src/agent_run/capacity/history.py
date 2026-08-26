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
    store: StateStore, *, runtime: str | None = None, limit: int = 500
) -> tuple[CapacitySeries, ...]:
    """Load stored samples grouped per identity, newest first, unfiltered by staleness.

    ``recent_capacity_samples`` filters rows by validity as of a reference time;
    passing ``at=0.0`` keeps every stored row (valid_until is always >= 0 when
    set) so trend calculations retain history that has since expired for
    "current" reporting purposes. Freshness for reporting is decided later by
    the forecast, using each row's own ``valid_until``.
    """

    if not isinstance(store, StateStore):
        raise ValidationError("store must be a StateStore")
    rows = store.recent_capacity_samples(at=0.0, runtime=runtime, limit=limit)
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
