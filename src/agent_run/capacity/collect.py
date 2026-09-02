"""Normalize enabled-adapter limits into private StateStore samples.

A failure or missing capability on one runtime never blocks samples from any
other enabled runtime; the failing runtime simply contributes no samples and
its capacity stays ``unknown`` downstream. No secrets or raw provider
responses are stored: only the structured :class:`LimitSample` fields.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from ..adapters.base import LimitSample, RuntimeAdapter
from ..adapters.registry import load_adapter
from ..config import CapacityConfig, Config, RuntimeConfig
from ..state import StateStore
from . import sources
from .topology import CapacityCollectionSlice, validate_slice

_logger = logging.getLogger("agent_run.capacity")

AdapterLoader = Callable[[str, RuntimeConfig], RuntimeAdapter]

STATUS_COLLECTED = "collected"
STATUS_UNSUPPORTED = "unsupported"
STATUS_FAILED = "failed"


@dataclass(frozen=True)
class CollectResult:
    runtime: str
    status: str
    sample_count: int
    error: str | None = None


@dataclass(frozen=True)
class CollectionReport:
    started_at: float
    finished_at: float
    results: tuple[CollectResult, ...]


def _default_loader(name: str, runtime_config: RuntimeConfig) -> RuntimeAdapter:
    return load_adapter(runtime_config)


def _epoch(value: datetime | None) -> float | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()





def _slice_from_samples(
    name: str,
    runtime_config: RuntimeConfig,
    samples: tuple[LimitSample, ...],
    started: float,
) -> CapacityCollectionSlice:
    """Build one validated, expiring slice from samples already collected once.

    ``samples`` remains in its original tuple form for legacy source callers.
    The slice observes the newest sample timestamp (or ``started`` when none
    exists) and expires after the shortest positive sample shelf life so no
    pool outlives its least-fresh constituent. Raises validation errors for
    malformed samples before persistence can begin.
    """

    for sample in samples:
        if not isinstance(sample, LimitSample):
            raise TypeError("limit sample must be a LimitSample")
        if sample.valid_for_seconds is not None and not isinstance(
            sample.valid_for_seconds, (int, float)
        ):
            raise ValueError("valid_for_seconds must be numeric when provided")
    observed = max(
        (epoch for epoch in (_epoch(sample.observed_at) for sample in samples) if epoch),
        default=started,
    )
    valid_for = min(
        (
            sample.valid_for_seconds
            for sample in samples
            if sample.valid_for_seconds and sample.valid_for_seconds > 0
        ),
        default=_SLICE_VALID_FOR_SECONDS,
    )
    return validate_slice(
        name,
        name,
        samples,
        sources.sample_topology(name, runtime_config, samples),
        observed,
        observed + valid_for,
    )



def _collect_runtime(
    store: StateStore,
    name: str,
    runtime_config: RuntimeConfig,
    capacity_config: CapacityConfig,
    load: AdapterLoader,
    started: float,
    agent_run_home: Path | None,
) -> CollectResult:
    """Collect and atomically persist one runtime while isolating its failures.

    Ordinary sources retain the historical single-slice collection path.
    ``codex_appserver`` may return one independently validated slice per
    configured account scope; every successful scope persists independently so
    a broken account cannot erase healthy or prior scope evidence.  Returns a
    status/count result and logs only the exception class on source failure.
    """

    try:
        if runtime_config.limits_source == "codex_appserver":
            from .topology import CapacityCollectionSlice

            slices = sources.collect_codex_appserver_slices(name, runtime_config, started)
            count = 0
            for slice_ in slices:
                if not isinstance(slice_, CapacityCollectionSlice):
                    raise TypeError("codex app-server collector returned an invalid slice")
                persist_slice(store, slice_)
                count += len(slice_.samples)
            _logger.debug("collect runtime=%s status=%s samples=%d", name, STATUS_COLLECTED, count)
            return CollectResult(name, STATUS_COLLECTED, count)
        samples = sources.collect_samples(
            name, runtime_config, capacity_config, load, agent_run_home
        )
        if samples is None:
            _logger.debug("collect runtime=%s status=%s", name, STATUS_UNSUPPORTED)
            return CollectResult(name, STATUS_UNSUPPORTED, 0)
        if not samples:
            return CollectResult(name, STATUS_COLLECTED, 0)
        persist_slice(store, _slice_from_samples(name, runtime_config, samples, started))
        count = len(samples)
    except Exception as error:  # isolate provider, generator, and sample failures
        _logger.warning(
            "collect runtime=%s status=%s samples=%d error=%s",
            name, STATUS_FAILED, 0, type(error).__name__,
        )
        return CollectResult(name, STATUS_FAILED, 0, type(error).__name__)
    _logger.debug("collect runtime=%s status=%s samples=%d", name, STATUS_COLLECTED, count)
    return CollectResult(name, STATUS_COLLECTED, count)


def collect_once(
    store: StateStore,
    config: Config,
    *,
    at: float | None = None,
    loader: AdapterLoader | None = None,
    agent_run_home: Path | None = None,
) -> CollectionReport:
    """Collect one bounded round of samples for every enabled runtime."""

    load = loader or _default_loader
    started = time.time() if at is None else at
    results = tuple(
        _collect_runtime(
            store, name, runtime_config, config.capacity, load, started, agent_run_home
        )
        for name, runtime_config in config.runtimes.items()
        if runtime_config.enabled
    )
    store.prune_capacity_samples(config.capacity.sample_retention)
    finished = time.time() if at is None else at
    _logger.info(
        "collect_once runtimes=%d failed=%d duration_ms=%.1f",
        len(results),
        sum(1 for result in results if result.status == STATUS_FAILED),
        (finished - started) * 1000,
    )
    return CollectionReport(started, finished, results)


#: Shelf life applied to a slice whose samples carry no ``valid_for_seconds``.
_SLICE_VALID_FOR_SECONDS = 900


def collect_slice(
    name: str,
    runtime_config: RuntimeConfig,
    capacity_config: CapacityConfig,
    load: sources.Loader,
    agent_run_home: Path | None = None,
    *,
    at: float | None = None,
) -> CapacityCollectionSlice | None:
    """Collect and fully validate one runtime's slice before any persistence.

    This is the narrow wiring point for a concurrent state consumer: it runs
    the runtime's configured source, derives the source's explicit topology,
    and validates the *entire* slice (samples plus topology plus shelf life)
    atomically — a caller that persists only what this function returned can
    never store a half-validated round. ``observed_at`` is the newest sample
    observation (or ``at``/now when samples carry none); ``valid_until``
    extends it by the samples' shortest positive ``valid_for_seconds`` or
    ``_SLICE_VALID_FOR_SECONDS``. Returns ``None`` exactly when the source
    concept does not apply to the runtime; an empty tuple of samples yields a
    valid empty-topology slice. Persistence is left to the caller so this
    stays usable from any store.
    """

    samples = sources.collect_samples(
        name, runtime_config, capacity_config, load, agent_run_home
    )
    if samples is None:
        return None
    started = time.time() if at is None else at
    return _slice_from_samples(name, runtime_config, samples, started)


def persist_slice(store: StateStore, collected: CapacityCollectionSlice) -> None:
    """Persist one already-validated slice atomically through the state API.

    This consumes :meth:`StateStore.append_capacity_samples` — samples and
    the route topology snapshot land in one transaction, so a slice is either
    wholly stored or not at all. Each sample's epoch fields come from the
    slice's own timestamps: the per-sample ``observed_at`` when the sample
    carries one, else the slice's. A sample with a positive shelf life gets
    its own expiry; legacy samples without one retain ``None`` while the
    topology snapshot keeps the slice expiry. The topology is
    serialized as its pool ids, key identities, and routes. Raises
    ``ValidationError`` (from the store) on the first malformed value; the
    caller supplies a ``StateStore`` opened for this thread.
    """

    sample_rows = []
    for sample in collected.samples:
        observed_epoch = _epoch(sample.observed_at) or collected.observed_at
        valid_until = None
        if sample.valid_for_seconds and sample.valid_for_seconds > 0:
            valid_until = observed_epoch + sample.valid_for_seconds
        sample_rows.append(
            {
                "lane": sample.lane,
                "window": sample.window,
                "source": sample.source,
                "target": sample.target,
                "remaining_percent": sample.remaining_percent,
                "reset_at": _epoch(sample.reset_at),
                "observed_at": observed_epoch,
                "valid_until": valid_until,
                "payload": None,
            }
        )
    store.append_capacity_samples(
        sample_rows,
        runtime=collected.runtime,
        scope_id=collected.scope_id,
        observed_at=collected.observed_at,
        valid_until=collected.valid_until,
        payload={
            "pools": [
                {
                    "pool_id": pool.pool_id,
                    "keys": [
                        {
                            "runtime": key.runtime,
                            "lane": key.lane,
                            "window": key.window,
                            "target": key.target,
                            "source": key.source,
                        }
                        for key in sorted(
                            pool.keys,
                            key=lambda item: (item.lane, item.window, item.target or "", item.source),
                        )
                    ],
                }
                for pool in collected.topology.pools
            ],
            "routes": [
                {
                    "route_id": route.route_id,
                    "runtime": route.runtime,
                    "account": route.account,
                    "quota_lane": route.quota_lane,
                    "pool_ids": list(route.pool_ids),
                }
                for route in collected.topology.routes
            ],
        },
    )
