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
from ..errors import CapacitySourceError
from ..state import StateStore
from . import sources
from .topology import CapacityCollectionSlice, validate_slice

_logger = logging.getLogger("agent_run.capacity")

AdapterLoader = Callable[[str, RuntimeConfig], RuntimeAdapter]

STATUS_COLLECTED = "collected"
STATUS_UNSUPPORTED = "unsupported"
STATUS_FAILED = "failed"
STATUS_PARTIAL = "partial"
STATUS_NO_DATA = "no_data"

#: Fixed per-slice issue code for a slice that validated but could not be
#: committed; later slices still persist.
ISSUE_PERSIST_FAILED = "persist_failed"


@dataclass(frozen=True)
class CollectResult:
    """One runtime's collection outcome for one round.

    ``status`` is one of ``collected``, ``partial``, ``failed``,
    ``no_data``, or ``unsupported``. ``sample_count`` counts only samples
    actually committed to the store this round. ``error`` keeps the
    historical single-failure summary -- a :class:`CapacitySourceError`
    reason code or an exception type name -- and ``issues`` carries every
    fixed reason code for scopes that failed or stayed empty, including
    per-slice persistence failures. Neither field ever holds raw provider
    output or exception text.
    """

    runtime: str
    status: str
    sample_count: int
    error: str | None = None
    issues: tuple[str, ...] = ()


@dataclass(frozen=True)
class CollectionReport:
    """All enabled runtimes' outcomes for one bounded collection round.

    ``started_at``/``finished_at`` are epoch seconds bounding the round and
    ``results`` holds one :class:`CollectResult` per enabled runtime.
    """

    started_at: float
    finished_at: float
    results: tuple[CollectResult, ...]

    @property
    def ok(self) -> bool:
        """True only when every runtime fully collected or is unsupported.

        ``partial``, ``failed``, and ``no_data`` outcomes are degraded
        rounds: the capacity view is incomplete or stale, so the command
        must not report success.
        """

        return all(
            result.status in (STATUS_COLLECTED, STATUS_UNSUPPORTED)
            for result in self.results
        )


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

    Ordinary sources keep the single-slice path: ``None`` is unsupported, a
    healthy empty tuple is ``no_data``, and operational failures arrive as
    :class:`CapacitySourceError` (or adapter exceptions) and become
    ``failed`` with a safe reason. ``codex_appserver`` returns one
    independently validated slice per configured account scope plus
    per-scope issue codes; every slice validates and persists on its own,
    so a broken account or a single slice's persistence failure can not
    erase healthy or prior scope evidence, and ``sample_count`` counts
    only samples actually committed. Logs carry fixed reasons, statuses,
    and exception class names only -- never provider output.
    """

    try:
        if runtime_config.limits_source == "codex_appserver":
            from .topology import CapacityCollectionSlice

            collected = sources.collect_codex_appserver(name, runtime_config, started)
            issues = list(collected.issues)
            committed = 0
            for slice_ in collected.slices:
                scope = (
                    slice_.scope_id
                    if isinstance(slice_, CapacityCollectionSlice)
                    else "invalid"
                )
                try:
                    if not isinstance(slice_, CapacityCollectionSlice):
                        raise TypeError(
                            "codex app-server collector returned an invalid slice"
                        )
                    persist_slice(store, slice_)
                except Exception as error:
                    # One scope's persistence failure must not prevent
                    # later healthy slices from committing.
                    issues.append(ISSUE_PERSIST_FAILED)
                    _logger.warning(
                        "persist runtime=%s scope=%s issue=%s error=%s",
                        name, scope, ISSUE_PERSIST_FAILED,
                        type(error).__name__,
                    )
                    continue
                committed += len(slice_.samples)
            if committed and not issues:
                status, count = STATUS_COLLECTED, committed
            elif committed:
                status, count = STATUS_PARTIAL, committed
            elif issues:
                return _result(name, STATUS_FAILED, 0, issues=issues)
            else:
                status, count = STATUS_NO_DATA, 0
        else:
            samples = sources.collect_samples(
                name, runtime_config, capacity_config, load, agent_run_home
            )
            if samples is None:
                status, count = STATUS_UNSUPPORTED, 0
            elif not samples:
                status, count = STATUS_NO_DATA, 0
            else:
                persist_slice(
                    store, _slice_from_samples(name, runtime_config, samples, started)
                )
                status, count = STATUS_COLLECTED, len(samples)
    except CapacitySourceError as error:
        _logger.warning(
            "collect runtime=%s status=%s reason=%s",
            name, STATUS_FAILED, error.reason,
        )
        return CollectResult(name, STATUS_FAILED, 0, error.reason)
    except Exception as error:  # isolate provider, generator, and sample failures
        _logger.warning(
            "collect runtime=%s status=%s samples=%d error=%s",
            name, STATUS_FAILED, 0, type(error).__name__,
        )
        return CollectResult(name, STATUS_FAILED, 0, type(error).__name__)
    return _result(name, status, count, issues=issues if status == STATUS_PARTIAL else ())


def _result(
    name: str,
    status: str,
    count: int,
    *,
    issues: tuple[str, ...] | list[str] = (),
) -> CollectResult:
    """Log one runtime's outcome at INFO and build its result."""

    _logger.info(
        "collect runtime=%s status=%s samples=%d", name, status, count
    )
    return CollectResult(name, status, count, None, tuple(issues))


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
        "collect_once runtimes=%d samples=%d failed=%d partial=%d no_data=%d"
        " duration_ms=%.1f",
        len(results),
        sum(result.sample_count for result in results),
        sum(1 for result in results if result.status == STATUS_FAILED),
        sum(1 for result in results if result.status == STATUS_PARTIAL),
        sum(1 for result in results if result.status == STATUS_NO_DATA),
        (finished - started) * 1000,
    )
    if finished - started >= config.capacity.collect_interval_seconds:
        # Each source keeps its own bounded deadline; this only makes a round
        # slow enough to skip its next scheduled tick visible to the operator.
        _logger.warning(
            "collect_once duration_seconds=%.1f >= interval_seconds=%s;"
            " the next scheduled tick may be skipped",
            finished - started, config.capacity.collect_interval_seconds,
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
                    "reset_credits": route.reset_credits,
                }
                for route in collected.topology.routes
            ],
        },
    )
