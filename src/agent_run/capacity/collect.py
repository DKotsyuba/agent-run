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
from typing import Callable

from ..adapters.base import LimitSample, RuntimeAdapter
from ..adapters.registry import load_adapter
from ..config import CapacityConfig, Config, RuntimeConfig
from ..state import StateStore
from . import sources

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


def _store_sample(
    store: StateStore, runtime: str, sample: LimitSample, observed_epoch: float
) -> None:
    reset_epoch = _epoch(sample.reset_at)
    valid_until = None
    if sample.valid_for_seconds is not None:
        valid_until = observed_epoch + max(float(sample.valid_for_seconds), 0.0)
    store.insert_capacity_sample(
        runtime=runtime,
        lane=sample.lane,
        window=sample.window,
        source=sample.source,
        target=sample.target,
        remaining_percent=sample.remaining_percent,
        reset_at=reset_epoch,
        observed_at=observed_epoch,
        valid_until=valid_until,
        payload={
            "lane": sample.lane,
            "window": sample.window,
            "target": sample.target,
            "source": sample.source,
            "remaining_percent": sample.remaining_percent,
            "reset_at": reset_epoch,
            "valid_for_seconds": sample.valid_for_seconds,
        },
    )


def _collect_runtime(
    store: StateStore,
    name: str,
    runtime_config: RuntimeConfig,
    capacity_config: CapacityConfig,
    load: AdapterLoader,
    started: float,
) -> CollectResult:
    count = 0
    try:
        samples = sources.collect_samples(name, runtime_config, capacity_config, load)
        if samples is None:
            _logger.debug("collect runtime=%s status=%s", name, STATUS_UNSUPPORTED)
            return CollectResult(name, STATUS_UNSUPPORTED, 0)
        for sample in samples:
            if not isinstance(sample, LimitSample):
                raise TypeError("limit sample must be a LimitSample")
            observed_epoch = _epoch(sample.observed_at)
            if observed_epoch is None:
                observed_epoch = started
            _store_sample(store, name, sample, observed_epoch)
            count += 1
    except Exception as error:  # isolate provider, generator, and sample failures
        _logger.warning(
            "collect runtime=%s status=%s samples=%d error=%s",
            name, STATUS_FAILED, count, type(error).__name__,
        )
        return CollectResult(name, STATUS_FAILED, count, type(error).__name__)
    _logger.debug("collect runtime=%s status=%s samples=%d", name, STATUS_COLLECTED, count)
    return CollectResult(name, STATUS_COLLECTED, count)


def collect_once(
    store: StateStore,
    config: Config,
    *,
    at: float | None = None,
    loader: AdapterLoader | None = None,
) -> CollectionReport:
    """Collect one bounded round of samples for every enabled runtime."""

    load = loader or _default_loader
    started = time.time() if at is None else at
    results = tuple(
        _collect_runtime(store, name, runtime_config, config.capacity, load, started)
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
