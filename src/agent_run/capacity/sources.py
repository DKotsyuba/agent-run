"""Resolve which evidence source answers a runtime's capacity limits.

The collector used to ask every adapter for live limits; a runtime now names
its source explicitly (``native``, ``omniroute``, ``codexbar``, ``none``) so a
stale or never-refreshed native channel stops being the only answer. Every
source failure is no evidence, never an exception: ``None`` means the source
concept does not apply to this runtime, ``()`` means it applies and produced
no samples.
"""

from __future__ import annotations

import json
import logging
import math
import subprocess
import time
from datetime import datetime
from typing import Callable, Mapping

from ..adapters import omniroute
from ..adapters.base import Capability, LimitSample, RuntimeAdapter
from ..config import CapacityConfig, RuntimeConfig

_logger = logging.getLogger("agent_run.capacity")

Loader = Callable[[str, RuntimeConfig], RuntimeAdapter]

_CODEXBAR_PROVIDERS = {"codex": "codex", "claude": "claude"}
_CODEXBAR_VALID_FOR_SECONDS = 900
_CODEXBAR_TIMEOUT_SECONDS = 60
_CODEXBAR_LANES = ("primary", "secondary", "tertiary")


def _timestamp(value: object) -> datetime | None:
    """Parse one ISO-8601 stamp ('Z' suffix included); anything else is unknown."""

    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed


def _run_codexbar(argv: list[str]) -> subprocess.CompletedProcess:
    """One argv run with no shell; failures surface as no evidence upstream."""

    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=_CODEXBAR_TIMEOUT_SECONDS,
        check=False,
    )


def _codexbar_samples(name: str, capacity: CapacityConfig) -> tuple[LimitSample, ...]:
    provider = _CODEXBAR_PROVIDERS.get(name)
    if provider is None:
        _logger.warning("codexbar source has no provider for runtime=%s", name)
        return ()
    argv = [str(capacity.codexbar_binary), "usage", "--provider", provider, "--json"]
    try:
        result = _run_codexbar(argv)
    except (OSError, subprocess.TimeoutExpired) as error:
        _logger.warning("codexbar runtime=%s failed: %s", name, type(error).__name__)
        return ()
    if result.returncode != 0:
        _logger.warning("codexbar runtime=%s rc=%d", name, result.returncode)
        return ()
    try:
        payload = json.loads(result.stdout)
    except ValueError:
        _logger.warning("codexbar runtime=%s unparseable stdout", name)
        return ()
    if not isinstance(payload, list) or not payload:
        _logger.warning("codexbar runtime=%s returned no account entries", name)
        return ()
    account = payload[0]
    usage = account.get("usage") if isinstance(account, Mapping) else None
    if not isinstance(usage, Mapping):
        _logger.warning("codexbar runtime=%s missing usage object", name)
        return ()

    observed_at = _timestamp(usage.get("updatedAt"))
    samples = []
    for lane in _CODEXBAR_LANES:
        window = usage.get(lane)
        if not isinstance(window, Mapping):
            continue
        used = window.get("usedPercent")
        if (
            isinstance(used, bool)
            or not isinstance(used, (int, float))
            or not math.isfinite(used)
        ):
            continue
        minutes = window.get("windowMinutes")
        label = (
            "five_hour"
            if minutes == 300
            else "seven_day"
            if minutes == 10080
            else f"min{minutes}"
        )
        samples.append(
            LimitSample(
                lane=lane,
                window=label,
                remaining_percent=max(0.0, min(100.0, 100.0 - float(used))),
                reset_at=_timestamp(window.get("resetsAt")),
                observed_at=observed_at,
                source="codexbar",
                target=None,
                valid_for_seconds=_CODEXBAR_VALID_FOR_SECONDS,
            )
        )
    return tuple(samples)


def collect_samples(
    name: str,
    runtime_config: RuntimeConfig,
    capacity_config: CapacityConfig,
    load: Loader,
) -> tuple[LimitSample, ...] | None:
    """Resolve the runtime's configured source; ``None`` is unsupported."""

    source = runtime_config.limits_source or "native"
    if source == "none":
        return ()
    if source == "omniroute":
        return omniroute.pool_samples(time.time())
    if source == "codexbar":
        return _codexbar_samples(name, capacity_config)

    adapter = load(name, runtime_config)
    info = adapter.describe()
    if Capability.LIVE_LIMITS not in info.capabilities:
        return None
    return tuple(adapter.limits(runtime_config, runtime_config.home))
