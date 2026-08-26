"""Bounded post-boot readiness checks for the managed OpenCode v2 service.

Split out of ``service.py`` to stay under this package's per-file line budget
(``tests/test_opencode_adapter.py::ProductionSizeTests``): the health and
model-roster proofs a spawned candidate must pass before ``start_service()``
records it.
"""

from __future__ import annotations

import subprocess
from typing import Callable, Mapping, Sequence

from .normalize import normalize_models
from .service import (
    _AUTH_REFUSED,
    PASSWORD_ENV,
    STARTUP_POLL_SECONDS,
    ServiceIsolationError,
    ServicePlan,
)


def await_health(
    plan: ServicePlan,
    process: subprocess.Popen,
    client: object,
    *,
    sleep: Callable[[float], None],
    monotonic: Callable[[], float],
) -> Mapping[str, object]:
    """Poll health until the candidate answers, dies, refuses, or runs out."""

    from .http import HttpError, TransientHttpError

    deadline = monotonic() + plan.startup_timeout_seconds
    while True:
        code = process.poll()
        if code is not None:
            raise ServiceIsolationError(
                f"opencode service exited with status {code} before it reported healthy"
            )
        try:
            return client.health()
        except TransientHttpError:
            pass
        except HttpError as error:
            if error.status in _AUTH_REFUSED:
                raise ServiceIsolationError(
                    f"opencode service refused the managed credentials with {error.status}; "
                    f"{PASSWORD_ENV} does not match the running service"
                ) from error
            raise
        if monotonic() >= deadline:
            raise ServiceIsolationError(
                "opencode service did not report healthy within "
                f"{plan.startup_timeout_seconds} seconds"
            )
        sleep(plan.poll_interval_seconds)


def await_model_roster(
    client: object,
    allowed: Sequence[str],
    *,
    plan: ServicePlan,
    started_at: float,
    sleep: Callable[[float], None],
    monotonic: Callable[[], float],
) -> None:
    """Poll /api/model until every configured model id is present and active.

    Live-proven v1 race: for ~1-2s after /api/health reports healthy,
    /api/model can still be empty while providers register async, and a
    session prompted in that window fails server-side with neither an
    assistant message nor a settled state. Anchored at ``started_at`` (taken
    right before the health phase) rather than a fresh clock, so this shares
    -- not extends -- the plan's one startup timeout budget.
    """

    deadline = started_at + plan.startup_timeout_seconds
    while True:
        ready = {info.id for info in normalize_models(client.models(), allowed)}
        missing = [model for model in allowed if model not in ready]
        if not missing:
            return
        if monotonic() >= deadline:
            raise ServiceIsolationError(
                "opencode service did not register model(s) "
                f"{', '.join(missing)} within {plan.startup_timeout_seconds} seconds"
            )
        sleep(STARTUP_POLL_SECONDS)
