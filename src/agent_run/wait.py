"""Blocking ``wait`` verbs: poll the durable store until a run is terminal.

``agent-run wait AGENT_ID`` and ``agent-run workflow wait RUN_ID`` replace the
hand-rolled ``until agent-run status ...; sleep`` loops orchestrators fall back
to when the MCP server is unavailable. The polling loop lives here so
:mod:`agent_run.cli` only wires parsed arguments to it, and both the sleeper
and the clock are injectable so tests advance virtual time without really
sleeping. Every poll goes through the same service facade the other verbs use;
this module never opens a store or spawns a process.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType

#: Seconds between two polls when ``--poll`` is omitted.
DEFAULT_POLL_SECONDS = 5.0
#: Smallest accepted poll interval; shorter requests would busy-loop the store.
MIN_POLL_SECONDS = 1.0

#: Exit code when the watcher's own ``--timeout`` elapsed while the run was
#: still going; the run's own terminal exit codes are the ones below.
WATCHER_TIMEOUT_EXIT = 5

#: Agent status -> process exit code once the agent is no longer running.
#: ``lost`` shares the failure code: a lost agent never reaches another
#: terminal status, so waiting on it would hang a watcher with no timeout.
AGENT_EXIT_CODES: Mapping[str, int] = MappingProxyType(
    {"succeeded": 0, "failed": 2, "lost": 2, "cancelled": 3, "timed_out": 4}
)

#: Workflow run status -> process exit code; ``lost`` shares the failure code.
WORKFLOW_EXIT_CODES: Mapping[str, int] = MappingProxyType(
    {"succeeded": 0, "failed": 2, "lost": 2, "cancelled": 3}
)


@dataclass(frozen=True)
class WaitOutcome:
    """What a wait verb prints and returns once it stops waiting.

    ``payload`` is the JSON-ready object for stdout: the same envelope the
    ``answer`` verb prints when an agent finished, otherwise the current status
    view. ``exit_code`` is the process exit status (see the exit-code mappings
    above). ``note`` is an optional one-line stderr message, set only when the
    watcher gave up while the run was still going.
    """

    exit_code: int
    payload: object
    note: str | None = None


def _status_name(status: object) -> str:
    """Return the plain status string for an enum member or a raw value.

    ``status`` may be an :class:`enum.Enum` member (``AgentView.status``),
    a plain string, or ``None``; unknown values read as ``"unknown"`` so a
    malformed report keeps the watcher polling instead of crashing it.
    """

    if status is None:
        return "unknown"
    if isinstance(status, Enum):
        return str(status.value)
    return str(status)


def _workflow_status(report: object) -> str:
    """Read the run status out of a ``workflow status`` report.

    ``report`` is whatever the facade returned for the run: the durable journal
    summary nests the row under ``"run"``, and a flat ``{"status": ...}`` report
    is accepted as well. Anything unreadable reads as ``"unknown"``.
    """

    if isinstance(report, Mapping):
        status = report.get("status")
        if status is None and isinstance(report.get("run"), Mapping):
            status = report["run"].get("status")
        return _status_name(status)
    return "unknown"


def _wait_until_terminal(
    poll_once: Callable[[], tuple[str, object]],
    exit_codes: Mapping[str, int],
    *,
    timeout: float,
    poll: float,
    sleep: Callable[[float], None],
    clock: Callable[[], float],
) -> WaitOutcome:
    """Poll until ``poll_once`` reports a status named in ``exit_codes``.

    ``poll_once`` reads the durable store once and returns the status name plus
    the payload to print if this iteration ends the wait. ``timeout`` is the
    watcher budget in seconds; zero or negative waits forever, because the
    run's own ``timeout_seconds`` already bounds it. ``poll`` is the seconds
    handed to ``sleep`` between polls, clamped up to ``MIN_POLL_SECONDS``, so
    the store is never busy-looped. ``clock`` must return monotonic seconds and
    ``sleep`` must block; both are injectable for tests.

    The status is read at least once before the budget is consulted, so an
    already-terminal run returns immediately without sleeping, and a watcher
    that gives up still reports the freshest status it saw.
    """

    interval = max(MIN_POLL_SECONDS, poll)
    deadline = None if timeout <= 0 else clock() + timeout
    while True:
        status, payload = poll_once()
        code = exit_codes.get(status)
        if code is not None:
            return WaitOutcome(code, payload)
        if deadline is not None and clock() >= deadline:
            return WaitOutcome(
                WATCHER_TIMEOUT_EXIT,
                payload,
                f"wait gave up after {timeout:g}s; status is still {status}",
            )
        sleep(interval)


def wait_for_agent(
    service,
    agent_id: str,
    *,
    timeout: float = 0.0,
    poll: float = DEFAULT_POLL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> WaitOutcome:
    """Block until ``agent_id`` reaches a terminal status.

    ``service`` is the same status/answer facade the other CLI verbs use, so
    each poll calls ``service.get(agent_id)`` -- an unknown id raises exactly as
    the ``status`` verb does -- and a terminal agent prints the very envelope
    the ``answer`` verb prints, fetched once via ``service.answer(agent_id)``.
    ``timeout``, ``poll``, ``sleep`` and ``clock`` are the watcher budget, the
    poll interval, and their injectable counterparts as described by
    :func:`_wait_until_terminal`.

    Raises whatever ``service`` raises, so an unknown or malformed id surfaces
    unchanged to the CLI's error envelope.
    """

    def poll_once() -> tuple[str, object]:
        view = service.get(agent_id)
        status = _status_name(view.status)
        if status in AGENT_EXIT_CODES:
            return status, service.answer(agent_id)
        return status, view

    return _wait_until_terminal(
        poll_once,
        AGENT_EXIT_CODES,
        timeout=timeout,
        poll=poll,
        sleep=sleep,
        clock=clock,
    )


def wait_for_workflow(
    service,
    run_id: str,
    *,
    timeout: float = 0.0,
    poll: float = DEFAULT_POLL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> WaitOutcome:
    """Block until workflow run ``run_id`` reaches a terminal status.

    ``service`` is the workflow facade the ``workflow status`` verb uses; every
    poll calls ``service.workflow_status(run_id)`` and the printed payload is
    that same report, so a terminal run prints exactly what ``workflow status``
    prints. ``lost`` counts as terminal and exits with the failure code. The
    remaining arguments and the error behaviour match :func:`wait_for_agent`.
    """

    def poll_once() -> tuple[str, object]:
        report = service.workflow_status(run_id)
        return _workflow_status(report), report

    return _wait_until_terminal(
        poll_once,
        WORKFLOW_EXIT_CODES,
        timeout=timeout,
        poll=poll,
        sleep=sleep,
        clock=clock,
    )
