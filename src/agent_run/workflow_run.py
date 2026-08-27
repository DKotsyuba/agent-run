"""Create one workflow run and hand it to its own detached runner process.

The run row is durable before the fork, and the runner claims ownership of it
before it reports READY, so a run id this module returns always names a run
some live process owns -- or the launch failed and the run is already ``lost``.
"""

from __future__ import annotations

import hashlib
import json
import math
import sys
from collections.abc import Mapping, Sequence
from contextlib import suppress
from pathlib import Path

from .errors import StateTransitionError, ValidationError
from .launch import DEFAULT_READY_TIMEOUT_SECONDS, launch_detached
from .paths import agent_run_home, state_db_path
from .state.db import json_text, nonblank
from .state.store import StateStore


WORKFLOW_RUNNER_MODULE = "agent_run.workflow_runner_main"
STEP_KINDS = frozenset({"sleep"})
MAX_PLAN_STEPS = 100
MAX_STEP_SECONDS = 3600.0
RESUMABLE_RUN_STATUSES = frozenset({"failed", "cancelled", "lost"})


def validate_plan(plan: object) -> tuple[dict[str, object], ...]:
    """Normalize a placeholder plan into JSON-safe, deterministic steps.

    ``sleep`` is the only step kind while the script engine is still a later
    task.  ``key_hint`` is what tells two otherwise identical steps apart:
    :func:`agent_run.state.step_key` hashes the whole spec with its position,
    so an unhinted repeat of a step at another position is still distinct.
    """

    if isinstance(plan, (str, bytes)) or not isinstance(plan, Sequence):
        raise ValidationError("workflow plan must be a sequence of steps")
    if not 0 < len(plan) <= MAX_PLAN_STEPS:
        raise ValidationError(f"workflow plan must hold 1 to {MAX_PLAN_STEPS} steps")
    steps: list[dict[str, object]] = []
    for position, step in enumerate(plan):
        if not isinstance(step, Mapping):
            raise ValidationError(f"workflow step {position} must be a mapping")
        kind = step.get("kind")
        if kind not in STEP_KINDS:
            raise ValidationError(
                f"workflow step {position} kind must be one of {sorted(STEP_KINDS)}"
            )
        seconds = step.get("seconds", 0)
        if (
            isinstance(seconds, bool)
            or not isinstance(seconds, (int, float))
            or not math.isfinite(seconds)
            or not 0 <= seconds <= MAX_STEP_SECONDS
        ):
            raise ValidationError(
                f"workflow step {position} seconds must be between 0 and {MAX_STEP_SECONDS:g}"
            )
        hint = step.get("key_hint", "")
        if not isinstance(hint, str):
            raise ValidationError(f"workflow step {position} key_hint must be a string")
        steps.append({"kind": kind, "seconds": float(seconds), "key_hint": hint})
    return tuple(steps)


def plan_sha(steps: Sequence[Mapping[str, object]]) -> str:
    """The script identity a run is created with: sha256 of its canonical JSON."""

    payload = [dict(step) for step in steps]
    return hashlib.sha256(json_text(payload).encode("utf-8")).hexdigest()


def start_workflow(
    home: str | Path | None,
    name: str,
    plan: object,
    *,
    executable: str | None = None,
    readiness_timeout_seconds: float = DEFAULT_READY_TIMEOUT_SECONDS,
) -> str:
    """Create the run row, launch its detached runner, and return the run id.

    Returns only once the runner has reported READY, which it does only after
    its ownership of the run is durable.  A launch that never got there leaves
    no run waiting for an owner that will never come: it is finished ``lost``.
    """

    nonblank("workflow name", name)
    steps = validate_plan(plan)
    plan_payload = [dict(step) for step in steps]
    root = agent_run_home(home)
    database = state_db_path(root)
    store = StateStore.open(database)
    try:
        run_id = store.create_workflow_run(name, plan_sha(steps), plan=plan_payload)
    finally:
        store.close()

    try:
        launch_detached(
            {
                "home": str(root),
                "run_id": run_id,
                "plan": plan_payload,
            },
            executable=sys.executable if executable is None else executable,
            module=WORKFLOW_RUNNER_MODULE,
            readiness_timeout_seconds=readiness_timeout_seconds,
        )
    except BaseException:
        with suppress(Exception):  # the launch failure is the reportable one
            _abandon(database, run_id)
        raise
    return run_id


def resume_workflow(
    home: str | Path | None,
    run_id: str,
    *,
    executable: str | None = None,
    readiness_timeout_seconds: float = DEFAULT_READY_TIMEOUT_SECONDS,
) -> str:
    """Relaunch a detached runner for one already-created run, replaying its journal.

    The run keeps its own id and its stored ``plan_json``; only a fresh runner
    identity claims it.  ``StateStore.open`` reconciles first, so a run whose
    owner died is already ``lost`` by the time it is inspected here -- a
    ``running`` status this sees is still owned by a live process, and
    resuming it would put two runners on one run, which is refused.  A step
    whose journal already holds a ``succeeded`` result is replayed from the
    journal rather than re-executed; see the module docstring of
    :mod:`agent_run.state.workflow` for exactly what that replay leaves behind.
    """

    nonblank("run_id", run_id)
    root = agent_run_home(home)
    database = state_db_path(root)
    store = StateStore.open(database)
    try:
        run = store.workflow_run_status(run_id)["run"]
    finally:
        store.close()

    status = run["status"]
    if status == "succeeded":
        raise ValidationError(f"workflow run has already succeeded: {run_id}")
    if status == "running":
        raise StateTransitionError(
            f"workflow run is still running under a live owner: {run_id}"
        )
    if status not in RESUMABLE_RUN_STATUSES:
        raise ValidationError(f"workflow run cannot be resumed from status: {status}")
    plan_json = run["plan_json"]
    if plan_json is None:
        raise ValidationError(f"workflow run has no stored plan to resume: {run_id}")
    plan_payload = json.loads(plan_json)

    try:
        launch_detached(
            {
                "home": str(root),
                "run_id": run_id,
                "plan": plan_payload,
                "resume": True,
            },
            executable=sys.executable if executable is None else executable,
            module=WORKFLOW_RUNNER_MODULE,
            readiness_timeout_seconds=readiness_timeout_seconds,
        )
    except BaseException:
        with suppress(Exception):  # the launch failure is the reportable one
            _abandon(database, run_id)
        raise
    return run_id


def _abandon(database: Path, run_id: str) -> None:
    """Close a run whose runner never proved it owns it.

    A no-op when the run is already terminal -- which is exactly the case for
    a resume attempt whose runner never got far enough to re-claim the row: the
    run is simply left in whatever resumable status it already had, ready to
    be resumed again.
    """

    store = StateStore.open(database)
    try:
        with suppress(ValidationError, StateTransitionError):
            store.finish_workflow_run(run_id, "lost")
    finally:
        store.close()
