"""Workflow lifecycle facade shared by the CLI and MCP transports.

`AgentService.workflow_start/status/cancel/answer` (service.py) and the CLI's
`_Runtime` (cli.py) both delegate here, so there is exactly one implementation
of the workflow lifecycle contract that every transport calls through.
"""

from __future__ import annotations

import json
import os
import signal
from pathlib import Path

from .domain import OrchestratorRef
from .errors import ValidationError
from .state.store import StateStore

_TERMINAL_WORKFLOW_STATUSES = {"succeeded", "failed", "cancelled", "lost"}


def workflow_start(
    home: str | Path,
    name: str,
    script: str,
    args: dict | None,
    orchestrator: OrchestratorRef | None,
) -> dict[str, str]:
    """Launch a script workflow, optionally binding its lifecycle notice.

    ``home`` is the agent-run home directory the workflow runner is launched
    against. ``name`` labels the run and ``script`` is the Python source the
    workflow runner executes; when ``args`` is not ``None`` it is prepended to
    ``script`` as a literal ``args = {...}`` assignment so the script can read
    it as a plain module-level name. ``orchestrator``, when given, is the
    caller's session reference bound to the run's lifecycle notice.

    Returns ``{"run_id": <str>}`` once the detached runner has reported ready;
    a launch that never reaches ready leaves no run waiting for an owner that
    will never come.
    """

    from .workflow_run import start_workflow

    source = script if args is None else f"args = {args!r}\n{script}"
    return {
        "run_id": start_workflow(home, name, {"script": source}, orchestrator=orchestrator)
    }


def workflow_status(store: StateStore, run_id: str) -> dict[str, object]:
    """Return the durable journal summary for one workflow run.

    ``store`` is an open ``StateStore``. Raises whatever ``store`` raises when
    ``run_id`` does not name a known workflow run.
    """

    return store.workflow_run_status(run_id)


def workflow_cancel(store: StateStore, run_id: str) -> dict[str, object]:
    """Request SIGTERM from a live workflow runner, refusing terminal runs.

    Raises ``ValidationError`` when the run has already reached a terminal
    status, or when its recorded owner process identity is missing or
    malformed, so a cancel request never signals an unrelated or absent
    process. Returns ``{"run_id": run_id, "cancel_requested": True}`` once the
    signal has been sent.
    """

    run = store.workflow_run_status(run_id)["run"]
    if run["status"] in _TERMINAL_WORKFLOW_STATUSES:
        raise ValidationError("terminal workflow run cannot be cancelled")
    identity = run["owner_pid_identity"]
    if not isinstance(identity, str) or not identity.split(" ", 1)[0].isdigit():
        raise ValidationError("workflow runner identity is not recorded")
    os.kill(int(identity.split(" ", 1)[0]), signal.SIGTERM)
    return {"run_id": run_id, "cancel_requested": True}


def workflow_answer(store: StateStore, run_id: str) -> dict[str, object]:
    """Return a terminal workflow's last persisted step result, if any.

    Raises ``ValidationError`` while the run has not yet reached a terminal
    status. ``result`` is ``None`` when no step has persisted a result yet,
    otherwise the JSON-decoded value of the most recently persisted one.
    """

    run = store.workflow_run_status(run_id)["run"]
    if run["status"] not in _TERMINAL_WORKFLOW_STATUSES:
        raise ValidationError("workflow run has not finished")
    row = store.connection.execute(
        """SELECT result_json FROM workflow_steps
           WHERE run_id = ? AND result_json IS NOT NULL ORDER BY rowid DESC LIMIT 1""",
        (run_id,),
    ).fetchone()
    return {
        "run_id": run_id,
        "status": run["status"],
        "result": None if row is None else json.loads(row["result_json"]),
    }
