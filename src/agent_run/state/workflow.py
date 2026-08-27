"""Workflow run and step journal, over the tables migration 002 created.

Resume semantics: a resumed run reuses its *run row* -- the caller passes the
same run id back in, and any step whose ``step_key`` already has a stored
result in that run is a cache hit via :func:`cached_step_result`.  A new run
(new run id) always starts with an empty step journal, even if it shares a
``script_sha`` with a prior run.  Resume identity therefore depends entirely
on ``step_key`` being a deterministic function of a step's spec and its
position -- see :func:`step_key`.

A step's terminal statuses (``succeeded``, ``failed``, ``skipped``, ``cached``)
are final: once written, a step row cannot be restarted or refinished, and a
finished run refuses to record any further step.  This mirrors the agent
transition guards in :mod:`agent_run.state.store`.

Replay representation: when a run is resumed (see :func:`resume_workflow_run`
and :mod:`agent_run.workflow_run`), a step whose ``step_key`` already has a
``succeeded`` row is *not* re-executed and its journal row is left completely
untouched -- no new ``record_step_start``/``finish_step`` call is made for it.
``workflow_run_status`` therefore still reports that step's original
``succeeded`` status for the replayed pass; a caller that needs to know how
many steps a given pass actually executed (as opposed to replayed from cache)
counts the run's ``record_step_start`` calls for that pass, since the journal
does not stamp *when* -- only *whether* -- a step ran.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid

from agent_run.errors import StateTransitionError, ValidationError

from .db import immediate, integer, json_text, nonblank, row_dict, timestamp

RUN_STATUSES = frozenset({"created", "running", "succeeded", "failed", "cancelled", "lost"})
RUN_TERMINAL = frozenset({"succeeded", "failed", "cancelled", "lost"})
RUN_RESUMABLE = frozenset({"failed", "cancelled", "lost"})
STEP_STATUSES = frozenset({"pending", "running", "succeeded", "failed", "skipped", "cached"})
STEP_TERMINAL = frozenset({"succeeded", "failed", "skipped", "cached"})


def step_key(spec: object, position: int) -> str:
    """A deterministic id for one step, derived from its spec and position.

    Hashes the canonical JSON encoding of ``spec`` (sorted keys, compact
    separators -- see :func:`agent_run.state.db.json_text`) together with
    ``position`` over SHA-256.  Two runs that build the same step at the same
    position always compute the same key, which is what lets
    :func:`cached_step_result` recognize a resumed step.
    """

    integer("position", position, minimum=0)
    digest = hashlib.sha256(json_text(spec).encode("utf-8"))
    digest.update(b"\x00")
    digest.update(str(position).encode("utf-8"))
    return digest.hexdigest()


def _run_row(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
    row = connection.execute(
        "SELECT * FROM workflow_runs WHERE id = ?", (run_id,)
    ).fetchone()
    if row is None:
        raise ValidationError(f"unknown workflow run: {run_id}")
    return row


def create_workflow_run(
    connection: sqlite3.Connection,
    name: str,
    script_sha: str,
    *,
    owner_identity: str | None = None,
    run_id: str | None = None,
    at: float | None = None,
    plan: object = None,
    orchestrator: object = None,
) -> str:
    """Insert one new run row, optionally recording the plan it will execute.

    ``plan`` is stored verbatim as JSON so a later :func:`resume_workflow_run`
    can relaunch the same steps without the caller having to keep them around;
    a run created without one (``plan`` left ``None``) simply cannot be
    resumed later -- see :func:`agent_run.workflow_run.resume_workflow`.
    """

    nonblank("name", name)
    nonblank("script_sha", script_sha)
    if owner_identity is not None:
        nonblank("owner_identity", owner_identity)
    candidate = run_id or f"wf_{uuid.uuid4().hex}"
    nonblank("run_id", candidate)
    created_at = timestamp(at)
    plan_json = None if plan is None else json_text(plan)
    with immediate(connection):
        session_id = None
        if orchestrator is not None:
            from ..domain import OrchestratorRef
            from .db import session_for_ref

            if not isinstance(orchestrator, OrchestratorRef):
                raise ValidationError("orchestrator must be an OrchestratorRef")
            session_id = session_for_ref(connection, orchestrator, created_at)
        connection.execute(
            """INSERT INTO workflow_runs
               (id, name, script_sha, status, owner_pid_identity, created_at, plan_json,
                orchestrator_session_id)
               VALUES (?, ?, ?, 'created', ?, ?, ?, ?)""",
            (candidate, name, script_sha, owner_identity, created_at, plan_json, session_id),
        )
    return candidate


def start_workflow_run(connection: sqlite3.Connection, run_id: str) -> None:
    nonblank("run_id", run_id)
    with immediate(connection):
        run = _run_row(connection, run_id)
        if run["status"] != "created":
            raise StateTransitionError(
                f"workflow run cannot start from status: {run['status']}"
            )
        connection.execute(
            "UPDATE workflow_runs SET status = 'running' WHERE id = ?", (run_id,)
        )


def claim_workflow_run(
    connection: sqlite3.Connection, run_id: str, owner_identity: str
) -> None:
    """Take durable ownership of a created run and start it, in one transaction.

    The detached runner may report READY only after this returns: an owner
    identity that outlives the process is exactly what lets reconciliation flip
    an abandoned run to ``lost`` instead of silently resuming it.  A run some
    other identity already owns is refused -- ownership is never stolen.
    """

    nonblank("run_id", run_id)
    nonblank("owner_identity", owner_identity)
    with immediate(connection):
        run = _run_row(connection, run_id)
        owner = run["owner_pid_identity"]
        if owner is not None and owner != owner_identity:
            raise StateTransitionError(f"workflow run is already owned: {owner}")
        if run["status"] != "created":
            raise StateTransitionError(
                f"workflow run cannot start from status: {run['status']}"
            )
        connection.execute(
            """UPDATE workflow_runs SET status = 'running', owner_pid_identity = ?
               WHERE id = ?""",
            (owner_identity, run_id),
        )


def resume_workflow_run(
    connection: sqlite3.Connection, run_id: str, owner_identity: str
) -> None:
    """Re-claim a finished-but-resumable run's row for a fresh runner.

    Unlike :func:`claim_workflow_run`, the row already carries a *terminal*
    status (``failed``, ``cancelled``, or ``lost``) and a stale owner from the
    runner that stopped short -- there is no live owner to protect here, only
    the run's own resumability.  The caller
    (:func:`agent_run.workflow_run.resume_workflow`) has already refused a
    ``running`` or ``succeeded`` run before a fresh runner is ever launched, so
    this only re-checks the row under the write lock against a race.
    """

    nonblank("run_id", run_id)
    nonblank("owner_identity", owner_identity)
    with immediate(connection):
        run = _run_row(connection, run_id)
        if run["status"] not in RUN_RESUMABLE:
            raise StateTransitionError(
                f"workflow run cannot resume from status: {run['status']}"
            )
        connection.execute(
            """UPDATE workflow_runs
               SET status = 'running', owner_pid_identity = ?, finished_at = NULL
               WHERE id = ?""",
            (owner_identity, run_id),
        )


def finish_workflow_run(
    connection: sqlite3.Connection,
    run_id: str,
    status: str,
    *,
    at: float | None = None,
) -> None:
    """Commit a terminal workflow state and its bound notice atomically."""

    nonblank("run_id", run_id)
    if status not in RUN_TERMINAL:
        raise ValidationError(f"workflow run status must be one of {sorted(RUN_TERMINAL)}")
    finished_at = timestamp(at)
    with immediate(connection):
        _run_row(connection, run_id)
        updated = connection.execute(
            """UPDATE workflow_runs SET status = ?, finished_at = ?
               WHERE id = ? AND status NOT IN ('succeeded', 'failed', 'cancelled', 'lost')""",
            (status, finished_at, run_id),
        ).rowcount
        if updated != 1:
            raise StateTransitionError("workflow run is already finished")
        run = _run_row(connection, run_id)
        if run["orchestrator_session_id"] is not None:
            connection.execute(
                """INSERT INTO workflow_deliveries
                   (id, run_id, orchestrator_session_id, state, next_attempt_at)
                   VALUES (?, ?, ?, 'pending', ?)""",
                (f"wnt_{uuid.uuid4().hex}", run_id,
                 run["orchestrator_session_id"], finished_at),
            )


def record_step_start(
    connection: sqlite3.Connection,
    run_id: str,
    step_key: str,
    spec: object,
    *,
    agent_id: str | None = None,
) -> None:
    nonblank("run_id", run_id)
    nonblank("step_key", step_key)
    spec_json = json_text(spec)
    with immediate(connection):
        run = _run_row(connection, run_id)
        if run["status"] != "running":
            raise StateTransitionError("workflow run must be running to record a step")
        existing = connection.execute(
            "SELECT status FROM workflow_steps WHERE run_id = ? AND step_key = ?",
            (run_id, step_key),
        ).fetchone()
        if existing is not None and existing["status"] in STEP_TERMINAL:
            raise StateTransitionError(f"step is already finished: {existing['status']}")
        connection.execute(
            """INSERT INTO workflow_steps
                   (run_id, step_key, spec_json, agent_id, status)
               VALUES (?, ?, ?, ?, 'running')
               ON CONFLICT(run_id, step_key) DO UPDATE SET
                   spec_json = excluded.spec_json,
                   agent_id = excluded.agent_id,
                   status = 'running',
                   result_json = NULL,
                   failure_kind = NULL,
                   failure_params_json = NULL""",
            (run_id, step_key, spec_json, agent_id),
        )


def finish_step(
    connection: sqlite3.Connection,
    run_id: str,
    step_key: str,
    status: str,
    *,
    result: object = None,
    failure_kind: str | None = None,
    failure_params: object = None,
) -> None:
    nonblank("run_id", run_id)
    nonblank("step_key", step_key)
    if status not in STEP_TERMINAL:
        raise ValidationError(f"step status must be one of {sorted(STEP_TERMINAL)}")
    if status == "failed":
        nonblank("failure_kind", failure_kind)
    elif failure_kind is not None:
        raise ValidationError("failure_kind is only valid when status is failed")
    result_json = None if result is None else json_text(result)
    failure_params_json = None if failure_kind is None else json_text(failure_params or {})
    with immediate(connection):
        updated = connection.execute(
            """UPDATE workflow_steps
               SET status = ?, result_json = ?, failure_kind = ?, failure_params_json = ?
               WHERE run_id = ? AND step_key = ? AND status = 'running'""",
            (status, result_json, failure_kind, failure_params_json, run_id, step_key),
        ).rowcount
        if updated != 1:
            row = connection.execute(
                "SELECT status FROM workflow_steps WHERE run_id = ? AND step_key = ?",
                (run_id, step_key),
            ).fetchone()
            if row is None:
                raise ValidationError(f"unknown workflow step: {run_id}/{step_key}")
            raise StateTransitionError(f"step is not running: {row['status']}")


def cached_step_result(
    connection: sqlite3.Connection, run_id: str, step_key: str
) -> object | None:
    nonblank("run_id", run_id)
    nonblank("step_key", step_key)
    row = connection.execute(
        """SELECT result_json FROM workflow_steps
           WHERE run_id = ? AND step_key = ? AND status IN ('succeeded', 'cached')""",
        (run_id, step_key),
    ).fetchone()
    if row is None or row["result_json"] is None:
        return None
    return json.loads(row["result_json"])


def workflow_run_status(
    connection: sqlite3.Connection, run_id: str, *, step_limit: int = 100
) -> dict[str, object]:
    integer("step_limit", step_limit, minimum=1)
    run = row_dict(_run_row(connection, run_id))
    steps = [
        dict(row)
        for row in connection.execute(
            """SELECT step_key, status, agent_id, failure_kind
               FROM workflow_steps WHERE run_id = ? ORDER BY rowid LIMIT ?""",
            (run_id, step_limit),
        )
    ]
    return {"run": run, "steps": steps}


def list_workflow_runs(
    connection: sqlite3.Connection,
    *,
    active_only: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, object]]:
    integer("limit", limit, minimum=1)
    integer("offset", offset, minimum=0)
    where = "WHERE status IN ('created', 'running')" if active_only else ""
    rows = connection.execute(
        f"""SELECT * FROM workflow_runs {where}
            ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?""",
        (limit, offset),
    )
    return [dict(row) for row in rows]
