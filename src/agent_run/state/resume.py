"""Durable resume lineage: proving a parent agent is a safe resume source.

Everything here runs inside the caller's write transaction in
:func:`agent_run.state.start.create_agent`. Splitting it out keeps the resume
admission rules -- which are about process liveness and chain shape, not about
capacity or idempotency -- readable on their own.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from agent_run.domain import TERMINAL, AgentId, AgentStatus
from agent_run.errors import ValidationError
from agent_run.lifecycle import ProcessOps, SystemProcessOps


# Finishing is necessary but not sufficient. A row reaches FAILED with
# ``engine_group_survived`` precisely because its process group outlived the
# supervisor, and LOST only means the supervisor stopped reporting -- neither
# is proof the runtime released its session. Status narrows the candidates;
# `_quiescent` below is what actually decides.
RESUMABLE: frozenset[AgentStatus] = frozenset(TERMINAL)


@dataclass(frozen=True, slots=True)
class ResumeLineage:
    """The chain position and source session one resumed child inherits.

    ``root_agent_id`` is the chain's first agent, ``sequence`` this child's
    1-based position in it, and ``runtime_session_id`` the native runtime
    session the adapter will be asked to continue.
    """

    root_agent_id: AgentId
    sequence: int
    runtime_session_id: str


def _quiescent(row: sqlite3.Row, ops: ProcessOps) -> bool:
    """Report whether the parent agent's runtime process is provably gone.

    ``row`` must carry ``process_group_id`` and ``supervisor_pid``. A recorded
    process group is probed directly: still alive means a live owner may still
    hold the runtime session, so the resume must fail closed. No recorded group
    is only conclusive when no supervisor pid was recorded either -- that is an
    agent which never reached spawn. A supervisor pid without a group is
    unprovable and therefore treated as not quiescent.

    Never raises for an unknown pid: :meth:`ProcessOps.group_alive` maps a
    missing group to ``False`` and an unsignalable one to ``True``.
    """

    pgid = row["process_group_id"]
    if pgid is not None:
        return not ops.group_alive(int(pgid))
    return row["supervisor_pid"] is None


def resume_lineage(
    connection: sqlite3.Connection,
    parent_agent_id: AgentId,
    ops: ProcessOps | None = None,
) -> ResumeLineage:
    """Read one parent row and prove it can still be resumed, or refuse.

    Must run inside the caller's write transaction: these checks and the child
    INSERT have to commit together, or a concurrent resume could pass this gate
    against a row another attempt is about to claim.

    ``ops`` is the process surface used for the liveness proof, defaulting to
    :class:`SystemProcessOps`; tests inject a fake. The session the child
    inherits is the parent's own ``runtime_session_id`` when it recorded one,
    falling back to the ``resume_of_runtime_session_id`` it was itself asked to
    attach to -- a child that failed before attaching never confirmed a session
    of its own, but the context it was pointed at is still the last confirmed
    one for the chain.

    Raise :class:`ValidationError` when the parent is unknown, has not
    finished, cannot be proved quiescent, or has no session identity at all --
    a resume with nothing to attach to fails closed rather than silently
    replaying the task as a fresh run.
    """

    row = connection.execute(
        """SELECT status, root_agent_id, sequence, runtime_session_id,
                  resume_of_runtime_session_id, process_group_id, supervisor_pid
           FROM agents WHERE id = ?""",
        (parent_agent_id,),
    ).fetchone()
    if row is None:
        raise ValidationError(f"agent not found: {parent_agent_id}")
    status = AgentStatus(str(row["status"]))
    if status not in RESUMABLE:
        raise ValidationError(
            f"agent {parent_agent_id} is not resumable in status {status.value}"
        )
    if not _quiescent(row, SystemProcessOps() if ops is None else ops):
        raise ValidationError(
            f"agent {parent_agent_id} finished but its runtime process is still "
            f"alive or unprovable; refusing to attach to a session it may own"
        )
    session = row["runtime_session_id"] or row["resume_of_runtime_session_id"]
    if session is None or not str(session).strip():
        raise ValidationError(
            f"agent {parent_agent_id} recorded no runtime session to resume"
        )
    root = row["root_agent_id"]
    return ResumeLineage(
        AgentId(str(root) if root else str(parent_agent_id)),
        int(row["sequence"]) + 1,
        str(session),
    )


def latest_child(
    connection: sqlite3.Connection, parent_agent_id: AgentId
) -> AgentId | None:
    """Return the latest chain node if ``parent_agent_id`` already has a child.

    Used only to name the winner in the error raised for a losing concurrent
    resume, so the caller can point at the current chain head instead of
    reporting an opaque constraint failure. Returns ``None`` when the parent
    has no child, which for a failed INSERT means the conflict was something
    other than the one-child guard.
    """

    row = connection.execute(
        """SELECT latest.id FROM agents AS child
           JOIN agents AS latest ON latest.root_agent_id = child.root_agent_id
           WHERE child.parent_agent_id = ?
           ORDER BY latest.sequence DESC LIMIT 1""", (parent_agent_id,)
    ).fetchone()
    return None if row is None else AgentId(str(row["id"]))
