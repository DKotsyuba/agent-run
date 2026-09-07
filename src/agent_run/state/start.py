"""Atomic, idempotent agent creation and active-cap enforcement."""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass

from agent_run.domain import (
    ACTIVE,
    AgentId,
    AgentStatus,
    StartRequest,
    new_agent_id,
    validate_agent_id,
)
from agent_run.errors import ValidationError
from agent_run.lifecycle import ProcessOps

from .resume import latest_child, resume_lineage
from .db import (
    idempotent_agent,
    immediate,
    insert_agent_row,
    insert_event,
    nonblank,
    request_json,
    request_json_matches,
    session_for_ref,
    timestamp,
)


@dataclass(frozen=True, slots=True)
class AgentCreation:
    """Result of one atomic creation or idempotent replay."""

    agent_id: AgentId
    created: bool


def _limit(name: str, value: int | None) -> int | None:
    """Validate an optional positive active-agent limit and return it."""

    if value is not None and (type(value) is not int or value < 1):
        raise ValidationError(f"{name} must be a positive integer")
    return value


def create_agent(
    connection: sqlite3.Connection,
    request: StartRequest,
    *,
    task_summary: str,
    config_revision: str,
    agent_id: str | AgentId | None = None,
    at: float | None = None,
    global_limit: int | None = None,
    runtime_limit: int | None = None,
    parent_agent_id: str | AgentId | None = None,
    identity_json: str | None = None,
    ops: ProcessOps | None = None,
    startup_owner_identity: str | None = None,
    startup_owner_birth_time: float | None = None,
    startup_deadline_seconds: float | None = None,
) -> AgentCreation:
    """Atomically admit one agent or replay its immutable request.

    ``config_revision`` may be replaced after asynchronous materialization, so
    replay equality deliberately uses the serialized request, the task summary
    and the resume parent. Capacity is checked after replay lookup.

    ``parent_agent_id`` makes this a resume: the new row joins that agent's
    chain instead of starting its own. The parent is validated by
    :func:`agent_run.state.resume.resume_lineage` *after* the idempotent-replay
    lookup, so replaying an already accepted resume returns its existing child
    even once the parent has become a stale source. At most one child may ever
    be accepted per parent; a second attempt loses the race on the partial
    unique index and raises :class:`ValidationError` naming the winner rather
    than branching the chain. ``ops`` is the process surface used for the
    parent's liveness proof; ``None`` selects the real one.
    ``identity_json`` is stored verbatim as this row's effective-identity
    snapshot and never participates in replay equality.

    Supplying ``startup_owner_identity`` makes this the service admission path:
    the row, ``created``/``start_accepted`` events, ``STARTING`` status, owner
    birth proof and finite deadline commit in this transaction. Omitting it
    retains low-level historical/test creation as ``CREATED``; partial owner
    parameters are rejected.
    """

    if not isinstance(request, StartRequest):
        raise ValidationError("request must be a StartRequest")
    nonblank("task_summary", task_summary)
    nonblank("config_revision", config_revision)
    global_limit = _limit("global active agent limit", global_limit)
    runtime_limit = _limit("runtime active agent limit", runtime_limit)
    created_at = timestamp(at)
    admitted = startup_owner_identity is not None
    if not admitted:
        if startup_owner_birth_time is not None or startup_deadline_seconds is not None:
            raise ValidationError("startup owner identity is required for admission")
    else:
        nonblank("startup owner identity", startup_owner_identity)
        if startup_owner_birth_time is not None and (
            isinstance(startup_owner_birth_time, bool)
            or not isinstance(startup_owner_birth_time, (int, float))
            or not math.isfinite(startup_owner_birth_time)
            or startup_owner_birth_time < 0
        ):
            raise ValidationError("startup owner birth time must be finite and nonnegative")
        if (
            isinstance(startup_deadline_seconds, bool)
            or not isinstance(startup_deadline_seconds, (int, float))
            or not math.isfinite(startup_deadline_seconds)
            or startup_deadline_seconds <= 0
        ):
            raise ValidationError("startup deadline must be positive and finite")
    candidate = new_agent_id() if agent_id is None else validate_agent_id(agent_id)
    parent = None if parent_agent_id is None else validate_agent_id(parent_agent_id)
    serialized = request_json(request)
    statuses = tuple(sorted(status.value for status in ACTIVE))
    placeholders = ",".join("?" for _ in statuses)
    with immediate(connection):
        if request.request_id is not None:
            existing = idempotent_agent(
                connection, request.request_id, request.orchestrator
            )
            if existing is not None:
                # Parent identity is part of the request even though it is not
                # in the serialized payload: the same request_id and the same
                # task aimed at a different parent is a different resume, not a
                # replay of this one.
                if (
                    not request_json_matches(existing["request_json"], serialized)
                    or existing["task_summary"] != task_summary
                    or existing["parent_agent_id"] != parent
                ):
                    raise ValidationError("request_id was reused for a different request")
                return AgentCreation(AgentId(str(existing["id"])), False)
        if global_limit is not None:
            active = connection.execute(
                f"SELECT COUNT(*) FROM agents WHERE status IN ({placeholders})",
                statuses,
            ).fetchone()[0]
            if active >= global_limit:
                raise ValidationError("global active agent limit reached")
        if runtime_limit is not None:
            active = connection.execute(
                f"""SELECT COUNT(*) FROM agents
                    WHERE runtime = ? AND status IN ({placeholders})""",
                (request.runtime, *statuses),
            ).fetchone()[0]
            if active >= runtime_limit:
                raise ValidationError(
                    f"runtime active agent limit reached: {request.runtime}"
                )
        session_id = (
            None
            if request.orchestrator is None
            else session_for_ref(connection, request.orchestrator, created_at)
        )
        lineage = None if parent is None else resume_lineage(connection, parent, ops)
        try:
            insert_agent_row(
                connection,
                candidate,
                request,
                session_id,
                task_summary,
                serialized,
                config_revision,
                created_at,
                parent_agent_id=parent,
                root_agent_id=None if lineage is None else lineage.root_agent_id,
                sequence=1 if lineage is None else lineage.sequence,
                resume_of_runtime_session_id=(
                    None if lineage is None else lineage.runtime_session_id
                ),
                identity_json=identity_json,
            )
        except sqlite3.IntegrityError as error:
            # Only the one-child guard is translated. Any other integrity
            # failure (request_id uniqueness, a foreign key) still surfaces as
            # itself rather than being mislabelled a lost resume race.
            winner = None if parent is None else latest_child(connection, parent)
            if winner is None:
                raise
            raise ValidationError(
                f"agent {parent} has already been resumed by {winner}"
            ) from error
        insert_event(
            connection,
            candidate,
            created_at,
            "created",
            to_status=AgentStatus.CREATED.value,
        )
        if admitted:
            connection.execute(
                """UPDATE agents SET status = ?, startup_owner_pid_identity = ?,
                          startup_owner_birth_time = ?, startup_deadline_at = ?
                   WHERE id = ?""",
                (
                    AgentStatus.STARTING.value,
                    startup_owner_identity,
                    startup_owner_birth_time,
                    created_at + float(startup_deadline_seconds),
                    candidate,
                ),
            )
            insert_event(
                connection,
                candidate,
                created_at,
                "start_accepted",
                from_status=AgentStatus.CREATED.value,
                to_status=AgentStatus.STARTING.value,
            )
    return AgentCreation(candidate, True)
