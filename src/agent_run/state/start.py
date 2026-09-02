"""Atomic, idempotent agent creation and active-cap enforcement."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from agent_run.domain import ACTIVE, AgentId, AgentStatus, StartRequest, new_agent_id, validate_agent_id
from agent_run.errors import ValidationError

from .db import (
    idempotent_agent,
    immediate,
    insert_agent_row,
    insert_event,
    nonblank,
    request_json,
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
) -> AgentCreation:
    """Atomically admit one agent or replay its immutable request.

    ``config_revision`` may be replaced after asynchronous materialization, so
    replay equality deliberately uses only the serialized request and task
    summary. Capacity is checked after replay lookup.
    """

    if not isinstance(request, StartRequest):
        raise ValidationError("request must be a StartRequest")
    nonblank("task_summary", task_summary)
    nonblank("config_revision", config_revision)
    global_limit = _limit("global active agent limit", global_limit)
    runtime_limit = _limit("runtime active agent limit", runtime_limit)
    created_at = timestamp(at)
    candidate = new_agent_id() if agent_id is None else validate_agent_id(agent_id)
    serialized = request_json(request)
    statuses = tuple(sorted(status.value for status in ACTIVE))
    placeholders = ",".join("?" for _ in statuses)
    with immediate(connection):
        if request.request_id is not None:
            existing = idempotent_agent(connection, request.request_id)
            if existing is not None:
                if (
                    existing["request_json"] != serialized
                    or existing["task_summary"] != task_summary
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
        insert_agent_row(
            connection,
            candidate,
            request,
            session_id,
            task_summary,
            serialized,
            config_revision,
            created_at,
        )
        insert_event(
            connection,
            candidate,
            created_at,
            "created",
            to_status=AgentStatus.CREATED.value,
        )
    return AgentCreation(candidate, True)
