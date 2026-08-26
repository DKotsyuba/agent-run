"""Derived read models for model-visible active-agent context."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agent_run.domain import ACTIVE
from agent_run.errors import ValidationError

from .db import integer, nonblank, timestamp

if TYPE_CHECKING:
    from .store import StateStore


def context_agents(
    store: StateStore,
    orchestrator_session_id: str,
    *,
    at: float,
    limit: int = 1_000,
) -> tuple[dict[str, object], ...]:
    from .store import StateStore

    if not isinstance(store, StateStore):
        raise ValidationError("store must be a StateStore")
    nonblank("orchestrator_session_id", orchestrator_session_id)
    integer("limit", limit, minimum=1)
    if limit > 1_000:
        raise ValidationError("limit must not exceed 1000")
    observed_at = timestamp(at)
    statuses = tuple(sorted(status.value for status in ACTIVE))
    placeholders = ",".join("?" for _ in statuses)
    rows = store.connection.execute(
        f"""SELECT a.*,
                   EXISTS(SELECT 1 FROM events e
                          WHERE e.agent_id = a.id AND e.kind = 'deadline_warning')
                       AS activity_warned,
                   MAX(0.0, ? - COALESCE(
                       (SELECT MAX(m.at) FROM messages m WHERE m.agent_id = a.id),
                       a.started_at, a.created_at)) AS activity_silence
            FROM agents a
            WHERE a.orchestrator_session_id = ?
              AND a.status IN ({placeholders})
            ORDER BY a.created_at DESC, a.id DESC LIMIT ?""",
        (observed_at, orchestrator_session_id, *statuses, limit),
    )
    result = []
    for row in rows:
        item = dict(row)
        item["warned"] = bool(item.pop("activity_warned"))
        item["silent_seconds"] = float(item.pop("activity_silence"))
        result.append(item)
    return tuple(result)
