"""Bounded reconciliation driven by exact detached-child exit proof."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agent_run.domain import ACTIVE, AgentId
from agent_run.errors import ValidationError

from .db import integer, timestamp

if TYPE_CHECKING:
    from .store import StateStore


def reconcile_reaped_agent(
    store: StateStore,
    agent_id: str | AgentId,
    supervisor_pid: int,
    *,
    at: float | None = None,
) -> bool:
    """Close the exact agent whose detached supervisor was reaped by waitpid."""

    from .store import StateStore

    if not isinstance(store, StateStore):
        raise ValidationError("store must be a StateStore")
    return store.reconcile_reaped(agent_id, supervisor_pid, checked_at=at)


def reconcile_reaped_supervisor(
    store: StateStore,
    supervisor_pid: int,
    *,
    at: float | None = None,
    limit: int = 100,
) -> tuple[AgentId, ...]:
    """Mark active rows owned by one reaped supervisor lost; never signal."""

    from .store import StateStore

    if not isinstance(store, StateStore):
        raise ValidationError("store must be a StateStore")
    integer("supervisor_pid", supervisor_pid, minimum=1)
    integer("limit", limit, minimum=1)
    if limit > 1_000:
        raise ValidationError("limit must not exceed 1000")
    checked_at = timestamp(at)
    statuses = tuple(sorted(status.value for status in ACTIVE))
    placeholders = ",".join("?" for _ in statuses)
    rows = list(
        store.connection.execute(
            f"""SELECT id, supervisor_pid, process_group_id, supervisor_identity
                FROM agents WHERE supervisor_pid = ?
                  AND status IN ({placeholders})
                ORDER BY created_at, id LIMIT ?""",
            (supervisor_pid, *statuses, limit),
        )
    )
    changed = []
    for row in rows:
        pid = row["supervisor_pid"]
        pgid = row["process_group_id"]
        identity = row["supervisor_identity"]
        if not isinstance(pid, int) or not isinstance(pgid, int) or not isinstance(identity, str):
            continue
        if store.reconcile(
            str(row["id"]),
            verdict="dead",
            supervisor_pid=pid,
            process_group_id=pgid,
            expected_identity=identity,
            alive=False,
            checked_at=checked_at,
            reason="detached supervisor exited",
        ):
            changed.append(AgentId(str(row["id"])))
    return tuple(changed)


def reconcile_active_agents(store, *, at: float | None = None, limit: int = 100) -> tuple[AgentId, ...]:
    """Boundedly close dead detached supervisors during an independent sweep."""
    from ..doctor import _probe_process

    checked_at = timestamp(at)
    statuses = tuple(sorted(status.value for status in ACTIVE))
    placeholders = ",".join("?" for _ in statuses)
    rows = store.connection.execute(
        f"SELECT id, supervisor_pid, process_group_id, supervisor_identity FROM agents "
        f"WHERE status IN ({placeholders}) ORDER BY created_at, id LIMIT ?",
        (*statuses, limit),
    )
    changed = []
    for row in rows:
        pid, pgid, expected = row["supervisor_pid"], row["process_group_id"], row["supervisor_identity"]
        if not isinstance(pid, int) or not isinstance(pgid, int) or not isinstance(expected, str):
            continue
        alive, identity, group_alive = _probe_process(pid, pgid)
        matches = identity == expected or (isinstance(identity, str) and identity.endswith(f" {expected}"))
        if alive and matches:
            continue
        if group_alive:
            # A group is recorded only for adapter sessions that own it.
            if alive:
                continue
            try:
                import os, signal
                os.killpg(pgid, signal.SIGTERM)
            except OSError:
                pass
            alive, identity, group_alive = _probe_process(pid, pgid)
            if alive or group_alive:
                continue
        if store.reconcile(
            str(row["id"]), verdict="dead" if not alive else "identity_mismatch",
            supervisor_pid=pid, process_group_id=pgid, expected_identity=expected,
            alive=alive, observed_identity=identity, checked_at=checked_at,
            reason="periodic detached supervisor reconciliation",
        ):
            changed.append(AgentId(str(row["id"])))
    return tuple(changed)
